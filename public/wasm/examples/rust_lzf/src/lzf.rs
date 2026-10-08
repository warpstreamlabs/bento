use std::sync::OnceLock;

use extism_pdk::*;
use lzf::LzfError;
use serde::Deserialize;

mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

use protos::message::{part::Content, Batch, Batches};

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Operation {
    Compress,
    Decompress,
}

impl Operation {
    fn run(self, data: &[u8]) -> Result<Vec<u8>, LzfError> {
        match self {
            Operation::Compress => lzf::compress(data),
            Operation::Decompress => decompress(data),
        }
    }
}

#[derive(Deserialize)]
struct Config {
    operation: Operation,
}

static OPERATION: OnceLock<Operation> = OnceLock::new();

#[plugin_fn]
pub fn init_plugin(Json(conf): Json<Config>) -> FnResult<()> {
    OPERATION
        .set(conf.operation)
        .map_err(|_| Error::msg("plugin already initialised"))?;
    Ok(())
}

#[plugin_fn]
pub fn process_batch(Protobuf(mut batch): Protobuf<Batch>) -> FnResult<Protobuf<Batches>> {
    let op = *OPERATION
        .get()
        .ok_or_else(|| Error::msg("plugin not initialised"))?;

    for part in batch.parts.iter_mut() {
        if let Some(Content::Raw(bytes)) = &mut part.content {
            match op.run(bytes) {
                Ok(out) => *bytes = out,
                // Per-part failures are reported on the part, not the batch.
                // Note: lzf::compress errors on input it can't shrink.
                Err(e) => part.error = format!("lzf {op:?} failed: {e}"),
            }
        }
    }

    let mut res = Batches::new();
    res.batches.push(batch);
    Ok(Protobuf(res))
}

// Raw LZF doesn't store the uncompressed length, so grow the buffer until it fits.
// Always terminates: LZF expands at most ~132x, so past that `BufferTooSmall`
// can't occur and corrupt input returns `DataCorrupted`.
fn decompress(data: &[u8]) -> Result<Vec<u8>, LzfError> {
    let mut size = (data.len() * 2).max(64);
    loop {
        match lzf::decompress(data, size) {
            Err(LzfError::BufferTooSmall) => size *= 2,
            res => return res,
        }
    }
}