use std::sync::OnceLock;

use extism_pdk::*;
use lzf::LzfError;
use protobuf::Message;
use serde::Deserialize;

mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

use protos::message::{part::Content, Batch, Batches};

type ProcessFn = fn(&[u8]) -> Result<Vec<u8>, LzfError>;

struct Processor {
    name: &'static str,
    run: ProcessFn,
}

static PROCESSOR: OnceLock<Processor> = OnceLock::new();

#[derive(Deserialize)]
struct Config {
    operation: String,
}

#[plugin_fn]
pub fn init_plugin(Json(conf): Json<Config>) -> FnResult<()> {
    let processor = match conf.operation.as_str() {
        "compress" => Processor {
            name: "compress",
            run: lzf::compress,
        },
        "decompress" => Processor {
            name: "decompress",
            run: decompress,
        },
        other => {
            return Err(Error::msg(format!(
                "invalid operation {other:?}, expected `compress` or `decompress`"
            ))
            .into())
        }
    };
    PROCESSOR
        .set(processor)
        .map_err(|_| Error::msg("plugin already initialised"))?;
    Ok(())
}

#[plugin_fn]
pub fn process_batch(input: Vec<u8>) -> FnResult<Vec<u8>> {
    let processor = PROCESSOR
        .get()
        .ok_or_else(|| Error::msg("plugin not initialised"))?;

    let mut batch = Batch::parse_from_bytes(&input)?;

    for part in batch.parts.iter_mut() {
        if let Some(Content::Raw(bytes)) = &mut part.content {
            match (processor.run)(bytes) {
                Ok(out) => *bytes = out,
                Err(e) => {
                    let msg = format!("lzf {} failed: {e}", processor.name);
                    error!("{msg}");
                    part.error = msg;
                }
            }
        }
    }

    let mut res = Batches::new();
    res.batches.push(batch);
    Ok(res.write_to_bytes()?)
}

// Raw LZF doesn't encode the uncompressed length, so grow the output buffer
// until it's large enough. This terminates: corrupt input returns
// `DataCorrupted`, and memory is capped by the manifest's `max_pages`.
fn decompress(data: &[u8]) -> Result<Vec<u8>, LzfError> {
    let mut size = (data.len() * 2).max(64);
    loop {
        match lzf::decompress(data, size) {
            Err(LzfError::BufferTooSmall) => size *= 2,
            res => return res,
        }
    }
}
