use std::sync::OnceLock;

use extism_pdk::*;
use protobuf::Message;
use serde::{Deserialize, Serialize};
use tokenizers::Tokenizer;

mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

use protos::message::{part::Content, Batch, Batches};

#[derive(Deserialize)]
struct Config {
    path: String,
    #[serde(default = "default_add_special_tokens")]
    add_special_tokens: bool,
}

fn default_add_special_tokens() -> bool {
    true
}

struct State {
    tokenizer: Tokenizer,
    add_special_tokens: bool,
}

static STATE: OnceLock<State> = OnceLock::new();

#[plugin_fn]
pub fn init_plugin(input: Vec<u8>) -> FnResult<()> {
    let conf: Config = serde_json::from_slice(&input)?;

    // Wasm instances are single threaded, so never attempt to spawn a pool.
    tokenizers::utils::parallelism::set_parallelism(false);

    let tokenizer = Tokenizer::from_file(&conf.path).map_err(|e| {
        Error::msg(format!(
            "failed to load tokenizer from {}: {}",
            conf.path, e
        ))
    })?;

    let _ = STATE.set(State {
        tokenizer,
        add_special_tokens: conf.add_special_tokens,
    });
    Ok(())
}

#[plugin_fn]
pub fn process_batch(input: Vec<u8>) -> FnResult<Vec<u8>> {
    let state = STATE
        .get()
        .ok_or_else(|| Error::msg("tokenizer has not been initialised"))?;

    let mut batch = Batch::parse_from_bytes(&input)?;
    for part in batch.parts.iter_mut() {
        match tokenize(state, part.content.as_ref()) {
            Ok(out) => part.content = Some(Content::Raw(out)),
            Err(e) => part.error = format!("failed to tokenize: {}", e),
        }
    }

    let mut res = Batches::new();
    res.batches.push(batch);
    Ok(res.write_to_bytes()?)
}

fn tokenize(state: &State, content: Option<&Content>) -> Result<Vec<u8>, String> {
    let text = match content {
        Some(Content::Raw(bytes)) => std::str::from_utf8(bytes)
            .map_err(|e| format!("content is not valid UTF-8: {}", e))?,
        Some(Content::Structured(value)) if value.has_string_value() => value.string_value(),
        Some(Content::Structured(_)) => return Err("structured content must be a string".into()),
        None => "",
    };

    let encoding = state
        .tokenizer
        .encode(text, state.add_special_tokens)
        .map_err(|e| e.to_string())?;

    serde_json::to_vec(&Output {
        ids: encoding.get_ids(),
        tokens: encoding.get_tokens(),
        attention_mask: encoding.get_attention_mask(),
    })
    .map_err(|e| e.to_string())
}

#[derive(Serialize)]
struct Output<'a> {
    ids: &'a [u32],
    tokens: &'a [String],
    attention_mask: &'a [u32],
}
