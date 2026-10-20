use std::sync::OnceLock;

use bento_pdk::{bento_plugin, Error, Message, Result};
use serde::{Deserialize, Serialize};
use tokenizers::Tokenizer;

#[derive(Deserialize)]
struct Config {
    path: String,
    #[serde(default = "default_add_special_tokens")]
    add_special_tokens: bool,
}

fn default_add_special_tokens() -> bool {
    true
}

struct TokenizerProc {
    tokenizer: Tokenizer,
    add_special_tokens: bool,
}

impl TokenizerProc {
    fn new(conf: Config) -> Result<Self> {
        // Wasm instances are single threaded, so never attempt to spawn a pool.
        tokenizers::utils::parallelism::set_parallelism(false);

        let tokenizer = Tokenizer::from_file(&conf.path).map_err(|e| {
            Error::from(format!(
                "failed to load tokenizer from {}: {}",
                conf.path, e
            ))
        })?;

        Ok(Self {
            tokenizer,
            add_special_tokens: conf.add_special_tokens,
        })
    }

    fn tokenize(&self, content: &[u8]) -> Result<Vec<u8>, String> {
        let text = std::str::from_utf8(content)
            .map_err(|e| format!("content is not valid UTF-8: {}", e))?;

        let encoding = self
            .tokenizer
            .encode(text, self.add_special_tokens)
            .map_err(|e| e.to_string())?;

        serde_json::to_vec(&Output {
            ids: encoding.get_ids(),
            tokens: encoding.get_tokens(),
            attention_mask: encoding.get_attention_mask(),
        })
        .map_err(|e| e.to_string())
    }
}

static TOKENIZER: OnceLock<TokenizerProc> = OnceLock::new();

#[bento_plugin(init)]
fn init(conf: Config) -> Result<()> {
    TOKENIZER
        .set(TokenizerProc::new(conf)?)
        .map_err(|_| "plugin already initialised")?;
    Ok(())
}

#[bento_plugin(process_batch)]
fn process_batch(mut batch: Vec<Message>) -> Result<Vec<Vec<Message>>> {
    let tokenizer = TOKENIZER.get().ok_or("plugin not initialised")?;
    for msg in &mut batch {
        let out = tokenizer.tokenize(&msg.bytes());
        match out {
            Ok(out) => msg.set_bytes(out),
            Err(e) => msg.set_error(&format!("failed to tokenize: {}", e)),
        }
    }
    Ok(vec![batch])
}

#[derive(Serialize)]
struct Output<'a> {
    ids: &'a [u32],
    tokens: &'a [String],
    attention_mask: &'a [u32],
}
