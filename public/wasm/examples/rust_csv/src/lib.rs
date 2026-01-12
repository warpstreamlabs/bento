use extism_pdk::*;
use csv::ReaderBuilder;
use serde_json::{Map, Value};
use protobuf::Message;

mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

use protos::message::{part::Content, Batch, Batches};

#[plugin_fn]
pub fn plugin_init(_input: Vec<u8>) -> FnResult<()> {
    Ok(())
}

#[plugin_fn]
pub fn process_batch(input: Vec<u8>) -> FnResult<Vec<u8>> {
    let mut batch = Batch::parse_from_bytes(&input)
        .map_err(|e| WithReturnCode::new(Error::msg(format!("Parse error: {}", e)), 1))?;

    for part in batch.parts.iter_mut() {
        if let Some(Content::Raw(ref mut bytes)) = part.content {
            match csv_to_json(bytes) {
                Ok(json) => *bytes = json,
                Err(e) => {
                    error!("CSV conversion failed: {}", e);
                    part.error = format!("CSV error: {}", e);
                }
            }
        }
    }

    let mut res = Batches::new();
    res.batches.push(batch);
    res.write_to_bytes()
        .map_err(|e| WithReturnCode::new(Error::msg(format!("Serialize error: {}", e)), 1))
}

fn csv_to_json(csv_data: &[u8]) -> Result<Vec<u8>, String> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_data);

    let hdrs = rdr.headers()
        .map_err(|e| format!("Header error: {}", e))?
        .clone();

    let mut rows = Vec::new();
    for result in rdr.records() {
        let rec = result.map_err(|e| format!("Record error: {}", e))?;
        let mut obj = Map::new();
        for (i, header) in hdrs.iter().enumerate() {
            let val = rec.get(i).unwrap_or("");
            obj.insert(header.to_string(), Value::String(val.to_string()));
        }
        rows.push(Value::Object(obj));
    }

    serde_json::to_vec(&rows)
        .map_err(|e| format!("JSON error: {}", e))
}