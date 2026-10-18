use bento_pdk::{bento_plugin, Message, Result};
use csv::ReaderBuilder;
use serde_json::{Map, Value};

#[bento_plugin(process_batch)]
fn process_batch(mut batch: Vec<Message>) -> Result<Vec<Vec<Message>>> {
    for msg in &mut batch {
        let json = csv_to_json(&msg.bytes());
        match json {
            Ok(json) => msg.set_bytes(json),
            Err(e) => msg.set_error(&format!("CSV error: {}", e)),
        }
    }
    Ok(vec![batch])
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
