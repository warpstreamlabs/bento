//! Bindings for Bento WASM plugins. Messages are exchanged with Bento as
//! protobuf (resources/protos/message.proto), and decoded into a [`Message`]
//! for each call. See website/docs/guides/plugins/about.md for the ABI.
//!
//! A plugin is built as a `cdylib` for `wasm32-unknown-unknown`, or for
//! `wasm32-wasip1` when it reads files mounted by Bento.
//!
//! A plugin exports its entry points with the [`bento_plugin`] attribute: an
//! optional `init`, called once per instance with the plugin's config, and
//! either `process_batch`, called with every message of a batch, or `process`,
//! called once per message. State that `init` prepares is kept in statics:
//!
//! ```ignore
//! use std::sync::OnceLock;
//!
//! use bento_pdk::{bento_plugin, Message, Result};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct Config {
//!     key: String,
//! }
//!
//! static KEY: OnceLock<String> = OnceLock::new();
//!
//! #[bento_plugin(init)]
//! fn init(conf: Config) -> Result<()> {
//!     KEY.set(conf.key).map_err(|_| "plugin already initialised")?;
//!     Ok(())
//! }
//!
//! #[bento_plugin(process_batch)]
//! fn process_batch(mut batch: Vec<Message>) -> Result<Vec<Vec<Message>>> {
//!     let key = KEY.get().ok_or("plugin not initialised")?;
//!     for msg in &mut batch {
//!         msg.meta_set(key, "true");
//!     }
//!     Ok(vec![batch])
//! }
//! ```
//!
//! A `process_batch` function returns zero or more batches, and a `process`
//! function zero or more messages, where a message that is not returned is
//! dropped. A message can be failed on its own with [`Message::set_error`]. If
//! either function fails, the messages continue unchanged except for being
//! marked with the error.

use std::borrow::Cow;
use std::slice;

use protobuf::well_known_types::struct_::{value::Kind, Value};
use protobuf::Message as _;
use serde::de::DeserializeOwned;

pub use bento_pdk_macros::bento_plugin;

mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

use protos::message::{part::Content, Batch, Batches, Part};

/// The error type of plugins.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// The result type of plugins.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[doc(hidden)]
pub fn init_plugin<C: DeserializeOwned>(config: Buffer, f: impl FnOnce(C) -> Result<()>) -> Buffer {
    match parse_config(config).and_then(f) {
        Ok(()) => Buffer::EMPTY,
        Err(err) => Buffer::retain(err.to_string().into_bytes()),
    }
}

#[doc(hidden)]
pub fn process(input: Buffer, f: impl FnOnce(Message) -> Result<Vec<Message>>) -> *const CallResult {
    let output = || -> Result<Vec<u8>> {
        let part = Part::parse_from_bytes(input.as_slice())?;
        let batch = f(Message(part))?;
        Ok(to_batch(batch).write_to_bytes()?)
    };
    CallResult::store(output())
}

#[doc(hidden)]
pub fn process_batch(input: Buffer, f: impl FnOnce(Vec<Message>) -> Result<Vec<Vec<Message>>>) -> *const CallResult {
    let output = || -> Result<Vec<u8>> {
        let batch = Batch::parse_from_bytes(input.as_slice())?;
        let batches = f(batch.parts.into_iter().map(Message).collect())?;

        let mut output = Batches::new();
        output.batches = batches.into_iter().map(to_batch).collect();
        Ok(output.write_to_bytes()?)
    };
    CallResult::store(output())
}

/// The result area that `process` and `process_batch` return the address of,
/// which holds the buffers of their output and error. The error is empty on
/// success.
#[doc(hidden)]
#[repr(C)]
pub struct CallResult {
    output: Buffer,
    error: Buffer,
}

impl CallResult {
    /// Stores the output of a call, or the error it failed with, in the result
    /// area, retaining both until Bento releases them.
    fn store(result: Result<Vec<u8>>) -> *const CallResult {
        // The result area lives as long as the instance. An instance makes one
        // call at a time, so one is enough.
        static mut AREA: CallResult = CallResult {
            output: Buffer::EMPTY,
            error: Buffer::EMPTY,
        };

        let value = match result {
            Ok(output) => CallResult {
                output: Buffer::retain(output),
                error: Buffer::EMPTY,
            },
            Err(err) => CallResult {
                output: Buffer::EMPTY,
                error: Buffer::retain(err.to_string().into_bytes()),
            },
        };

        let area = std::ptr::addr_of_mut!(AREA);
        unsafe { area.write(value) };
        area
    }
}

/// Deserializes the plugin's config, which is empty when it has none.
fn parse_config<C: DeserializeOwned>(config: Buffer) -> Result<C> {
    let data = match config.as_slice() {
        [] => b"{}".as_slice(),
        data => data,
    };
    Ok(serde_json::from_slice(data)?)
}

fn to_batch(batch: Vec<Message>) -> Batch {
    let mut out = Batch::new();
    out.parts = batch.into_iter().map(|msg| msg.0).collect();
    out
}

//------------------------------------------------------------------------------

/// A message, decoded from the input of the current call.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Message(Part);

impl Message {
    /// Creates a message with content.
    pub fn new(content: impl Into<Vec<u8>>) -> Self {
        let mut msg = Message::default();
        msg.set_bytes(content);
        msg
    }

    /// Returns the payload of the message, where structured content is encoded
    /// as JSON.
    pub fn bytes(&self) -> Cow<'_, [u8]> {
        match &self.0.content {
            Some(Content::Raw(raw)) => Cow::Borrowed(raw),
            Some(Content::Structured(value)) => Cow::Owned(to_json(value).to_string().into_bytes()),
            None => Cow::Borrowed(&[]),
        }
    }

    /// Replaces the payload of the message.
    pub fn set_bytes(&mut self, content: impl Into<Vec<u8>>) {
        self.0.set_raw(content.into());
    }

    /// Returns the error of the message, if it has one.
    pub fn error(&self) -> Option<&str> {
        Some(self.0.error.as_str()).filter(|err| !err.is_empty())
    }

    /// Sets the error of the message, an empty string clears it.
    pub fn set_error(&mut self, err: &str) {
        self.0.error = err.to_owned();
    }

    /// Returns a metadata value, if the key exists. Values that are not strings
    /// are encoded as JSON.
    pub fn meta_get(&self, key: &str) -> Option<String> {
        let value = self.0.metadata.as_ref()?.fields.get(key)?;
        match &value.kind {
            Some(Kind::StringValue(s)) => Some(s.clone()),
            _ => Some(to_json(value).to_string()),
        }
    }

    /// Sets a metadata value.
    pub fn meta_set(&mut self, key: &str, value: &str) {
        let mut v = Value::new();
        v.set_string_value(value.to_owned());
        self.0.metadata.mut_or_insert_default().fields.insert(key.to_owned(), v);
    }
}

/// Converts a protobuf value to JSON. Whole numbers are encoded as integers,
/// as Bento does.
fn to_json(value: &Value) -> serde_json::Value {
    match &value.kind {
        Some(Kind::NumberValue(n)) if n.fract() == 0.0 && n.abs() < (1u64 << 53) as f64 => (*n as i64).into(),
        Some(Kind::NumberValue(n)) => (*n).into(),
        Some(Kind::StringValue(s)) => s.clone().into(),
        Some(Kind::BoolValue(b)) => (*b).into(),
        Some(Kind::StructValue(s)) => s.fields.iter().map(|(k, v)| (k.clone(), to_json(v))).collect(),
        Some(Kind::ListValue(l)) => l.values.iter().map(to_json).collect(),
        _ => serde_json::Value::Null,
    }
}

//------------------------------------------------------------------------------

/// A range of plugin memory exchanged with Bento, packed as its address in the
/// upper 32 bits and its length in the lower 32 bits. Zero is empty.
#[doc(hidden)]
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Buffer(u64);

impl Buffer {
    const EMPTY: Buffer = Buffer(0);

    /// Retains data until Bento releases it, returning a buffer referencing it.
    fn retain(data: Vec<u8>) -> Self {
        if data.is_empty() {
            return Self::EMPTY;
        }
        let len = data.len();
        let ptr = Box::into_raw(data.into_boxed_slice()) as *mut u8;
        Buffer((ptr as u64) << 32 | len as u64)
    }

    /// Returns the contents of a buffer passed in by Bento, which are only valid
    /// until the current call returns.
    fn as_slice(&self) -> &[u8] {
        if *self == Self::EMPTY {
            return &[];
        }
        let (ptr, len) = ((self.0 >> 32) as usize as *const u8, self.0 as u32 as usize);
        unsafe { slice::from_raw_parts(ptr, len) }
    }
}

/// Allocates and retains a buffer for Bento to write into.
#[no_mangle]
pub extern "C" fn alloc(size: u32) -> *mut u8 {
    if size == 0 {
        return std::ptr::null_mut();
    }
    Box::into_raw(vec![0u8; size as usize].into_boxed_slice()) as *mut u8
}

/// Releases a buffer allocated by `alloc` or returned to Bento.
///
/// # Safety
///
/// Must only be called by Bento, once per buffer.
#[no_mangle]
pub unsafe extern "C" fn release(ptr: *mut u8, len: u32) {
    if !ptr.is_null() {
        drop(Box::from_raw(slice::from_raw_parts_mut(ptr, len as usize)));
    }
}
