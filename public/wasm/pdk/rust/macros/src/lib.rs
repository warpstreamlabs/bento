//! Attribute macros for Bento WASM plugins. Use them through `bento_pdk`.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Ident, ItemFn};

/// Exports a function as a plugin entry point, which is one of:
///
/// - `init`: `fn(config: C) -> Result<()>`, where `C` is deserialized from
///   the plugin's JSON config. Optional, and called once per instance.
/// - `process`: `fn(msg: Message) -> Result<Vec<Message>>`
/// - `process_batch`: `fn(batch: Vec<Message>) -> Result<Vec<Vec<Message>>>`
#[proc_macro_attribute]
pub fn bento_plugin(attr: TokenStream, item: TokenStream) -> TokenStream {
    let kind = parse_macro_input!(attr as Ident);
    let func = parse_macro_input!(item as ItemFn);
    let name = &func.sig.ident;

    let export = match kind.to_string().as_str() {
        "init" => quote! {
            #[unsafe(no_mangle)]
            pub extern "C" fn init_plugin(config: ::bento_pdk::Buffer) -> ::bento_pdk::Buffer {
                ::bento_pdk::init_plugin(config, super::#name)
            }
        },
        "process" => quote! {
            #[unsafe(no_mangle)]
            pub extern "C" fn process(input: ::bento_pdk::Buffer) -> *const ::bento_pdk::CallResult {
                ::bento_pdk::process(input, super::#name)
            }
        },
        "process_batch" => quote! {
            #[unsafe(no_mangle)]
            pub extern "C" fn process_batch(input: ::bento_pdk::Buffer) -> *const ::bento_pdk::CallResult {
                ::bento_pdk::process_batch(input, super::#name)
            }
        },
        _ => {
            return syn::Error::new(kind.span(), "expected init, process or process_batch")
                .to_compile_error()
                .into()
        }
    };

    // The export is generated in its own module so that it doesn't clash with
    // the function it calls, which may share its name.
    let module = format_ident!("__bento_{}", kind);
    quote! {
        #func

        #[doc(hidden)]
        mod #module {
            #export
        }
    }
    .into()
}
