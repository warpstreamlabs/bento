/// Makes a message louder.
fn process(message: &[u8]) -> Vec<u8> {
    [message, b"!!!!111!!11!"].concat()
}

/// WebAssembly export that is called for each message being processed.
#[cfg_attr(target_arch = "wasm32", export_name = "process")]
pub unsafe extern "C" fn _process() {
    let ptr_size = _msg_as_bytes();

    let content_ptr = (ptr_size >> 32) as u32;
    let content_size = ptr_size as u32;
    let message = ptr_to_bytes(content_ptr, content_size);

    let processed = process(&message).into_boxed_slice();
    let (ptr, len) = bytes_to_ptr(&processed);
    std::mem::forget(processed);

    _msg_set_bytes(ptr, len);
}

//------------------------------------------------------------------------------

#[link(wasm_import_module = "bento_wasm")]
extern "C" {
    /// WebAssembly import for mutating Bento messages.
    ///
    /// Note: This is an ownership transfer: Bento copies the bytes and frees
    /// them with [`_deallocate`] before it returns.
    #[link_name = "v0_msg_set_bytes"]
    fn _msg_set_bytes(ptr: u32, size: u32);

    /// WebAssembly import for accessing Bento messages.
    ///
    /// Note: This is an ownership transfer: Bento allocates the bytes with
    /// [`_allocate`], and never frees them.
    #[link_name = "v0_msg_as_bytes"]
    fn _msg_as_bytes() -> u64;
}

//------------------------------------------------------------------------------

/// Returns bytes from WebAssembly compatible numeric types representing their
/// pointer and length.
///
/// Note: This takes ownership of the bytes, which were allocated by
/// [`_allocate`], so they're freed when they're dropped.
unsafe fn ptr_to_bytes(ptr: u32, len: u32) -> Box<[u8]> {
    Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr as *mut u8, len as usize))
}

/// Returns a pointer and size pair for the given bytes in a way compatible
/// with WebAssembly numeric types.
///
/// Note: This doesn't change the ownership of the bytes. To intentionally
/// leak them, use [`std::mem::forget`] on the input after calling this.
fn bytes_to_ptr(bytes: &[u8]) -> (u32, u32) {
    (bytes.as_ptr() as u32, bytes.len() as u32)
}

/// WebAssembly export that allocates a pointer (linear memory offset) that can
/// be used for a message.
///
/// This is an ownership transfer, which means the caller must call
/// [`_deallocate`] when finished.
#[cfg_attr(target_arch = "wasm32", export_name = "allocate")]
pub extern "C" fn _allocate(size: u32) -> *mut u8 {
    allocate(size as usize)
}

/// Allocates size bytes and leaks the pointer where they start.
fn allocate(size: usize) -> *mut u8 {
    Box::into_raw(vec![0u8; size].into_boxed_slice()) as *mut u8
}

/// WebAssembly export that deallocates a pointer of the given size (linear
/// memory offset, byteCount) allocated by [`_allocate`].
#[cfg_attr(target_arch = "wasm32", export_name = "deallocate")]
pub unsafe extern "C" fn _deallocate(ptr: u32, size: u32) {
    deallocate(ptr as *mut u8, size as usize);
}

/// Retakes the pointer which allows its memory to be freed.
///
/// Note: The size must be the allocation's, as it's freed as a boxed slice.
unsafe fn deallocate(ptr: *mut u8, size: usize) {
    drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, size)));
}
