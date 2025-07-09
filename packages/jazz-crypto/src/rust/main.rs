

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::slice;

// --- Internal Rust implementations ---

fn _no_args_return_string() -> String {
    "Hello from Rust!".to_string()
}

fn _args_return_string(arg1: String) -> String {
    format!("Hello, {}!", arg1)
}

fn _no_args_return_ab() -> Vec<u8> {
    vec![10, 20, 30, 40, 50]
}

fn _args_return_ab(arg1: Vec<u8>) -> Vec<u8> {
    arg1.into_iter().map(|x| x.wrapping_add(10)).collect()
}


// --- FFI Layer ---

// Function to deallocate memory for strings created in Rust
#[no_mangle]
pub extern "C"
fn free_rust_string(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    unsafe {
        let _ = CString::from_raw(s);
    }
}

// FFI wrapper for no_args_return_string
#[no_mangle]
pub extern "C"
fn no_args_return_string() -> *mut c_char {
    let result_str = _no_args_return_string();
    CString::new(result_str).unwrap().into_raw()
}

// FFI wrapper for args_return_string
#[no_mangle]
pub extern "C"
fn args_return_string(arg1: *const c_char) -> *mut c_char {
    let c_str = unsafe { CStr::from_ptr(arg1) };
    let rust_str = c_str.to_str().unwrap().to_string();
    let result_str = _args_return_string(rust_str);
    CString::new(result_str).unwrap().into_raw()
}

// Struct to pass byte array across FFI boundary
#[repr(C)]
pub struct ByteBuffer {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
}

impl From<Vec<u8>> for ByteBuffer {
    fn from(mut vec: Vec<u8>) -> Self {
        vec.shrink_to_fit();
        let ptr = vec.as_mut_ptr();
        let len = vec.len();
        let cap = vec.capacity();
        std::mem::forget(vec); // Prevent Rust from dropping the Vec
        ByteBuffer { ptr, len, cap }
    }
}

// Function to deallocate memory for byte buffers created in Rust
#[no_mangle]
pub extern "C"
fn free_rust_byte_buffer(buf: ByteBuffer) {
    if buf.ptr.is_null() {
        return;
    }
    unsafe {
        let _ = Vec::from_raw_parts(buf.ptr, buf.len, buf.cap);
    }
}

// FFI wrapper for no_args_return_ab
#[no_mangle]
pub extern "C"
fn no_args_return_ab() -> ByteBuffer {
    let result_vec = _no_args_return_ab();
    result_vec.into()
}

// FFI wrapper for args_return_ab
#[no_mangle]
pub extern "C"
fn args_return_ab(arg1_ptr: *const u8, arg1_len: usize) -> ByteBuffer {
    let rust_vec = unsafe { slice::from_raw_parts(arg1_ptr, arg1_len).to_vec() };
    let result_vec = _args_return_ab(rust_vec);
    result_vec.into()
}
