// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//! OPFS (Origin Private File System) VFS implementation for WASM.
//!
//! This module provides a VFS implementation that uses the OPFS FileSystemSyncAccessHandle
//! API for synchronous file operations in Web Workers. This allows the VfsImpl trait to
//! remain completely synchronous while running in the browser.
//!
//! # Requirements
//!
//! - Must run in a Dedicated Web Worker (OPFS sync API is Worker-only)
//! - Requires HTTPS (secure context needed for OPFS)
//! - Build with: `RUSTFLAGS=--cfg=web_sys_unstable_apis`
//!
//! # Browser Support
//!
//! - Chrome 102+ (March 2023)
//! - Firefox 111+ (March 2023)
//! - Safari 15.2+ (partial)
//! - Edge 102+


use wasm_bindgen::prelude::*;
use web_sys::FileSystemSyncAccessHandle;

use super::{OffsetAlloc, VfsImpl};

/// OPFS-backed VFS implementation for WASM.
///
/// Uses the FileSystemSyncAccessHandle API which provides synchronous
/// read/write/flush operations when running in a Web Worker.
pub struct OpfsVfs {
    handle: FileSystemSyncAccessHandle,
    offset_alloc: OffsetAlloc,
}

impl OpfsVfs {
    /// Asynchronously open or create a file in OPFS.
    ///
    /// This must be called before any VfsImpl operations. The returned OpfsVfs
    /// can then be used synchronously.
    ///
    /// # Arguments
    ///
    /// * `path` - The filename to open/create in the OPFS root directory
    ///
    /// # Errors
    ///
    /// Returns a JsValue error if:
    /// - Not running in a Web Worker
    /// - OPFS is not available (not a secure context)
    /// - File operations fail
    pub async fn open(path: &str) -> Result<Self, JsValue> {
        // Get OPFS root directory via WorkerGlobalScope
        let global: web_sys::WorkerGlobalScope = js_sys::global().unchecked_into();
        let navigator = global.navigator();
        let storage = navigator.storage();

        let root_promise = storage.get_directory();
        let root: web_sys::FileSystemDirectoryHandle =
            wasm_bindgen_futures::JsFuture::from(root_promise)
                .await?
                .unchecked_into();

        // Get or create the file
        let mut opts = web_sys::FileSystemGetFileOptions::new();
        opts.create(true);
        let file_promise = root.get_file_handle_with_options(path, &opts);
        let file_handle: web_sys::FileSystemFileHandle =
            wasm_bindgen_futures::JsFuture::from(file_promise)
                .await?
                .unchecked_into();

        // Get synchronous access handle
        let sync_promise = file_handle.create_sync_access_handle();
        let handle: FileSystemSyncAccessHandle =
            wasm_bindgen_futures::JsFuture::from(sync_promise)
                .await?
                .unchecked_into();

        let size = handle.get_size().map_err(|e| e)? as usize;
        Ok(Self {
            handle,
            offset_alloc: OffsetAlloc::new_with(size),
        })
    }

    /// Close the OPFS file handle.
    ///
    /// This should be called when done with the file to ensure proper cleanup.
    /// After calling close(), the VFS should not be used.
    pub fn close(&self) {
        self.handle.close();
    }
}

impl VfsImpl for OpfsVfs {
    fn read(&self, offset: usize, buf: &mut [u8]) {
        let mut opts = web_sys::FileSystemReadWriteOptions::new();
        opts.at(offset as f64);
        // read_with_u8_array_and_options returns the number of bytes read
        let _bytes_read = self
            .handle
            .read_with_u8_array_and_options(buf, &opts)
            .expect("OPFS read failed");
    }

    fn write(&self, offset: usize, buf: &[u8]) {
        let mut opts = web_sys::FileSystemReadWriteOptions::new();
        opts.at(offset as f64);
        // write_with_u8_array_and_options returns the number of bytes written
        let _bytes_written = self
            .handle
            .write_with_u8_array_and_options(buf, &opts)
            .expect("OPFS write failed");
    }

    fn flush(&self) {
        self.handle.flush().expect("OPFS flush failed");
    }

    fn alloc_offset(&self, size: usize) -> usize {
        let offset = self.offset_alloc.alloc(size);
        let new_size = offset + size;

        // Extend file if needed
        let current_size = self
            .handle
            .get_size()
            .expect("OPFS get_size failed") as usize;
        if current_size < new_size {
            self.handle
                .truncate_with_u32(new_size as u32)
                .expect("OPFS truncate failed");
        }

        offset
    }

    fn dealloc_offset(&self, offset: usize) {
        // OPFS doesn't support sparse files, so we just track for potential reuse
        self.offset_alloc.dealloc_offset(offset);
    }

    fn open(_path: impl AsRef<std::path::Path>) -> Self
    where
        Self: Sized,
    {
        panic!("Use OpfsVfs::open().await for WASM - synchronous open is not supported")
    }
}

// Safety: WASM is single-threaded, so Send and Sync are safe
unsafe impl Send for OpfsVfs {}
unsafe impl Sync for OpfsVfs {}

impl Drop for OpfsVfs {
    fn drop(&mut self) {
        // Close the handle when dropped to ensure proper cleanup
        self.handle.close();
    }
}
