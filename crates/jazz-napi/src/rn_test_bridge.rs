//! Test-only byte carrier for the exact RN owner/C ABI. No database semantics
//! live here. This proves the TypeScript/native command path, not mobile JSI.
use jazz_native_relay::*;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use std::{ffi::c_void, rc::Rc};

fn check(status: JazzNativeRelayStatus) -> Result<()> {
    if status == JazzNativeRelayStatus::Ok {
        Ok(())
    } else {
        Err(Error::from_reason(format!(
            "RN test bridge C ABI: {status:?}"
        )))
    }
}
fn bytes(
    call: impl FnOnce(*mut JazzNativeRelayBytes) -> JazzNativeRelayStatus,
) -> Result<Uint8Array> {
    let mut out = JazzNativeRelayBytes {
        data: std::ptr::null_mut(),
        len: 0,
    };
    let status = call(&mut out);
    let copy = if out.len == 0 {
        Vec::new()
    } else {
        // The ABI owns this allocation; always copy before releasing it.
        unsafe { std::slice::from_raw_parts(out.data, out.len).to_vec() }
    };
    unsafe { jazz_native_relay_bytes_free(&mut out) };
    check(status)?;
    Ok(copy.into())
}
struct Host {
    host: *mut JazzNativeRelayHost,
    lease: *mut JazzNativeRelayHostLease,
}
impl Drop for Host {
    fn drop(&mut self) {
        unsafe {
            jazz_native_relay_host_lease_invalidate_foreground_runtime(self.lease);
            jazz_native_relay_host_lease_free(self.lease);
            jazz_native_relay_host_free(self.host);
        }
    }
}
#[napi]
pub struct RnTestHost {
    inner: Rc<Host>,
}
#[napi]
impl RnTestHost {
    #[napi(constructor)]
    pub fn new() -> Self {
        let host = jazz_native_relay_host_new();
        let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
        Self {
            inner: Rc::new(Host { host, lease }),
        }
    }
    #[napi(getter)]
    pub fn abi_version(&self) -> u32 {
        jazz_native_relay_abi_version().into()
    }
    /// Fixture platform admission; intentionally absent from default builds.
    #[napi]
    pub fn admit(&self, config: String) -> Result<Uint8Array> {
        bytes(|out| unsafe {
            jazz_native_relay_host_admit_scope_json(
                self.inner.host,
                config.as_ptr(),
                config.len(),
                out,
            )
        })
    }
    #[napi]
    pub fn open_attached(&self, capability: Uint8Array) -> Result<RnTestForeground> {
        let mut handle = 0;
        check(unsafe {
            jazz_native_relay_host_lease_open_attached_foreground(
                self.inner.lease,
                capability.as_ptr(),
                capability.len(),
                &mut handle,
            )
        })?;
        Ok(RnTestForeground {
            host: self.inner.clone(),
            handle,
            closed: false,
        })
    }
}
#[napi]
pub struct RnTestForeground {
    host: Rc<Host>,
    handle: u64,
    closed: bool,
}
unsafe extern "C" fn wake(context: *mut c_void, _foreground: u64, kind: u8, _delay: u64) {
    let callback = context.cast::<ThreadsafeFunction<String, ()>>();
    if kind == 3 {
        // C ABI guarantees no further calls and synchronizes in-flight wakes.
        unsafe { drop(Box::from_raw(callback)) };
    } else {
        let urgency = if kind == 0 { "immediate" } else { "deferred" };
        unsafe { &*callback }.call(
            Ok(urgency.to_owned()),
            ThreadsafeFunctionCallMode::NonBlocking,
        );
    }
}
#[napi]
impl RnTestForeground {
    #[napi]
    pub fn execute(&self, command: Uint8Array) -> Result<Uint8Array> {
        bytes(|out| unsafe {
            jazz_native_relay_host_lease_execute_foreground(
                self.host.lease,
                self.handle,
                command.as_ptr(),
                command.len(),
                out,
            )
        })
    }
    #[napi]
    pub fn tick(&self) -> Result<()> {
        check(unsafe {
            jazz_native_relay_host_lease_tick_attached_foreground(self.host.lease, self.handle)
        })
    }
    #[napi]
    pub fn set_tick_scheduler(&self, callback: ThreadsafeFunction<String, ()>) -> Result<()> {
        let context = Box::into_raw(Box::new(callback));
        let result = check(unsafe {
            jazz_native_relay_host_lease_set_foreground_wake_callback(
                self.host.lease,
                self.handle,
                Some(wake),
                context.cast(),
            )
        });
        if result.is_err() {
            unsafe { drop(Box::from_raw(context)) };
        }
        result
    }
    #[napi]
    pub fn close(&mut self) -> Result<bool> {
        if self.closed {
            return Ok(false);
        }
        let mut closed = false;
        check(unsafe {
            jazz_native_relay_host_lease_close_attached_foreground(
                self.host.lease,
                self.handle,
                &mut closed,
            )
        })?;
        self.closed = true;
        Ok(closed)
    }
}
impl Drop for RnTestForeground {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
