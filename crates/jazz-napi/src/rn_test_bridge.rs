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

pub struct RnTestHost {
    inner: Option<Rc<Host>>,
}

impl RnTestHost {
    pub fn new() -> Self {
        let host = jazz_native_relay_host_new();
        let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
        Self {
            inner: Some(Rc::new(Host { host, lease })),
        }
    }

    pub fn close(&mut self) -> bool {
        self.inner.take().is_some()
    }

    pub fn abi_version(&self) -> u32 {
        jazz_native_relay_abi_version().into()
    }
    /// Fixture platform admission; intentionally absent from default builds.
    pub fn admit(&self, config: String) -> Result<Uint8Array> {
        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| Error::from_reason("RN fixture host is closed"))?;
        bytes(|out| unsafe {
            jazz_native_relay_host_admit_scope_json(inner.host, config.as_ptr(), config.len(), out)
        })
    }

    pub fn open_attached(&self, capability: Uint8Array) -> Result<RnTestForeground> {
        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| Error::from_reason("RN fixture host is closed"))?;
        let mut handle = 0;
        check(unsafe {
            jazz_native_relay_host_lease_open_attached_foreground(
                inner.lease,
                capability.as_ptr(),
                capability.len(),
                &mut handle,
            )
        })?;
        Ok(RnTestForeground {
            host: inner.clone(),
            handle,
            closed: false,
            wake: None,
        })
    }
}

pub struct RnTestForeground {
    host: Rc<Host>,
    handle: u64,
    closed: bool,
    wake: Option<Box<WakeCallback>>,
}
type WakeCallback = ThreadsafeFunction<String, (), String, napi::Status, false>;
unsafe extern "C" fn wake(context: *mut c_void, _foreground: u64, kind: u8, delay: u64) {
    if kind == 3 {
        return;
    }
    let urgency = match kind {
        0 => "immediate".to_owned(),
        1 => "deferred".to_owned(),
        _ => format!("after:{delay}"),
    };
    unsafe { &*context.cast::<WakeCallback>() }
        .call(urgency, ThreadsafeFunctionCallMode::NonBlocking);
}

impl RnTestForeground {
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

    pub fn tick(&self) -> Result<()> {
        check(unsafe {
            jazz_native_relay_host_lease_tick_attached_foreground(self.host.lease, self.handle)
        })
    }

    pub fn set_tick_scheduler(&mut self, callback: WakeCallback) -> Result<()> {
        let mut callback = Box::new(callback);
        let context = (&mut *callback as *mut WakeCallback).cast();
        check(unsafe {
            jazz_native_relay_host_lease_set_foreground_wake_callback(
                self.host.lease,
                self.handle,
                Some(wake),
                context,
            )
        })?;
        // Successful replacement synchronously inerts the old registration.
        // The C ABI does not send CANCELLED for replacement: the carrier owns
        // both boxes and releases the old one only after that boundary.
        self.wake = Some(callback);
        Ok(())
    }
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
        self.wake = None;
        Ok(closed)
    }
}
impl Drop for RnTestForeground {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

// Match the existing hidden NAPI test helpers. Externals carry Rust-owned
// objects with finalizers, never raw pointer integers or public package types.
#[napi(js_name = "__testRnHostNew", skip_typescript)]
pub fn host_new() -> External<RnTestHost> {
    External::new(RnTestHost::new())
}
#[napi(js_name = "__testRnHostAbiVersion", skip_typescript)]
pub fn host_abi_version(host: &External<RnTestHost>) -> u32 {
    host.abi_version()
}
#[napi(js_name = "__testRnHostAdmit", skip_typescript)]
pub fn host_admit(host: &External<RnTestHost>, config: String) -> Result<Uint8Array> {
    host.admit(config)
}
#[napi(js_name = "__testRnHostClose", skip_typescript)]
pub fn host_close(host: &mut External<RnTestHost>) -> bool {
    host.close()
}
#[napi(js_name = "__testRnHostOpenAttached", skip_typescript)]
pub fn host_open_attached(
    host: &External<RnTestHost>,
    capability: Uint8Array,
) -> Result<External<RnTestForeground>> {
    host.open_attached(capability).map(External::new)
}
#[napi(js_name = "__testRnForegroundExecute", skip_typescript)]
pub fn foreground_execute(
    foreground: &External<RnTestForeground>,
    command: Uint8Array,
) -> Result<Uint8Array> {
    foreground.execute(command)
}
#[napi(js_name = "__testRnForegroundTick", skip_typescript)]
pub fn foreground_tick(foreground: &External<RnTestForeground>) -> Result<()> {
    foreground.tick()
}
#[napi(js_name = "__testRnForegroundSetTickScheduler", skip_typescript)]
pub fn foreground_set_tick_scheduler(
    foreground: &mut External<RnTestForeground>,
    callback: WakeCallback,
) -> Result<()> {
    foreground.set_tick_scheduler(callback)
}
#[napi(js_name = "__testRnForegroundClose", skip_typescript)]
pub fn foreground_close(foreground: &mut External<RnTestForeground>) -> Result<bool> {
    foreground.close()
}
