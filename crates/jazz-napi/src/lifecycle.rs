//! Node owns facade lifetimes; the shared Binding owns upload semantics.
//! Weak registries let a closing facade retire retained handles without making
//! the runtime keep itself alive. Cleanup failures retain bounded diagnostics.
use super::*;
use futures::future::{LocalBoxFuture, Shared, WeakShared};
use std::collections::BTreeMap;
use std::rc::Weak;

type CloseResult = std::result::Result<(), Rc<HostError>>;
type CloseFuture = LocalBoxFuture<'static, CloseResult>;

#[derive(Default)]
pub(super) struct CleanupFailures {
    entries: Vec<(u64, String)>,
    additional: u64,
}
impl CleanupFailures {
    fn record(&mut self, sequence: u64, error: String) {
        self.entries.push((sequence, error));
        self.compact();
    }
    fn merge(&mut self, other: Self) {
        self.entries.extend(other.entries);
        self.additional = self.additional.saturating_add(other.additional);
        self.compact();
    }
    fn compact(&mut self) {
        self.entries.sort_by_key(|(sequence, _)| *sequence);
        self.additional = self
            .additional
            .saturating_add(self.entries.len().saturating_sub(2) as u64);
        self.entries.truncate(2);
    }
    fn message(&self) -> Option<String> {
        let (sequence, first) = self.entries.first()?;
        let mut message = format!("streaming cleanup failed at sequence {sequence}: {first}");
        if let Some((_, second)) = self.entries.get(1) {
            message.push_str(&format!("; secondary: {second}"));
        }
        if self.additional != 0 {
            message.push_str(&format!(
                "; {} additional cleanup failure(s)",
                self.additional
            ));
        }
        Some(message)
    }
}

pub(super) struct View {
    id: u64,
    host: Weak<Host>,
    pub(super) live: Rc<Cell<bool>>,
    uploads: RefCell<BTreeMap<u64, Weak<RefCell<Option<Upload>>>>>,
    resources: RefCell<BTreeMap<u64, Weak<dyn Retire>>>,
    initializations: RefCell<BTreeMap<String, Binding>>,
    failures: RefCell<CleanupFailures>,
    close: RefCell<Option<WeakShared<CloseFuture>>>,
    closed: RefCell<Option<CloseResult>>,
}
impl View {
    pub(super) fn new(host: &Rc<Host>) -> Rc<Self> {
        let id = host.next_resource.get();
        host.next_resource.set(
            id.checked_add(1)
                .expect("native lifetime identity exhausted"),
        );
        let view = Rc::new(Self {
            id,
            host: Rc::downgrade(host),
            live: Rc::new(Cell::new(true)),
            uploads: RefCell::default(),
            resources: RefCell::default(),
            initializations: RefCell::default(),
            failures: RefCell::default(),
            close: RefCell::default(),
            closed: RefCell::default(),
        });
        host.views.borrow_mut().insert(id, Rc::downgrade(&view));
        view
    }
    fn record_failure(&self, error: HostError) {
        let Some(host) = self.host.upgrade() else {
            return;
        };
        let sequence = host.next_resource.get();
        host.next_resource.set(sequence.saturating_add(1));
        self.failures
            .borrow_mut()
            .record(sequence, error.to_string());
        host.request_tick();
    }
    pub(super) fn retire(&self) {
        self.live.set(false);
        let resources = std::mem::take(&mut *self.resources.borrow_mut());
        for resource in resources
            .into_values()
            .filter_map(|resource| resource.upgrade())
        {
            resource.retire();
        }
        for (token, binding) in std::mem::take(&mut *self.initializations.borrow_mut()) {
            if let Some(host) = self.host.upgrade() {
                host.initialization_views.borrow_mut().remove(&token);
            }
            if binding.check_initialization_token(&token).is_ok()
                && let Err(error) = binding.cancel_initialization_transaction(&token)
            {
                self.record_failure(error);
            }
        }
        for resource in std::mem::take(&mut *self.uploads.borrow_mut()).into_values() {
            if let Some(resource) = resource.upgrade()
                && let Err(error) = abort(&resource)
            {
                self.record_failure(error);
            }
        }
    }
    pub(super) fn track_initialization(self: &Rc<Self>, token: &str, binding: &Binding) {
        if let Some(host) = self.host.upgrade() {
            host.initialization_views
                .borrow_mut()
                .insert(token.to_owned(), Rc::downgrade(self));
        }
        self.initializations
            .borrow_mut()
            .insert(token.to_owned(), binding.clone());
    }
    pub(super) fn forget_initialization(&self, token: &str) {
        if let Some(host) = self.host.upgrade() {
            let owner = host.initialization_views.borrow_mut().remove(token);
            if let Some(owner) = owner.and_then(|view| view.upgrade()) {
                owner.initializations.borrow_mut().remove(token);
            }
        }
    }
}

trait Retire {
    fn retire(&self);
}
struct Slot<T> {
    value: RefCell<Option<T>>,
    cancel: RefCell<Option<Box<dyn FnOnce()>>>,
}
impl<T> Retire for Slot<T> {
    fn retire(&self) {
        self.value.borrow_mut().take();
        if let Some(cancel) = self.cancel.borrow_mut().take() {
            cancel();
        }
    }
}

/// Own a read, stream or pending seal until completion, cancellation or facade
/// retirement. The view stores only a weak reference; a retained JS handle
/// cannot keep a closed view's underlying query registered.
pub(super) struct Resource<T> {
    slot: Rc<Slot<T>>,
    registration: Option<(u64, Rc<View>)>,
}
impl<T: 'static> Resource<T> {
    pub(super) fn new(view: Option<&Rc<View>>, value: T) -> Self {
        let slot = Rc::new(Slot {
            value: RefCell::new(Some(value)),
            cancel: RefCell::default(),
        });
        let registration = view.map(|view| {
            let host = view.host.upgrade().expect("live native host");
            let id = host.next_resource.get();
            host.next_resource.set(
                id.checked_add(1)
                    .expect("native lifetime identity exhausted"),
            );
            let erased: Rc<dyn Retire> = slot.clone();
            view.resources
                .borrow_mut()
                .insert(id, Rc::downgrade(&erased));
            (id, view.clone())
        });
        Self { slot, registration }
    }
    pub(super) fn on_cancel(self, cancel: impl FnOnce() + 'static) -> Self {
        *self.slot.cancel.borrow_mut() = Some(Box::new(cancel));
        self
    }
    pub(super) fn borrow_mut(&self) -> std::cell::RefMut<'_, Option<T>> {
        self.slot.value.borrow_mut()
    }
    pub(super) fn complete(&self) {
        self.slot.cancel.borrow_mut().take();
    }
    pub(super) fn cancel(&self) {
        self.slot.retire();
    }
}
impl<T> Drop for Resource<T> {
    fn drop(&mut self) {
        self.slot.retire();
        if let Some((id, view)) = &self.registration {
            view.resources.borrow_mut().remove(id);
            if let Some(host) = view.host.upgrade() {
                host.request_tick();
            }
        }
    }
}
impl Drop for View {
    fn drop(&mut self) {
        self.retire();
        if let Some(host) = self.host.upgrade() {
            host.views.borrow_mut().remove(&self.id);
            host.cleanup_failures
                .borrow_mut()
                .merge(std::mem::take(&mut *self.failures.borrow_mut()));
        }
    }
}

enum Upload {
    Streaming(binding_host::StreamingMutation),
    Staged(Box<binding_host::StagedStreamingMutation>),
}
fn abort(resource: &RefCell<Option<Upload>>) -> HostResult<bool> {
    match resource.borrow_mut().take() {
        None => Ok(false),
        Some(Upload::Streaming(mut mutation)) => mutation.abort(),
        Some(Upload::Staged(mut staged)) => staged.abort(),
    }
}
pub(super) struct UploadHandle {
    pub(super) inner: Inner,
    id: u64,
    resource: Rc<RefCell<Option<Upload>>>,
}
impl UploadHandle {
    pub(super) fn new(inner: Inner, mutation: binding_host::StreamingMutation) -> Self {
        let id = inner.host.next_resource.get();
        inner.host.next_resource.set(
            id.checked_add(1)
                .expect("native lifetime identity exhausted"),
        );
        let resource = Rc::new(RefCell::new(Some(Upload::Streaming(mutation))));
        inner
            .view
            .uploads
            .borrow_mut()
            .insert(id, Rc::downgrade(&resource));
        Self {
            inner,
            id,
            resource,
        }
    }
    pub(super) fn push(&self, bytes: &[u8]) -> napi::Result<()> {
        self.inner.ensure_open()?;
        let mut resource = self.resource.borrow_mut();
        let Some(Upload::Streaming(mutation)) = resource.as_mut() else {
            return Err(closed());
        };
        mutation.push(bytes).map_err(host_error)
    }
    pub(super) fn finish(&self) -> napi::Result<binding_host::Write> {
        self.inner.ensure_open()?;
        let mut resource = self.resource.borrow_mut();
        let Some(Upload::Streaming(mutation)) = resource.as_mut() else {
            return Err(closed());
        };
        let write = mutation.finish().map_err(host_error)?;
        resource.take();
        Ok(write)
    }
    pub(super) fn stage(&self) -> napi::Result<()> {
        self.inner.ensure_open()?;
        let mut resource = self.resource.borrow_mut();
        let Some(Upload::Streaming(mutation)) = resource.as_mut() else {
            return Err(closed());
        };
        let staged = mutation.stage().map_err(host_error)?;
        *resource = Some(Upload::Staged(Box::new(staged)));
        Ok(())
    }
    pub(super) fn attach(&self, transaction: &str) -> napi::Result<()> {
        self.inner.ensure_open()?;
        let Some(Upload::Staged(mut staged)) = self.resource.borrow_mut().take() else {
            return Err(closed());
        };
        staged.attach(transaction).map_err(host_error)
    }
    pub(super) fn abort(&self) -> napi::Result<bool> {
        abort(&self.resource).map_err(host_error)
    }
}
impl Drop for UploadHandle {
    fn drop(&mut self) {
        self.inner.view.uploads.borrow_mut().remove(&self.id);
        if let Err(error) = abort(&self.resource) {
            self.inner.view.record_failure(error);
        }
        // Core upload leases schedule bounded reclamation on their normal tick.
        self.inner.binding.hooks.schedule_tick();
    }
}

fn copy_error(error: &HostError) -> HostError {
    match error {
        HostError::Engine(error) => {
            HostError::Engine(CoreError::new(error.code, error.message.clone()))
        }
        HostError::Argument(message) => HostError::Argument(message.clone()),
    }
}
fn with_cleanup(mut error: HostError, failures: &CleanupFailures) -> HostError {
    if let Some(cleanup) = failures.message() {
        let message = match &mut error {
            HostError::Engine(error) => &mut error.message,
            HostError::Argument(message) => message,
        };
        message.push_str("; secondary: ");
        message.push_str(&cleanup);
    }
    error
}
fn joined(future: Shared<CloseFuture>) -> ReadFuture {
    Box::pin(async move {
        future
            .await
            .map(|()| Vec::new())
            .map_err(|error| copy_error(&error))
    })
}

pub(super) fn close(inner: Inner, owner: bool) -> ReadFuture {
    if let Some(result) = inner.view.closed.borrow().clone() {
        return Box::pin(async move {
            result
                .map(|()| Vec::new())
                .map_err(|error| copy_error(&error))
        });
    }
    if let Some(future) = inner
        .view
        .close
        .borrow()
        .as_ref()
        .and_then(WeakShared::upgrade)
    {
        return joined(future);
    }
    if owner {
        inner.host.closing.set(true);
        inner.binding.db.stop_mutation_admission();
        let views = inner
            .host
            .views
            .borrow()
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        for view in views {
            view.retire();
        }
        inner.binding.close_initializations();
    } else {
        inner.view.retire();
    }
    inner.binding.hooks.progress.wake_all();
    let view = inner.view.clone();
    let future: CloseFuture = Box::pin(async move {
        let _drain = inner.host.cleanup_drain.lock().await;
        // The owner already drained and retired this view. A later facade
        // close must not touch the fenced SQLite connection or its new owner.
        if inner.host.closed.get() {
            return Ok(());
        }
        // A failed tick can require later receipt or cleanup progress. Keep
        // owner access, the lease and the tick route until a retry succeeds.
        let close_result = inner.binding.tick().await;
        let mut failures = std::mem::take(&mut *inner.view.failures.borrow_mut());
        if owner {
            for view in inner.host.views.borrow().values().filter_map(Weak::upgrade) {
                failures.merge(std::mem::take(&mut *view.failures.borrow_mut()));
            }
            failures.merge(std::mem::take(
                &mut *inner.host.cleanup_failures.borrow_mut(),
            ));
        }
        // Report this attempt's cleanup failures once, alongside its primary
        // storage error. A successful retry must not replay stale diagnostics.
        if let Err(error) = close_result {
            return Err(Rc::new(with_cleanup(error, &failures)));
        }
        if owner {
            if let Err(error) = inner.binding.db.retire_storage_owner() {
                return Err(Rc::new(with_cleanup(HostError::Engine(error), &failures)));
            }
            inner.host.closed.set(true);
            inner.host.tick_scheduler.borrow_mut().take();
            inner.host.mutation_errors.borrow_mut().take();
            inner.host.upstream_mutation_errors.borrow_mut().take();
            inner.host.file_lock.borrow_mut().take();
        }
        let result = failures
            .message()
            .map_or(Ok(()), |message| Err(Rc::new(HostError::Argument(message))));
        if !owner {
            inner.host.cleanup_failures.borrow_mut().merge(failures);
        }
        *inner.view.closed.borrow_mut() = Some(result.clone());
        settle_promises();
        result
    });
    let future = future.shared();
    *view.close.borrow_mut() = future.downgrade();
    joined(future)
}

#[cfg(test)]
#[path = "tests/lifecycle.rs"]
mod tests;
