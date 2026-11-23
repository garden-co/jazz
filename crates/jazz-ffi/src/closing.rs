//! Whether a handle is closed: a level-triggered signal, so a wait on a
//! closed handle's behalf ends at once however late it starts.

use tokio::sync::watch;

/// One handle's (or session's) closed state. Clones share it.
#[derive(Clone)]
pub(crate) struct Closed(watch::Sender<bool>);

impl Closed {
    pub(crate) fn new() -> Self {
        Self(watch::Sender::new(false))
    }

    /// Closes it. Whether this call did (only the first does).
    pub(crate) fn close(&self) -> bool {
        self.0
            .send_if_modified(|closed| !std::mem::replace(closed, true))
    }

    pub(crate) fn is_closed(&self) -> bool {
        *self.0.borrow()
    }

    /// The signal, for the waits on the client's thread it ends.
    pub(crate) fn signal(&self) -> watch::Receiver<bool> {
        self.0.subscribe()
    }

    /// Ready once closed.
    pub(crate) async fn wait(&self) {
        // The sender is `self`'s, so the channel can't end first.
        let _ = self.signal().wait_for(|closed| *closed).await;
    }
}

/// Ready once `closed` is, if there is one.
pub(crate) async fn closed_if(closed: &Option<Closed>) {
    match closed {
        Some(closed) => closed.wait().await,
        None => std::future::pending().await,
    }
}
