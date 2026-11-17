//! Snippets for the docs: a Rust client signed in as one of the app's users
//! (the Rust tab beside Swift, Kotlin and Go). It syncs through the app's
//! Jazz endpoint, as a browser does.

use todo_server_docs::jazz_app;

/// The app's own sign-in, in the snippets.
pub mod my_sign_in {
    /// The signed-in user's access token, or `None` while no one is.
    pub async fn current_token() -> Option<String> {
        std::env::var("MY_APP_TOKEN").ok()
    }
}

/// Opens the app as an end user, with a device account or the app's own
/// sign-in, and waits until signed in.
pub async fn open_client(
    app_id: jazz_app::AppId,
    signed_in_with_my_auth: bool,
) -> jazz_app::Result<jazz_app::Jazz> {
    // #region context-setup-rust-client
    use futures_util::StreamExt;
    use jazz_app::{Auth, Config, DeviceKey, SessionState, Storage, TokenReason};

    // An end user's client syncs through your app's Jazz endpoint (its
    // backend's handler), never with the core directly.
    let auth = if signed_in_with_my_auth {
        // Your own sign-in's bearer token, asked for whenever one is needed:
        // on open, before it expires, and fresh after the backend refused it.
        Auth::bearer(|_reason: TokenReason| async { Ok(my_sign_in::current_token().await) })
    } else {
        // A device account: the key is the account. Keep it in your
        // platform's secure store; on a host without one, in a file only
        // this user can read.
        Auth::Device(DeviceKey::load_or_create(
            "./jazz-data/device.key".as_ref(),
        )?)
    };
    let jazz = jazz_app::open(
        Config::new(app_id)
            .server("https://app.example/jazz")
            .storage(Storage::Dir("./jazz-data".into())) // or Storage::Memory
            .user(auth),
    )
    .await?;
    let session = jazz.session().expect("an end user's client has a session");
    let mut states = session.changes();
    while let Some(state) = states.next().await {
        if let SessionState::SignedIn { user } = state {
            println!("signed in as {:?}", user.account);
            break;
        }
    }
    // #endregion context-setup-rust-client
    Ok(jazz)
}

/// A session that follows who is signed in, as TypeScript's
/// `createJazzClient` does.
pub async fn open_session(
    app_id: jazz_app::AppId,
) -> jazz_app::Result<(jazz_app::JazzSession, jazz_app::Jazz)> {
    // #region context-setup-rust-session
    use futures_util::StreamExt;
    use jazz_app::{
        AppFiles, Initial, JazzSession, SessionConfig, SessionStatus, Storage, TokenReason,
    };

    // A session follows who is signed in: a device account (a guest) until
    // your sign-in names a user, who takes the guest's account over if they
    // have none yet. Call `session.refresh()` when your sign-in changes.
    let session = JazzSession::open(
        AppFiles::new(jazz_app::APP, None),
        SessionConfig::new(app_id)
            .server("https://app.example/jazz")
            // Each account's store in a directory of its own, the device
            // account's key beside them.
            .storage(Storage::Dir("./jazz-data".into()))
            // `None` while no one is signed in.
            .bearer(|_reason: TokenReason| async { Ok(my_sign_in::current_token().await) })
            .initial(Initial::Device),
    )
    .await?;
    let mut snapshots = session.changes();
    while let Some(snapshot) = snapshots.next().await {
        if snapshot.status == SessionStatus::Ready {
            println!("signed in as {:?}", snapshot.user.map(|user| user.account));
            break;
        }
    }
    // The client for the user signed in now. Get it again when
    // `client_epoch` changes: one kept from before fails with
    // `Error::Replaced`.
    let jazz = session.jazz()?;
    // #endregion context-setup-rust-session
    Ok((session, jazz))
}
