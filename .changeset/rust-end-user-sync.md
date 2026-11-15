---
"jazz-tools": patch
---

Rust: `jazz-app` opens an app as an end user signed in through the app's own backend (`Config::end_user(EndUser::new(...))`), as a React Native client is. The client signs in at `<backend>/apps/<id>/accounts/backend-sign-in` with the app's bearer, syncs over the backend's `/ws` with that bearer on the upgrade, and asks for a fresh bearer before it expires or when the backend refuses it. Below it, a `JazzClient` whose `AppContext` holds a `backend_session` and a `jwt_token` but no secret presents the token as the upgrade's `Authorization` header and needs no `bundle` (that combination was refused before).

`Jazz::signed_out` turns true if a renewed bearer names another user (the client then stops syncing). `JazzClient` gains `upstream_status_changes`, `update_relayed_sign_in` and `stop_upstream`.
