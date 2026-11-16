---
"jazz-tools": patch
---

Jazz for Swift: sessions as TS's React layer. `JazzApp.session(appId:serverUrl:bearer:…)` (generated) makes a session that opens in the background; `session.client()` waits until someone is signed in, and `session.clients` follows each user (TS's `followDb`). In SwiftUI, `JazzRoot(session) { … } signedOut: { … }` (TS's `<JazzProvider>`) shows your views with the signed-in user's client, `@JazzAll(query)` is a live query (`useAll`), and `@Environment(\.jazz)` / `@Environment(\.jazzSession)` are `useDb` / `useJazzSession`.
