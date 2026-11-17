# StagePlan in SwiftUI

A small SwiftUI app (iOS 17 and macOS 14) over StagePlan's Swift bindings
(`../swift-cli/JazzApp.swift`): it lists a show's tasks with `@JazzAll`, which
keeps them current, and adds tasks. It opens a local client so it runs
without a server, and puts it in the environment (`.environment(\.jazz, …)`).
Your app keeps a `JazzApp.session(appId:serverUrl:…)` and wraps its views in
`JazzRoot(session) { … } signedOut: { … }`, which does that for the signed-in
user.

```bash
brew install xcodegen
xcodegen generate          # writes StagePlanUI.xcodeproj
open StagePlanUI.xcodeproj # run the StagePlanUI scheme on an iPhone simulator or My Mac
```

It depends on `garden-co/jazz-swift`. CI (`.github/workflows/swift-mirror.yml`)
builds it for macOS and runs its UI test on an iPhone simulator, against the
release's package before it's published.
