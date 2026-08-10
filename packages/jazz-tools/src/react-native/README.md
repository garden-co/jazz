# React Native bindings

`jazz-tools/react-native` runs the Jazz Rust core through the `jazz-rn`
Turbo Module. It does not load WASM and does not use a JavaScript SQLite
driver. One Rust actor thread owns the database; the TypeScript runtime keeps
WebSockets, subscriptions, and the public Jazz API on the JavaScript side.

Install `jazz-rn` directly in every React Native application because it is an
optional peer of `jazz-tools`. Rebuild the native app after installing or
upgrading it; Expo Go cannot load this custom native module.

Persistent storage uses the Rust Groove SQLite backend. Supply an absolute
filesystem `dataDirectory`; `dbName` remains a logical name and is sanitized
before the final path is formed as `<dataDirectory>/<dbName>.db`.

```tsx
import "jazz-tools/expo/polyfills";

import { expoDataDirectory } from "jazz-tools/expo";
import { JazzProvider } from "jazz-tools/react-native";

export function App() {
  return (
    <JazzProvider
      config={{
        appId: "todo-mobile",
        dataDirectory: expoDataDirectory(),
      }}
      fallback={null}
    >
      {/* application */}
    </JazzProvider>
  );
}
```

`withExpoDataDirectory(config)` is also available when constructing config
outside JSX. A memory driver does not require `dataDirectory` (the general
Jazz configuration currently requires a `serverUrl` for memory mode).
Expo's default is the app Documents directory; production apps should apply
their platform's backup-exclusion policy if the reconstructible database must
not be included in device backups.

Persistent node and author identities are derived deterministically from the
application, environment, user branch, authenticated subject, and logical
database name. Reopening the same database therefore resumes pending local
writes instead of orphaning its outbox. Attaching the first upstream transport
performs a synchronous bootstrap tick so replay does not depend on a foreign
callback that may have fired before the connection existed.

Native writes are durable by default. `Db.shutdown()` closes transports,
cancels pending native waiters, checkpoints SQLite, and joins the actor thread.
After a terminal native panic or close, construct a new client rather than
reusing the old instance.

For native development, bindings are generated from
`crates/jazz-rn/rust/src/lib.rs` with `ubrn`; iOS and Android builds package the
same Rust core, bundled SQLite, and zstd transport support.
