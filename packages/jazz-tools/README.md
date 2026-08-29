# jazz-tools

CLI package for Jazz 2.

## Usage

```bash
npx jazz-tools@alpha server
```

To use a specific prerelease:

```bash
npx jazz-tools@2.0.0-alpha.0 server
```

## Supported binary targets

- macOS arm64
- macOS x64
- Linux x64
- Linux arm64
- Windows x64

If your platform is not supported in the npm package, install with Cargo from source.

## React Native alpha boundary

`jazz-tools/react-native` has a narrow native-foreground alpha: with a matching
installed `jazz-rn` development/release artifact and an opaque capability issued
by trusted platform admission, it opens a normal in-memory foreground Db
attached to the native SQLite relay. Local-first schema-backed query,
subscription, and ordinary full-cell write transactions use Jazz's regular API.
It is not yet a device-support claim: remote tiers, large values, and several
advanced operation families remain unavailable, and Android/iOS device receipts
are still required. Configurations without the platform-issued capability, and
the proposal-only `sqliteStorage` option, still fail before opening a driver.
