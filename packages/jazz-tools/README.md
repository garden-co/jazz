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

If your platform is not supported in the npm package, install with Cargo from source.

## React Native alpha boundary

The `jazz-tools/react-native` entry point currently exposes compile-level binding
scaffolding only. Persistent React Native/Expo databases are not available in
this alpha: the default persistent configuration and the proposal-only
`sqliteStorage` option both fail before opening a driver. Explicit memory mode
has only been exercised by Node-based wiring tests, not Metro/Hermes or a device,
and is not a supported persistence alternative.
