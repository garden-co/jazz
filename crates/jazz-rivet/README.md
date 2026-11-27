# jazz-rivet

This Rust executable registers a `jazz` actor with Rivet. Each actor hosts a Jazz core and persists its data in its own Rivet SQLite database. Your TypeScript backend serves clients and connects to the actor through the `rivet()` store.

## Run locally

Install Rust 1.100 or newer, a native C/C++ build toolchain, and a Rivet 3.x Engine binary. From the TypeScript app directory, install the runner that matches the app’s Jazz version:

```sh
version=$(node -p 'require("./node_modules/jazz-tools/package.json").version')
cargo install --locked "jazz-rivet@$version"
```

Set `JAZZ_CORE_SECRET` to the same secret in the Jazz core and backend environments and start the core with:

```sh
export JAZZ_CORE_SECRET='<your-saved-secret>'
export RIVET_ENGINE_BINARY_PATH=/absolute/path/to/rivet-engine
export RIVET_POOL=jazz
jazz-rivet
```

See the [Rivet deployment guide](https://jazz.tools/docs/setup/deployment/rivet) for the TypeScript example and Rivet Cloud deployment steps.
