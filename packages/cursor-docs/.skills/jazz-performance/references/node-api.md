# Node-API

Jazz uses a WASM-based crypto implementation that provides near-native performance while ensuring full compatibility across a wide variety of environments.

For even higher performance on Node.js or Deno, you can enable the native crypto (Node-API) implementation. Node-API is Node.js's native API for building modules in Native Code (Rust/C++) that interact directly with the underlying system, allowing for true native execution speed.

You can use it as follows:

```ts
import { startWorker } from "jazz-tools/worker";
import { NapiCrypto } from "jazz-tools/napi";

const { worker } = await startWorker({
  syncServer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  accountID: process.env.JAZZ_WORKER_ACCOUNT,
  accountSecret: process.env.JAZZ_WORKER_SECRET,
  crypto: await NapiCrypto.create(),
});
```

**Note:**

The Node-API implementation is not available on all platforms. It is only available on Node.js 20.x and higher. The supported platforms are:

* macOS (x64, ARM64)
* Linux (x64, ARM64, ARM, musl)

It does not work in edge runtimes.

## On Next.js

In order to use Node-API with Next.js, you need to tell Next.js to bundle the native modules in your build.

You can do this by adding the required packages to the `serverExternalPackages` array in your `next.config.js`.

**Note:** if you're deploying to Vercel, be sure to use the `nodejs` runtime!

```ts
// next.config.js

module.exports = {
  serverExternalPackages: [
    "cojson-core-napi",
    "cojson-core-napi-linux-x64-gnu",
    "cojson-core-napi-linux-x64-musl",
    "cojson-core-napi-linux-arm64-gnu",
    "cojson-core-napi-linux-arm64-musl",
    "cojson-core-napi-darwin-x64",
    "cojson-core-napi-darwin-arm64",
    "cojson-core-napi-linux-arm-gnueabihf",
  ],
};
```
