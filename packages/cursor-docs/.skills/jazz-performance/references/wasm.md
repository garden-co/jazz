# WASM Initialisation

If you want to initialize the WASM asynchronously (**suggested**), you can use the `initWasm` function. Otherwise, the WASM will be initialized synchronously and will block the main thread (**not recommended**).

```ts
import { initWasm } from "jazz-tools/wasm";

await initWasm();

// Your code here...
```

## Edge runtimes

On some edge platforms, such as Cloudflare Workers or Vercel Edge Functions, environment security restrictions may trigger WASM crypto to fail.

To avoid this failure, you can ensure that Jazz uses the WASM implementation by importing the WASM loader before using Jazz. For example:

```ts
import "jazz-tools/load-edge-wasm";
// Other Jazz Imports

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    // Jazz application logic
    return new Response("Hello from Jazz on Cloudflare!");
  },
};
```

Currently, the Jazz Loader is tested on the following edge environments:

* Cloudflare Workers
* Vercel Functions

## Requirements

* Edge runtime environment that supports WebAssembly
* `jazz-tools/load-edge-wasm` must be imported before any Jazz import
