// `build:backend`: the WASM a backend's `createJazz` runs on in a Cloudflare
// Worker (jazz-tools' `workerd` entry, in the app's backend Durable Object),
// built for size (see ../size-build.mjs): the default build plus the
// `backend` feature (`src/backend.rs`), which the browser build leaves out.
import { join } from "node:path";
import { root, sizeTunedBuild, wasmOptLevelFromArgs } from "../size-build.mjs";

sizeTunedBuild({
  label: "build:backend",
  features: "backend",
  outDir: join(root, "backend/pkg"),
  outName: "jazz_wasm_backend",
  wasmOptLevel: wasmOptLevelFromArgs(),
});
