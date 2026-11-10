// Build outputs need not exist during a source-only typecheck. Bundlers resolve
// the real module emitted by build:durable-objects when this provider is used.
declare module "*durable-object-worker/artifact.js" {
  export const manifest: {
    format: string;
    version: string;
    workerSha256: string;
    wasmSha256: string;
    digest: string;
  };
  export const workerGzip: string;
  export const wasmGzip: string;
}
declare module "jazz-tools/package.json" {
  const pkg: { version: string };
  export default pkg;
}
