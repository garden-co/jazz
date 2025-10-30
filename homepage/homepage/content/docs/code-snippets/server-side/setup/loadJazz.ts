import "jazz-tools/load-edge-wasm";
// Other Jazz Imports

export default {
  // @ts-expect-error Cloudflare specific request type
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    // Jazz application logic
    return new Response("Hello from Jazz on Cloudflare!");
  },
};
