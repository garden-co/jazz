// adapter-node takes the public origin from ORIGIN; this app configures it as
// APP_ORIGIN, so default one from the other before the server starts.
process.env.ORIGIN ??= process.env.APP_ORIGIN;
await import("./build/index.js");
