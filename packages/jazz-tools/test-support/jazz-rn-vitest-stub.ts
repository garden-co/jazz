class RnRuntime {}

const jazz_rn = {
  RnRuntime,
  mintLocalFirstToken: () => "local-first-token",
  mintAnonymousToken: () => "anonymous-token",
};

async function uniffiInitAsync(): Promise<void> {
  // No-op for Node-side tests.
}

export { jazz_rn, uniffiInitAsync };

export default {
  jazz_rn,
  uniffiInitAsync,
};
