// Keep browser receipt selection data-only: the package's Node gate must be
// able to assert this split without starting a second Vitest browser server.
export const topologyReceipt = "tests/browser/topology.e2e.test.ts";
export const providerReceipt = "tests/browser/provider.e2e.test.tsx";
