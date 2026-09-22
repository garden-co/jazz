/**
 * Browser resolution stub for the Node-only foreground lease implementation.
 *
 * `DefaultRuntimeSource` has one public shape on every JavaScript host, but
 * durable foreground-node leases are a Node filesystem concern.  The package
 * `browser` map resolves the dynamic Node import to this module before a
 * browser bundler walks its dependency graph, so a client bundle never even
 * reaches `node:fs` or `node:crypto`.
 */
import type { ForegroundNodeLease } from "../runtime-source.js";

export type NodeForegroundNodeLeaseOptions = {
  appId: string;
  env: string;
  authScope: string;
};

export async function acquireNodeForegroundNodeLease(
  _options: NodeForegroundNodeLeaseOptions,
): Promise<ForegroundNodeLease> {
  throw new Error("Node foreground node leases are unavailable in browser bundles");
}
