import { CojsonMessageChannel, type Peer } from "cojson";
import type {
  WaitForConnectionOptions,
  ExposeOptions,
  PostMessageTarget,
} from "cojson/src/CojsonMessageChannel/types.js";
import { Account, AnonymousJazzAgent } from "jazz-tools";
import { activeAccountContext } from "../tools/implementation/activeAccountContext.js";

/**
 * Options for JazzMessageChannel.expose()
 */
export interface JazzMessageChannelExposeOptions extends ExposeOptions {
  /**
   * The account or anonymous agent to use for the connection.
   * If not provided, falls back to the active account context.
   */
  loadAs?: Account | AnonymousJazzAgent;
}

/**
 * JazzMessageChannel provides a high-level API for creating Jazz connections
 * via the MessageChannel API. It wraps cojson's CojsonMessageChannel and
 * automatically manages the node connection.
 */
export class JazzMessageChannel {
  /**
   * Expose a Jazz connection to a target.
   * This is the host-side API, typically called from the main thread.
   *
   * @param target - Any object with a postMessage method (Worker, Window, etc.)
   * @param opts - Configuration options including the account to use
   * @returns A promise that resolves once the connection is established
   */
  static async expose(
    target: PostMessageTarget,
    opts: JazzMessageChannelExposeOptions = {},
  ): Promise<void> {
    const { loadAs, ...cojsonOpts } = opts;

    // Get account from loadAs or fall back to active account context
    const accountOrAgent = loadAs ?? activeAccountContext.maybeGet();

    if (!accountOrAgent) {
      throw new Error(
        "No account provided and no active account context available",
      );
    }

    const node =
      accountOrAgent instanceof AnonymousJazzAgent
        ? accountOrAgent.node
        : accountOrAgent.$jazz.localNode;

    const peer = await CojsonMessageChannel.expose(target, cojsonOpts);

    node.syncManager.addPeer(peer);
  }

  /**
   * Accept an incoming Jazz connection.
   * Same as cojson CojsonMessageChannel.waitForConnection().
   */
  static waitForConnection(opts?: WaitForConnectionOptions): Promise<Peer> {
    return CojsonMessageChannel.waitForConnection(opts);
  }
}

// Re-export types for convenience
export type {
  WaitForConnectionOptions,
  AcceptFromPortOptions,
} from "cojson/src/CojsonMessageChannel/types.js";
