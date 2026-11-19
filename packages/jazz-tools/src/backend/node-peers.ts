/**
 * The optional peers a Node backend may load: literal specifiers, each
 * inside its own `try`, so Node bundlers (Turbopack, webpack) bundle the
 * package when it's installed and turn a missing one into an error at
 * runtime, and file tracers ship it. Only Node's entry imports this module:
 * a Cloudflare Worker's bundler would refuse a peer that isn't installed.
 */
import type { NodePeer } from "./backend-native.js";

export async function importNodePeer(peer: NodePeer): Promise<unknown> {
  switch (peer) {
    case "ws":
      try {
        return await import(/* @vite-ignore */ "ws");
      } catch (error) {
        throw peerError(error);
      }
    case "@vercel/functions":
      try {
        return await import(/* @vite-ignore */ "@vercel/functions");
      } catch (error) {
        throw peerError(error);
      }
    case "@vercel/functions/oidc":
      try {
        return await import(/* @vite-ignore */ "@vercel/functions/oidc");
      } catch (error) {
        throw peerError(error);
      }
    case "@vercel/sandbox":
      try {
        return await import(/* @vite-ignore */ "@vercel/sandbox");
      } catch (error) {
        throw peerError(error);
      }
  }
}

/** The import's own error, which callers name the missing peer from. */
function peerError(error: unknown): unknown {
  return error;
}
