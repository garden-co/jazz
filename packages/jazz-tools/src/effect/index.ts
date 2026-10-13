/**
 * Effect v4 bindings for Jazz: the {@link Jazz} service, live queries as
 * Streams, and transactions that commit or roll back with the Effect that
 * runs inside them. Server request handlers get the same service from
 * `jazz-tools/effect/backend`.
 */
export {
  Jazz,
  type JazzDb,
  type JazzLayerOptions,
  type JazzTransaction,
  type WaitOptions,
} from "./jazz.js";
export { JazzError, JazzWriteRejected } from "./errors.js";
