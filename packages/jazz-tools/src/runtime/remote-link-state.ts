import { DerivedStatePublisher } from "./sync-status.js";

/**
 * Live reachability of the upstream server, as observed by one host.
 *
 * - `none`: no server is configured, so no remote can hold other data.
 * - `connecting`: a server is configured and its first connection attempt
 *   (or an explicit reconnect) has not settled yet.
 * - `connected`: the upstream transport is admitted.
 * - `unavailable`: the application disconnected explicitly, the connection
 *   dropped and is retrying, or the attempt failed.
 *
 * Hosts report it to the core Db, which uses it only to decide whether a
 * local-first read's first load may wait for the server. It never
 * changes write durability or strict `ReadTier.Remote` reads.
 */
export type RemoteLinkState = "none" | "connecting" | "connected" | "unavailable";

/** Publishes {@link RemoteLinkState} changes; see {@link DerivedStatePublisher}. */
export class RemoteLinkStatePublisher extends DerivedStatePublisher<RemoteLinkState> {}
