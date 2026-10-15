// Bounded, ephemeral known-row routing. A serving relay is trusted by its client;
// it proxies only replies correlated to its selected authenticated upstream.
import { toHex, equalBytes } from "../../support/bytes.js";
import { uuidV4 } from "../../support/hash.js";
import { deferred } from "../../support/runtime.js";
import {
  type AuthorityContext,
  type CurrentRowsReceipt,
  type CurrentRowsRequest,
  MAX_CURRENT_ROWS,
  type PolicyBindingKey,
  type SyncMessage,
  currentRowCoordinateEquals,
  handleTransportBackpressure,
  policyBindingKeyEquals,
  policyBindingKeyId,
  queueSyncContextControl,
  scheduleTickIn,
  transportError,
} from "../../_extern/db-rest-wasm.js";

const MAX_PENDING = 64;
type Id = Uint8Array;
const idKey = (id: Id) => toHex(id);

export type CurrentRowsResult =
  | { kind: "Applied"; receipt: CurrentRowsReceipt }
  | { kind: "Unknown" };

export interface CurrentRowsRoute {
  request: CurrentRowsRequest;
  context: PolicyBindingKey;
  upstream?: AuthorityContext;
  downstream?: [number, Id];
  sender?: (r: CurrentRowsResult) => void;
}

interface CurrentRowsFloor {
  cut: number;
  progress: number;
  policy: number;
  protectedRequests: Set<string>;
}

export class CurrentRowsRouter {
  routes = new Map<string, CurrentRowsRoute>();
  responses = new Map<number, CurrentRowsReceipt[]>();
  cancels: [AuthorityContext, Id][] = [];
  progress = new Map<number, number>();
  /** Floors are per exact context and Core epoch, never global policy state. */
  floors = new Map<string, CurrentRowsFloor>();

  admit(route: CurrentRowsRoute): boolean {
    let queued = 0;
    for (const q of this.responses.values()) queued += q.length;
    if (this.routes.size + this.cancels.length + queued >= MAX_PENDING) return false;
    this.routes.set(idKey(route.request.requestId), route);
    return true;
  }

  finish(id: string, receipt: CurrentRowsReceipt | undefined): void {
    const route = this.routes.get(id);
    if (!route) return;
    this.routes.delete(id);
    if (route.downstream) {
      const [epoch, downstreamId] = route.downstream;
      const r = receipt ?? unknownReceipt(route.request, route.context);
      r.requestId = downstreamId;
      let q = this.responses.get(epoch);
      if (!q) this.responses.set(epoch, (q = []));
      q.push(r);
    } else if (route.sender) {
      route.sender(receipt ? { kind: "Applied", receipt } : { kind: "Unknown" });
    }
  }

  private dropRoute(id: string): void {
    const route = this.routes.get(id);
    this.routes.delete(id);
    if (route?.upstream) this.cancels.push([route.upstream, route.request.requestId]);
  }

  cancelDownstream(epoch: number, id: Id): void {
    for (const [key, route] of [...this.routes])
      if (route.downstream && route.downstream[0] === epoch && equalBytes(route.downstream[1], id))
        this.dropRoute(key);
  }

  disconnect(epoch: number): void {
    for (const [id, route] of [...this.routes]) {
      const down = route.downstream?.[0] === epoch;
      if (!down && route.upstream?.connectionId !== epoch) continue;
      if (down) this.dropRoute(id);
      else this.finish(id, undefined);
    }
    this.responses.delete(epoch);
    this.progress.delete(epoch);
    this.cancels = this.cancels.filter(([u]) => u.connectionId !== epoch);
    // Retain monotonic floors: a disconnect never grants replay authority.
  }
}

export function validRequest(request: CurrentRowsRequest): boolean {
  const rows = request.rows;
  return (
    rows.length > 0 &&
    rows.length <= MAX_CURRENT_ROWS &&
    rows.every((row: any) => row.table.length > 0 && row.table.length <= 256) &&
    rows.every((row: any, i: number) =>
      rows.slice(0, i).every((prev: any) => !currentRowCoordinateEquals(prev, row)),
    )
  );
}

export function unknownReceipt(
  request: CurrentRowsRequest,
  context: PolicyBindingKey,
): CurrentRowsReceipt {
  return {
    requestId: request.requestId,
    rows: request.rows.slice(),
    outcomes: request.rows.map(() => ({ kind: "Unknown" })),
    context,
    core: new Uint8Array(16),
    coreEpoch: 0,
    claimsRevision: 0,
    policyEpoch: 0,
    settledThrough: 0,
    authorizationProgress: 0,
    versionCarriers: [],
  };
}

/** Node::request_current_rows: internal pilot hook. */
export function requestCurrentRows(node: any, rows: any[], context: PolicyBindingKey) {
  return requestCurrentRowsForOwner(
    node.currentRows,
    node.scheduler,
    node.admittedUpstreamAuthority !== undefined,
    rows,
    context,
  );
}

/** Owned requester shared by local operations and relay repair owners. */
export async function requestCurrentRowsForOwner(
  router: CurrentRowsRouter,
  scheduler: any,
  hasUpstream: boolean,
  rows: any[],
  context: PolicyBindingKey,
): Promise<CurrentRowsResult> {
  const id = uuidV4();
  const done = deferred<CurrentRowsResult>();
  const request: CurrentRowsRequest = { requestId: id, rows, delegatedSession: undefined };
  if (validRequest(request) && hasUpstream)
    router.admit({ request, context, sender: done.resolve });
  else done.resolve({ kind: "Unknown" });
  scheduleTickIn(scheduler, "Immediate");
  try {
    return await done.promise;
  } finally {
    // Guard: cancel the upstream request if still routed.
    const key = idKey(id);
    const route = router.routes.get(key);
    router.routes.delete(key);
    if (route?.upstream) router.cancels.push([route.upstream, id]);
    scheduleTickIn(scheduler, "Immediate");
  }
}

function sendCurrentRows(conn: any, message: SyncMessage): boolean {
  // Requests on an upstream link go after the read barrier naming the writes
  // sent before them (SPEC 8 §8.2.1).
  if (conn.link.kind === "Upstream") {
    const barrier = conn.link.state.readBarrier.pendingBarrier();
    if (barrier) {
      if (!sendCurrentRowsMessage(conn, barrier)) return false;
      conn.link.state.readBarrier.barrierAccepted();
    }
  }
  return sendCurrentRowsMessage(conn, message);
}

function sendCurrentRowsMessage(conn: any, message: SyncMessage): boolean {
  try {
    conn.transport.send(message);
    return true;
  } catch (error) {
    if (handleTransportBackpressure(conn.node, conn.scheduler, error)) return false;
    throw transportError(error);
  }
}

/** PeerConnection::pump_current_rows */
export function pumpCurrentRows(conn: any): void {
  const router: CurrentRowsRouter = conn.currentRows;
  if (conn.link.kind === "Subscriber") {
    const queue = router.responses.get(conn.connectionEpoch);
    while (queue?.length)
      queueSyncContextControl(conn.pendingControlResponses, {
        kind: "CurrentRowsReceipt",
        receipt: queue.shift(),
      });
    return;
  }
  if (conn.link.kind !== "Upstream") return;
  const expected: AuthorityContext | undefined = conn.link.state.expectedScopeAuthority;
  if (!expected) return;
  if (!conn.admittedUpstreamAuthority?.sameAdmittedLink(expected)) return;
  for (const [id, route] of [...router.routes])
    if (route.upstream && !route.upstream.sameAdmittedLink(expected)) router.finish(id, undefined);
  for (const [id, route] of [...router.routes]) {
    if (route.upstream) continue;
    const request = { ...route.request };
    const scopedSession =
      conn.node.clientRelayScope()?.admitsSession(route.context.identity) ?? false;
    if (conn.transport.permitsDelegatedSessions() || scopedSession) {
      request.delegatedSession = {
        identity: route.context.identity,
        claims: route.context.claims(),
      };
    } else if (route.downstream || !equalBytes(route.context.identity, expected.link)) {
      router.finish(id, undefined);
      continue;
    }
    if (!sendCurrentRows(conn, { kind: "CurrentRowsRequest", request })) return;
    const live = router.routes.get(id);
    if (live) live.upstream = expected;
  }
  for (;;) {
    const index = router.cancels.findIndex(([u]) => u.sameAdmittedLink(expected));
    if (index < 0) break;
    const requestId = router.cancels[index][1];
    if (!sendCurrentRows(conn, { kind: "CurrentRowsCancel", requestId })) return;
    router.cancels.splice(index, 1);
  }
}

export async function receiveCurrentRows(
  node: any,
  router: CurrentRowsRouter,
  selected: AuthorityContext | undefined,
  expected: AuthorityContext | undefined,
  eligible: boolean,
  receipt: CurrentRowsReceipt,
): Promise<void> {
  if (!expected) return;
  const id = idKey(receipt.requestId);
  const route = router.routes.get(id);
  const valid =
    route !== undefined &&
    eligible &&
    selected?.sameAdmittedLink(expected) === true &&
    route.upstream?.sameAdmittedLink(expected) === true &&
    receipt.rows.length === route.request.rows.length &&
    receipt.rows.every((r: any, i: number) =>
      currentRowCoordinateEquals(r, route.request.rows[i]),
    ) &&
    policyBindingKeyEquals(receipt.context, route.context) &&
    receipt.outcomes.length === receipt.rows.length;
  if (!valid) return;
  if (receipt.outcomes.every((o: any) => o.kind === "Unknown")) {
    if (!receipt.versionCarriers.length) router.finish(id, undefined);
    return;
  }
  if (
    receipt.core.every((b: number) => b === 0) ||
    receipt.coreEpoch === 0 ||
    receipt.authorizationProgress === 0
  ) {
    // Unusable evidence answers nothing: release the route now (INV-SYNC-48).
    router.finish(id, undefined);
    return;
  }
  const key = `${toHex(receipt.core)}:${receipt.coreEpoch}:${policyBindingKeyId(receipt.context)}`;
  const floor = router.floors.get(key);
  if (
    floor &&
    (receipt.settledThrough < floor.cut ||
      receipt.authorizationProgress < floor.progress ||
      receipt.policyEpoch < floor.policy)
  ) {
    router.finish(id, undefined);
    return;
  }
  if (router.floors.size >= MAX_PENDING && !router.floors.has(key)) {
    for (const [k, f] of [...router.floors])
      if (![...f.protectedRequests].some((r) => router.routes.has(r))) router.floors.delete(k);
    if (router.floors.size >= MAX_PENDING) {
      router.finish(id, undefined);
      return;
    }
  }
  const state = await node.lock();
  try {
    if (
      receipt.rows.some((row: any) => {
        let current: any;
        try {
          current = state.currentRowCoordinate(row.table, row.row);
        } catch {
          return true;
        }
        return !current || !currentRowCoordinateEquals(current, row);
      }) ||
      state.activeCatalogueSeq() > receipt.policyEpoch ||
      state.committedGlobalTime() > receipt.settledThrough
    ) {
      router.finish(id, undefined);
      return;
    }
    await state.ingestCurrentRowsReceipt(receipt);
  } finally {
    node.unlock?.();
  }
  // At most one floor per outstanding route budget.
  const protectedRequests = new Set<string>();
  for (const [rid, r] of router.routes)
    if (
      rid !== id &&
      policyBindingKeyEquals(r.context, receipt.context) &&
      r.upstream?.sameAdmittedLink(expected)
    )
      protectedRequests.add(rid);
  router.floors.set(key, {
    cut: receipt.settledThrough,
    progress: receipt.authorizationProgress,
    policy: receipt.policyEpoch,
    protectedRequests,
  });
  router.finish(id, receipt);
}
