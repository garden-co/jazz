// Subscription registration, routing, known-state, and reset lifecycle.
//
// Owns the control-plane state that maps shapes and bindings to durable view
// identities. Query compilation, maintained terminal reduction, and row
// materialization remain separate stages.
import { SortedMap } from "../../../support/index.js";
import type {
  NodeState,
  ShapeId,
  ShapeAst,
  ValidatedQuery,
  Subscribe,
  SubscriptionKey,
  PolicyBindingKey,
  AuthorityResultKey,
  BindingViewKey,
  SchemaVersionId,
  RowUuid,
  GlobalTime,
  Value,
  Binding,
  AuthorSubject,
  KnownStateDeclaration,
  SupportingRow,
  VersionRow,
  RegisteredBinding,
  RegisterShapeOptions as RegisterShapeOptionsT,
} from "../../../_extern/node-qe-b.js";
import {
  Err,
  installNodeMethods,
  debugFmt,
  eprintln,
  coveredInputTrace,
  RegisterShapeOptions,
  ReadViewKeyDefault,
  ShapeAstVERSION,
  relationQueryToQuery,
  MAX_SHAPE_REGISTRATIONS_PER_PEER,
  MAX_RETAINED_PEER_SHAPES,
  PolicyBindingKeyFromDelegatedSession,
  PolicyBindingKeyFromCanonicalParts,
  AuthorityResultKeyUnscoped,
  AuthorityResultKeyPolicyScoped,
  BindingViewKeyFromCanonicalSubscriptionKey,
  AuthorSubjectStatic,
  DurabilityTier,
  RetainedRootWindowSource,
  cmpSupportingRow,
  eqKey,
} from "../../../_extern/node-qe-b.js";

type RegisteredBindingUsageKey = [
  binding: unknown,
  readView: unknown,
  policy: PolicyBindingKey | undefined,
];

function registerShapeWithOptions(
  this: NodeState,
  shapeId: ShapeId,
  ast: ShapeAst,
  opts: RegisterShapeOptionsT,
): void {
  const shape = this.validateShapeAstForRegistration(shapeId, ast);
  this.recordShapeOptions(shapeId, opts);
  this.retainValidatedShapeRegistration(shapeId, ast, shape);
  this.query.locallyRegisteredShapes.add(shapeId);
}

/** Retain a peer shape together with the exact read-view/compiler options carried by its registration. */
function registerShapeForPeerWithOptions(
  this: NodeState,
  peer: number,
  shapeId: ShapeId,
  ast: ShapeAst,
  opts: RegisterShapeOptionsT,
): void {
  const existing = this.parking.parkedShapeRegistrations.get(shapeId);
  if (existing !== undefined && !eqKey(existing, ast)) {
    throw Err.InvalidStoredValue("conflicting parked shape registration");
  }
  const shape = this.validateShapeAstForRegistration(shapeId, ast);
  const owners = this.query.peerShapeOwners;
  const alreadyOwned = owners.get(shapeId)?.has(peer) ?? false;
  if (!alreadyOwned) {
    let peerShapeCount = 0;
    for (const set of owners.values()) if (set.has(peer)) peerShapeCount++;
    if (peerShapeCount >= MAX_SHAPE_REGISTRATIONS_PER_PEER) {
      throw Err.UnsupportedSyncMessage("peer shape registration limit exceeded");
    }
    if (!owners.has(shapeId) && owners.size >= MAX_RETAINED_PEER_SHAPES) {
      throw Err.UnsupportedSyncMessage("global shape registration limit exceeded");
    }
  }
  this.recordShapeOptions(shapeId, opts);
  this.retainValidatedShapeRegistration(shapeId, ast, shape);
  let set = owners.get(shapeId);
  if (!set) owners.set(shapeId, (set = new Set()));
  set.add(peer);
}

function recordShapeOptions(this: NodeState, shapeId: ShapeId, opts: RegisterShapeOptionsT): void {
  // Legacy/default subscriptions carry `ReadViewKey::default()`; preserve the
  // received options at that wire identity as well.
  if (eqKey(opts, RegisterShapeOptions.default())) {
    this.query.registeredShapeOptions.set([shapeId, ReadViewKeyDefault()], opts.clone());
  }
  this.query.registeredShapeOptions.set([shapeId, opts.readViewKey()], opts);
}

function retainValidatedShapeRegistration(
  this: NodeState,
  shapeId: ShapeId,
  ast: ShapeAst,
  shape: ValidatedQuery | undefined,
): void {
  if (shape === undefined) {
    const had = this.parking.parkedShapeRegistrations.has(shapeId);
    this.parking.parkedShapeRegistrations.set(shapeId, ast);
    if (!had) this.syncMetrics.parkedCatalogueShapes += 1;
    return;
  }
  this.query.registeredShapes.set(shapeId, shape);
  this.drainParkedBindingDeltasForShape(shapeId);
}

function validateShapeAstForRegistration(
  this: NodeState,
  shapeId: ShapeId,
  ast: ShapeAst,
): ValidatedQuery | undefined {
  if (ast.version !== ShapeAstVERSION)
    throw Err.InvalidStoredValue("unsupported query AST version");
  let schema;
  if (eqKey(ast.schemaVersion, this.catalogue.localSchemaVersionId)) schema = this.catalogue.schema;
  else {
    const payload = this.catalogue.catalogueSchemas.get(ast.schemaVersion);
    if (!payload) return undefined;
    schema = payload.schema;
  }
  const query =
    ast.body.kind === "Query" ? ast.body.query : relationQueryToQuery(ast.body.relation);
  const shape = query.validateWithSchemaVersion(schema, ast.schemaVersion);
  if (!eqKey(shape.shapeId(), shapeId)) throw Err.InvalidStoredValue("shape id does not match AST");
  return shape;
}

function drainParkedShapeRegistrations(this: NodeState): void {
  const ready: [ShapeId, ShapeAst][] = [];
  for (const [shapeId, ast] of this.parking.parkedShapeRegistrations) {
    if (this.catalogue.catalogueSchemas.has(ast.schemaVersion)) ready.push([shapeId, ast.clone()]);
  }
  for (const [shapeId, ast] of ready) {
    const shape = this.validateShapeAstForRegistration(shapeId, ast);
    if (!shape) throw Err.InvalidStoredValue("catalogued shape registration remained unresolved");
    this.parking.parkedShapeRegistrations.delete(shapeId);
    this.syncMetrics.parkedCatalogueShapesResolved += 1;
    this.retainValidatedShapeRegistration(shapeId, ast, shape);
  }
}

function releaseShapeForPeer(this: NodeState, peer: number, shapeId: ShapeId): void {
  const owners = this.query.peerShapeOwners.get(shapeId);
  if (!owners || !owners.delete(peer) || owners.size > 0) return;
  this.query.peerShapeOwners.delete(shapeId);
  this.reclaimShapeIfUnowned(shapeId);
}

function releaseShapesForPeer(this: NodeState, peer: number): void {
  const newlyUnowned: ShapeId[] = [];
  for (const [shapeId, owners] of [...this.query.peerShapeOwners]) {
    owners.delete(peer);
    if (owners.size === 0) {
      newlyUnowned.push(shapeId);
      this.query.peerShapeOwners.delete(shapeId);
    }
  }
  for (const shapeId of newlyUnowned) this.reclaimShapeIfUnowned(shapeId);
}

/** Retain a shape while this node is actively serving it to one concrete downstream publication. */
function registerQuerySubscriptionForPeer(
  this: NodeState,
  publicationOwner: number,
  shapeId: ShapeId,
  ast: ShapeAst,
  opts: RegisterShapeOptionsT,
  subscribe: Subscribe,
  policyBinding: PolicyBindingKey,
): void {
  const shape = this.validateShapeAstForRegistration(shapeId, ast);
  if (!eqKey(opts.readViewKey(), subscribe.subscription.readView)) {
    throw Err.InvalidStoredValue(
      "served subscription read-view does not match its registration options",
    );
  }
  this.recordShapeOptions(shapeId, opts);
  this.retainValidatedShapeRegistration(shapeId, ast, shape);
  const subscription = subscribe.subscription;
  this.applySubscribeWithAdmittedPolicyBinding(subscribe, policyBinding.clone());
  getOrInsert(
    this.query.outboundShapeOwners,
    shapeId,
    () => new SortedMap<any, true>(cmpOwner),
  ).set([publicationOwner, subscription], true);
  getOrInsert(
    this.query.outboundBindingOwners,
    [subscription, policyBinding],
    () => new Set<number>(),
  ).add(publicationOwner);
}

const cmpOwner = (a: any, b: any) =>
  debugFmt(a) < debugFmt(b) ? -1 : debugFmt(a) > debugFmt(b) ? 1 : 0;

function getOrInsert<K, V>(
  map: { get(k: K): V | undefined; set(k: K, v: V): unknown },
  key: K,
  make: () => V,
): V {
  let v = map.get(key);
  if (v === undefined) map.set(key, (v = make()));
  return v;
}

/** Release one served publication's ownership. */
function releaseQuerySubscriptionForPeer(
  this: NodeState,
  publicationOwner: number,
  subscription: SubscriptionKey,
): void {
  const policyBinding =
    this.uniqueRegisteredBindingForSubscription(subscription)?.authorityResultKey.policyBinding;
  if (policyBinding === undefined) return;
  this.releaseQuerySubscriptionForPeerWithAdmittedPolicyBinding(
    publicationOwner,
    subscription,
    policyBinding,
  );
}

/** Release one served usage under the immutable policy scope that admitted it. */
function releaseQuerySubscriptionForPeerWithAdmittedPolicyBinding(
  this: NodeState,
  publicationOwner: number,
  subscription: SubscriptionKey,
  policyBinding: PolicyBindingKey,
): void {
  const shapeId = subscription.shapeId;
  const shapeOwners = this.query.outboundShapeOwners.get(shapeId);
  if (!shapeOwners || !shapeOwners.delete([publicationOwner, subscription])) return;
  if (shapeOwners.size === 0) this.query.outboundShapeOwners.delete(shapeId);
  const bindingKey = [subscription, policyBinding.clone()];
  const owners = this.query.outboundBindingOwners.get(bindingKey);
  let bindingBecameUnowned = false;
  if (owners) {
    owners.delete(publicationOwner);
    bindingBecameUnowned = owners.size === 0;
  }
  if (bindingBecameUnowned) {
    this.query.outboundBindingOwners.delete(bindingKey);
    this.applyUnsubscribeWithAdmittedPolicyBinding(subscription, policyBinding);
  }
  this.reclaimShapeIfUnowned(shapeId);
}

function retain<K, V>(map: any, keep: (k: K, v: V) => boolean): void {
  for (const [k, v] of [...map]) if (!keep(k, v)) map.delete(k);
}

function reclaimShapeIfUnowned(this: NodeState, shapeId: ShapeId): void {
  const q = this.query;
  if (
    q.locallyRegisteredShapes.has(shapeId) ||
    q.peerShapeOwners.has(shapeId) ||
    q.outboundShapeOwners.has(shapeId)
  )
    return;
  this.parking.parkedShapeRegistrations.delete(shapeId);
  this.parking.parkedBindingDeltas.delete(shapeId);
  q.registeredShapes.delete(shapeId);
  retain(q.registeredShapeOptions, ([registered]: any) => !eqKey(registered, shapeId));
  q.registeredBindings.delete(shapeId);
  retain(q.queryShapeCache, ([cached]: any) => !eqKey(cached, shapeId));
  q.compiledQueryProgramCache.clear();
  q.queryProgramTemplates.clear();
  q.supportedQueryProgramRequests.clear();
  retain(q.appliedViewUpdateGenerations, (key: any) => !eqKey(key.shapeId, shapeId));
  retain(q.authorityResults, (key: any) => !eqKey(key.bindingView.shapeId, shapeId));
  retain(q.outboundBindingOwners, ([subscription]: any) => !eqKey(subscription.shapeId, shapeId));
}

function applySubscribe(this: NodeState, subscribe: Subscribe): void {
  const policyBinding =
    subscribe.delegatedSession !== undefined
      ? PolicyBindingKeyFromDelegatedSession(subscribe.delegatedSession)
      : undefined;
  const shape = this.query.registeredShapes.get(subscribe.shapeId);
  if (!shape) {
    getOrInsert(this.parking.parkedBindingDeltas, subscribe.shapeId, () => [] as any[]).push({
      subscribe,
      policyBinding,
    });
    return;
  }
  this.applyKnownShapeSubscribe(shape, subscribe, policyBinding);
}

/** Register a server-admitted subscriber usage under the exact immutable policy snapshot. */
function applySubscribeWithAdmittedPolicyBinding(
  this: NodeState,
  subscribe: Subscribe,
  policyBinding: PolicyBindingKey,
): void {
  if (
    subscribe.delegatedSession !== undefined &&
    !eqKey(PolicyBindingKeyFromDelegatedSession(subscribe.delegatedSession), policyBinding)
  ) {
    throw Err.InvalidStoredValue("delegated subscription policy binding disagrees with admission");
  }
  const shape = this.query.registeredShapes.get(subscribe.shapeId);
  // Server admission always registers the shape before this call.
  if (!shape) throw Err.InvalidStoredValue("admitted subscription referenced unregistered shape");
  this.applyKnownShapeSubscribe(shape, subscribe, policyBinding);
}

function authorityStateFor(node: NodeState, subscription: SubscriptionKey): any {
  try {
    return node.query.authorityResults.get(node.authorityResultKeyForSubscription(subscription));
  } catch {
    return undefined;
  }
}

/** Whether this receiver's installed supporting set for `subscription` is exactly `revision`. */
function holdsSupportingRevision(
  this: NodeState,
  subscription: SubscriptionKey,
  revision: Uint8Array,
): boolean {
  const state = authorityStateFor(this, subscription);
  return state?.supportingRevision !== undefined && eqKey(state.supportingRevision, revision);
}

function forgetSupportingRevision(this: NodeState, subscription: SubscriptionKey): void {
  const state = authorityStateFor(this, subscription);
  if (state) state.supportingRevision = undefined;
}

/** Declare nothing for this view on its next open, so the serving peer resends every row. */
function forgetDeclaredKnownState(this: NodeState, subscription: SubscriptionKey): void {
  const state = authorityStateFor(this, subscription);
  if (state) {
    state.supportingRevision = undefined;
    state.settledThrough = undefined;
  }
}

/** Return the exact policy-scoped durable identity fixed at subscription admission. */
function authorityResultKeyForSubscription(
  this: NodeState,
  subscription: SubscriptionKey,
): AuthorityResultKey {
  const binding = this.uniqueRegisteredBindingForSubscription(subscription);
  if (binding) return binding.authorityResultKey.clone();
  let view: BindingViewKey | undefined;
  try {
    view = this.canonicalWholeTableBindingViewKey(subscription);
  } catch {
    view = undefined;
  }
  if (view !== undefined) return AuthorityResultKeyUnscoped(view);
  throw Err.InvalidStoredValue("subscription referenced unregistered binding");
}

/** Return an authority receipt only when the canonical binding view names exactly one policy scope. */
function uniqueAuthorityResultKeyForBindingView(
  this: NodeState,
  bindingView: BindingViewKey,
): AuthorityResultKey | undefined {
  const matches = [...this.query.authorityResults.keys()].filter((key: any) =>
    eqKey(key.bindingView, bindingView),
  );
  return matches.length === 1 ? matches[0] : undefined;
}

function drainParkedBindingDeltasForShape(this: NodeState, shapeId: ShapeId): void {
  const deltas = this.parking.parkedBindingDeltas.get(shapeId);
  if (deltas === undefined) return;
  const shape = this.query.registeredShapes.get(shapeId);
  if (!shape) return;
  this.parking.parkedBindingDeltas.delete(shapeId);
  for (const parked of deltas)
    this.applyKnownShapeSubscribe(shape, parked.subscribe, parked.policyBinding);
}

function applyKnownShapeSubscribe(
  this: NodeState,
  shape: ValidatedQuery,
  subscribe: Subscribe,
  policyBinding: PolicyBindingKey | undefined,
): void {
  const bindingViewKey = subscribeBindingViewKey(shape, subscribe);
  const authorityResultKey =
    policyBinding !== undefined
      ? AuthorityResultKeyPolicyScoped(bindingViewKey, policyBinding)
      : AuthorityResultKeyUnscoped(bindingViewKey);
  const readView = subscribe.subscription.readView;
  let options = this.query.registeredShapeOptions.get([subscribe.shapeId, readView])?.clone();
  if (options === undefined && eqKey(readView, ReadViewKeyDefault())) {
    for (const [[shapeId], opts] of this.query.registeredShapeOptions) {
      if (eqKey(shapeId, subscribe.shapeId) && eqKey(opts, RegisterShapeOptions.default())) {
        options = opts.clone();
        break;
      }
    }
  }
  if (options === undefined)
    throw Err.InvalidStoredValue("subscription referenced unregistered read-view options");
  if (coveredInputTrace()) {
    eprintln(
      `JAZZ_COVERED_INPUT_TRACE stage=subscribe_receipt binding=${debugFmt(bindingViewKey)} delegated=${debugFmt(subscribe.delegatedSession)} authority=${debugFmt(authorityResultKey)}`,
    );
  }
  // A new wire subscription needs a fresh authority receipt. It may retire
  // only its own policy-scoped retained local page.
  if (this.query.retainedRootWindowSources.delete(authorityResultKey)) {
    this.clearSettledResultView(authorityResultKey.clone());
  }
  getOrInsert(this.query.authorityResults, authorityResultKey.clone(), () =>
    this.newAuthorityResultState(),
  ).knownStateDeclared = subscribe.knownState !== undefined;
  const registeredKey = registeredBindingUsageKey(
    subscribe.subscription,
    authorityResultKey.policyBinding?.clone(),
  );
  const registered: RegisteredBinding = {
    values: subscribe.values,
    readView,
    bindingViewKey,
    authorityResultKey,
    options,
    compilerIdentity: subscribe.delegatedSession?.identity.clone() ?? AuthorSubjectStatic.SYSTEM,
  };
  getOrInsert(this.query.registeredBindings, subscribe.shapeId, () =>
    this.newRegisteredBindingMap(),
  ).set(registeredKey, registered);
}

function applyUnsubscribe(this: NodeState, subscription: SubscriptionKey): void {
  let parkedPolicyBinding: { value: PolicyBindingKey | undefined } | undefined;
  const parked = this.parking.parkedBindingDeltas.get(subscription.shapeId);
  if (parked) {
    for (const p of parked.filter((p: any) => eqKey(p.subscribe.subscription, subscription))) {
      if (parkedPolicyBinding !== undefined) {
        // A bare wire handle must not choose between policy scopes that share
        // the same usage-site key.
        if (!eqKey(parkedPolicyBinding.value, p.policyBinding)) return;
      } else parkedPolicyBinding = { value: p.policyBinding };
    }
  }
  if (parkedPolicyBinding !== undefined)
    this.removeParkedBindingUsage(subscription, parkedPolicyBinding.value);
  const registeredKey = this.uniqueRegisteredBindingUsageKey(subscription);
  // A bare wire handle is deliberately insufficient to retire an ambiguous
  // multiplexed policy scope.
  if (registeredKey === undefined) return;
  this.applyUnsubscribeRegisteredBinding(subscription, registeredKey);
}

/** Retire exactly the policy scope selected by authenticated connection admission. */
function applyUnsubscribeWithAdmittedPolicyBinding(
  this: NodeState,
  subscription: SubscriptionKey,
  policyBinding: PolicyBindingKey,
): void {
  this.removeParkedBindingUsage(subscription, policyBinding);
  this.applyUnsubscribeRegisteredBinding(
    subscription,
    registeredBindingUsageKey(subscription, policyBinding),
  );
}

function removeParkedBindingUsage(
  this: NodeState,
  subscription: SubscriptionKey,
  policyBinding: PolicyBindingKey | undefined,
): void {
  const parked = this.parking.parkedBindingDeltas.get(subscription.shapeId);
  if (!parked) return;
  const kept = parked.filter(
    (p: any) =>
      !eqKey(p.subscribe.subscription, subscription) || !eqKey(p.policyBinding, policyBinding),
  );
  if (kept.length === 0) this.parking.parkedBindingDeltas.delete(subscription.shapeId);
  else this.parking.parkedBindingDeltas.set(subscription.shapeId, kept);
}

function applyUnsubscribeRegisteredBinding(
  this: NodeState,
  subscription: SubscriptionKey,
  registeredKey: RegisteredBindingUsageKey,
): void {
  const bindings = this.query.registeredBindings.get(subscription.shapeId);
  const registered = bindings?.get(registeredKey);
  if (!registered) return;
  const authorityResultKey = registered.authorityResultKey;
  let retainedLocalWindow: any;
  const shape = this.query.registeredShapes.get(subscription.shapeId);
  if (shape && this.authoredCommitDurability === DurabilityTier.None) {
    const window = RetainedRootWindowSource.forShape(shape);
    const state = this.query.authorityResults.get(authorityResultKey);
    if (window.isBounded() && state?.sourceClosure.kind === "Claimed") retainedLocalWindow = window;
  }
  bindings.delete(registeredKey);
  if (!this.registeredBindingResolvesToAuthorityResultKey(authorityResultKey)) {
    // Once the last downstream usage site releases this exact binding view,
    // revoke its authority-selected membership.
    if (retainedLocalWindow)
      this.query.retainedRootWindowSources.set(authorityResultKey.clone(), retainedLocalWindow);
    else this.retireAuthorityResultView(authorityResultKey);
  }
}

function registeredBindingResolvesToBindingViewKey(
  this: NodeState,
  bindingViewKey: BindingViewKey,
): boolean {
  const bindings = this.query.registeredBindings.get(bindingViewKey.shapeId);
  if (!bindings) return false;
  for (const registered of bindings.values()) {
    if (
      eqKey(registered.readView, bindingViewKey.readView) &&
      eqKey(registered.bindingViewKey, bindingViewKey)
    )
      return true;
  }
  return false;
}

function registeredBindingResolvesToAuthorityResultKey(
  this: NodeState,
  key: AuthorityResultKey,
): boolean {
  const bindings = this.query.registeredBindings.get(key.bindingView.shapeId);
  if (!bindings) return false;
  for (const registered of bindings.values())
    if (eqKey(registered.authorityResultKey, key)) return true;
  return false;
}

/** Forget an in-memory authority result that has no live wire owner. */
function invalidateOwnerlessSettledResultView(
  this: NodeState,
  bindingViewKey: BindingViewKey,
): boolean {
  const keys = [...this.query.authorityResults.keys()].filter((key: any) =>
    eqKey(key.bindingView, bindingViewKey),
  );
  if (this.registeredBindingResolvesToBindingViewKey(bindingViewKey) || keys.length === 0)
    return false;
  for (const key of keys) this.retireAuthorityResultView(key);
  return true;
}

/** Retiring an authority connection keeps its input cache but removes the live settlement claim. */
function invalidateAuthorityResultSettlement(this: NodeState, key: AuthorityResultKey): void {
  const state = this.query.authorityResults.get(key);
  if (state) state.liveSettled = false;
}

function hasSettledAuthorityResult(this: NodeState, key: AuthorityResultKey): boolean {
  return (
    (this.query.authorityResults.get(key)?.liveSettled ?? false) &&
    !this.query.retainedRootWindowSources.has(key)
  );
}

/** Only exact deletion witnesses already admitted on this selected usage. */
async function selectedDeletionWitnesses(
  this: NodeState,
  key: AuthorityResultKey,
  schema: SchemaVersionId,
): Promise<SortedMap<SupportingRow, VersionRow>> {
  const state = this.query.authorityResults.get(key);
  const inputs = state
    ? [...state.coveredInputVersions.values()].filter(
        (input: any) => input.version.layer === "Deletion",
      )
    : [];
  const witnesses = new SortedMap<SupportingRow, VersionRow>(cmpSupportingRow);
  for (const input of inputs) {
    const version = await this.coveredInputVersion(input, schema);
    if (version === undefined) throw Err.MissingTransaction(input.version.tx);
    witnesses.set(input, version);
  }
  return witnesses;
}

/** Root identities suffice only for the unprojected scalar pilot. */
function scalarAuthorityInputRows(
  this: NodeState,
  key: AuthorityResultKey,
  table: string,
): Set<RowUuid> {
  const out = new Set<RowUuid>();
  const state = this.query.authorityResults.get(key);
  if (!state) return out;
  const shape = this.query.registeredShapes.get(key.bindingView.shapeId);
  let physical: any;
  if (shape) {
    try {
      physical = this.scopePhysicalTable(shape.schemaVersion(), table);
    } catch {
      physical = undefined;
    }
  }
  for (const input of state.coveredInputVersions.values()) {
    if (physical !== undefined && eqKey(physical, input.physicalTable)) out.add(input.row);
  }
  return out;
}

function appliedAuthorityResultGeneration(this: NodeState, key: AuthorityResultKey): number {
  return this.query.authorityResults.get(key)?.appliedViewUpdateGeneration ?? 0;
}

function allocateAuthoritativeResetGeneration(this: NodeState): number {
  const generation = this.nextAuthoritativeResetGeneration;
  if (generation === 0 || generation >= Number.MAX_SAFE_INTEGER) {
    throw Err.InvalidStoredValue("authoritative reset generation exhausted");
  }
  this.nextAuthoritativeResetGeneration = generation + 1;
  return generation;
}

/** Generation of the exact authority-selected source frontier, if claimed. */
function authoritySourceClosureGeneration(
  this: NodeState,
  key: AuthorityResultKey,
): number | undefined {
  const closure = this.query.authorityResults.get(key)?.sourceClosure;
  return closure?.kind === "Claimed" ? closure.generation : undefined;
}

/** Snapshot exact authority receipts whose next publication must be a reset. */
function snapshotPendingAuthoritativeResets(this: NodeState): any {
  const out = new SortedMap<AuthorityResultKey, number>(cmpOwner);
  for (const [key, state] of this.query.authorityResults) {
    if (state.pendingAuthoritativeReset !== undefined)
      out.set(key.clone(), state.pendingAuthoritativeReset);
  }
  return out;
}

/** Acknowledge one exact reset only if no newer reset superseded it. */
function acknowledgeAuthoritativeReset(
  this: NodeState,
  key: AuthorityResultKey,
  generation: number,
): boolean {
  const state = this.query.authorityResults.get(key);
  if (!state || state.pendingAuthoritativeReset !== generation) return false;
  state.pendingAuthoritativeReset = undefined;
  return true;
}

function publicationDeferredForAuthorityResult(this: NodeState, key: AuthorityResultKey): boolean {
  return this.query.authorityResults.get(key)?.deferredPublication ?? false;
}

function openingPendingForAuthorityResult(this: NodeState, key: AuthorityResultKey): boolean {
  return this.query.authorityResults.get(key)?.pendingOpening ?? false;
}

/** Return the settlement watermark for one exact authority result. */
function settledThroughForAuthorityResult(
  this: NodeState,
  key: AuthorityResultKey,
): GlobalTime | undefined {
  return this.query.authorityResults.get(key)?.settledThrough;
}

async function knownStateDeclarationForSubscription(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  subscription: SubscriptionKey,
  _values: Value[],
  identity: AuthorSubject,
  policyBinding: [AuthorSubject, Map<string, Value>] | undefined,
): Promise<KnownStateDeclaration | undefined> {
  const bindingViewKey: BindingViewKey = {
    shapeId: shape.shapeId(),
    bindingId: binding.bindingId(),
    readView: subscription.readView,
  };
  // Resolve the exact future receipt from the same immutable delegated policy
  // snapshot that will be sent with the Subscribe.
  const authorityResultKey =
    policyBinding !== undefined
      ? AuthorityResultKeyPolicyScoped(
          bindingViewKey,
          PolicyBindingKeyFromCanonicalParts(policyBinding[0], new Map(policyBinding[1])),
        )
      : AuthorityResultKeyUnscoped(bindingViewKey);
  // A retained local window is not proof of the complete upstream input.
  if (this.query.retainedRootWindowSources.has(authorityResultKey)) return undefined;
  if (this.query.authorityResults.get(authorityResultKey)?.settledThrough === undefined) {
    await this.restoreSubscriptionWatermark(shape, binding, authorityResultKey, identity);
  }
  const state = this.query.authorityResults.get(authorityResultKey);
  const position = state?.settledThrough;
  if (position !== undefined) {
    const authorizationProgress = state.authorizationProgress;
    // "Q at W": with an installed, claimed supporting set the serving peer can
    // answer with only the rows that moved past W.
    if (
      state.pendingAuthoritativeReset === undefined &&
      state.sourceClosure.kind === "Claimed" &&
      state.supportingRevision !== undefined
    ) {
      return {
        kind: "Watermark",
        position,
        authorizationProgress,
        supportingRevision: state.supportingRevision,
      };
    }
    return authorizationProgress !== undefined
      ? {
          kind: "FastWithAuthorizationProgress",
          completeness: "FastCurrentMembership",
          position,
          authorizationProgress,
        }
      : { kind: "Fast", completeness: "FastCurrentMembership", position };
  }
  // Without a watermark there is no known state to declare.
  return undefined;
}

function bindingViewKeyForSubscription(
  this: NodeState,
  subscription: SubscriptionKey,
): BindingViewKey {
  const registered = this.uniqueRegisteredBindingForSubscription(subscription);
  if (registered) return registered.bindingViewKey;
  const key = this.canonicalWholeTableBindingViewKey(subscription);
  if (key !== undefined) return key;
  throw Err.InvalidStoredValue("subscription referenced unregistered binding");
}

/** Resolve a wire handle only when one admitted scope owns it. */
function uniqueRegisteredBindingForSubscription(
  this: NodeState,
  subscription: SubscriptionKey,
): RegisteredBinding | undefined {
  return this.uniqueRegisteredBindingEntry(subscription)?.[1];
}

function uniqueRegisteredBindingUsageKey(
  this: NodeState,
  subscription: SubscriptionKey,
): RegisteredBindingUsageKey | undefined {
  return this.uniqueRegisteredBindingEntry(subscription)?.[0];
}

function uniqueRegisteredBindingEntry(
  this: NodeState,
  subscription: SubscriptionKey,
): [RegisteredBindingUsageKey, RegisteredBinding] | undefined {
  const bindings = this.query.registeredBindings.get(subscription.shapeId);
  if (!bindings) return undefined;
  const matches = [...bindings].filter(
    ([key]: any) => eqKey(key[0], subscription.bindingId) && eqKey(key[1], subscription.readView),
  );
  return matches.length === 1 ? matches[0] : undefined;
}

function canonicalWholeTableBindingViewKey(
  this: NodeState,
  subscription: SubscriptionKey,
): BindingViewKey | undefined {
  for (const table of this.catalogue.schema.tables) {
    if (eqKey(this.wholeTableSubscriptionKey(table.name), subscription)) {
      return BindingViewKeyFromCanonicalSubscriptionKey(subscription);
    }
  }
  return undefined;
}

/** The binding view a subscribe names: its shape bound to its parameter values, read through its read view. */
function subscribeBindingViewKey(shape: ValidatedQuery, subscribe: Subscribe): BindingViewKey {
  const names = [...shape.params().keys()];
  if (subscribe.values.length !== names.length)
    throw Err.InvalidStoredValue("binding arity mismatch");
  const binding = shape.bind(new Map(names.map((n, i) => [n, subscribe.values[i]])));
  return {
    shapeId: subscribe.shapeId,
    bindingId: binding.bindingId(),
    readView: subscribe.subscription.readView,
  };
}

function registeredBindingUsageKey(
  subscription: SubscriptionKey,
  policyBinding: PolicyBindingKey | undefined,
): RegisteredBindingUsageKey {
  return [subscription.bindingId, subscription.readView, policyBinding];
}

installNodeMethods({
  registerShapeWithOptions,
  registerShapeForPeerWithOptions,
  recordShapeOptions,
  retainValidatedShapeRegistration,
  validateShapeAstForRegistration,
  drainParkedShapeRegistrations,
  releaseShapeForPeer,
  releaseShapesForPeer,
  registerQuerySubscriptionForPeer,
  releaseQuerySubscriptionForPeer,
  releaseQuerySubscriptionForPeerWithAdmittedPolicyBinding,
  reclaimShapeIfUnowned,
  applySubscribe,
  applySubscribeWithAdmittedPolicyBinding,
  holdsSupportingRevision,
  forgetSupportingRevision,
  forgetDeclaredKnownState,
  authorityResultKeyForSubscription,
  uniqueAuthorityResultKeyForBindingView,
  drainParkedBindingDeltasForShape,
  applyKnownShapeSubscribe,
  applyUnsubscribe,
  applyUnsubscribeWithAdmittedPolicyBinding,
  removeParkedBindingUsage,
  applyUnsubscribeRegisteredBinding,
  registeredBindingResolvesToBindingViewKey,
  registeredBindingResolvesToAuthorityResultKey,
  invalidateOwnerlessSettledResultView,
  invalidateAuthorityResultSettlement,
  hasSettledAuthorityResult,
  selectedDeletionWitnesses,
  scalarAuthorityInputRows,
  appliedAuthorityResultGeneration,
  allocateAuthoritativeResetGeneration,
  authoritySourceClosureGeneration,
  snapshotPendingAuthoritativeResets,
  acknowledgeAuthoritativeReset,
  publicationDeferredForAuthorityResult,
  openingPendingForAuthorityResult,
  settledThroughForAuthorityResult,
  knownStateDeclarationForSubscription,
  bindingViewKeyForSubscription,
  uniqueRegisteredBindingForSubscription,
  uniqueRegisteredBindingUsageKey,
  uniqueRegisteredBindingEntry,
  canonicalWholeTableBindingViewKey,
});
