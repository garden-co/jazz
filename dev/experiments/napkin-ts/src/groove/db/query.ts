import {
  type GraphBuilder,
  type InputSourceDelta,
  type InputSourceId,
  type InputSourceReplacement,
  type MultisinkSubscription,
  type PreparedShapeId,
  type Query,
  type RecordDeltas,
  type RoutedMultisinkTerminal,
  type Subscription,
  type SubscriptionId,
  type TickMetrics,
  type Value,
  type Waker,
  type Poll,
  PENDING,
  RecordDescriptor,
  TemplateGraphInput,
  planQuery,
  planPreparedShape,
} from "../../_extern/groove-db-storage.js";
import { StagedWriteOverlay } from "../storage/index.js";
import { type Database, DbError } from "./index.js";
import { MeteredStorage, PreparedShape } from "./storage_helpers.js";

const noopWaker: Waker = () => {};

/** `std::future::poll_fn`: re-poll whenever the supplied waker fires. */
export function pollFn<T>(poll: (cx: Waker) => Poll<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    const step = () => {
      try {
        const r = poll(step);
        if (r !== PENDING) resolve(r as T);
      } catch (error) {
        reject(error);
      }
    };
    step();
  });
}

const fromIvm = async <T>(p: Promise<T> | T): Promise<T> => {
  try {
    return await p;
  } catch (error) {
    throw DbError.from(error);
  }
};
const ivmSync = <T>(f: () => T): T => {
  try {
    return f();
  } catch (error) {
    throw DbError.from(error);
  }
};

type SubscriptionEvent<T> = { kind: "Update"; update: T } | { kind: "Error"; error: any };

function subscriptionEventResult<T>(event: SubscriptionEvent<T>): T {
  if (event.kind === "Update") return event.update;
  if (event.error?.kind === "Ended") throw new DbError("SubscriptionEnded");
  throw new DbError("SubscriptionFailed", event.error);
}

/** Checked before an input-source replacement or delta touches a refcount. */
const isInputSourcePreflightError = (error: any) =>
  [
    "RecordEncoding",
    "ForeignInputSource",
    "InputSourceRetired",
    "BindingSourceDescriptorMismatch",
  ].includes(error?.kind);

export class QueryMethods {
  /** Owned, metered read view with the resident staged writes layered over durable storage. */
  ownedReadStorage(this: Database): MeteredStorage {
    return new MeteredStorage(
      new StagedWriteOverlay(this.storage, this.residentWrites),
      this.storageReadMetrics,
    );
  }

  describeTemplateInput(this: Database, graph: GraphBuilder) {
    return new TemplateGraphInput(graph, this.graphOutputDescriptor(graph));
  }

  /** Infer the exact output contract without installing or executing a graph. */
  graphOutputDescriptor(this: Database, graph: GraphBuilder): RecordDescriptor {
    this.ensureNotPoisoned();
    return ivmSync(() => this.ivmRuntime.inferBuilderOutput(graph));
  }

  allocateInputSource(this: Database, descriptor: RecordDescriptor): InputSourceId {
    return this.ivmRuntime.allocateInputSource(descriptor);
  }

  replaceInputSources(this: Database, replacements: InputSourceReplacement[], detachCold = false) {
    this.ensureNotPoisoned();
    const storage = this.ownedReadStorage();
    const tick = detachCold
      ? this.ivmRuntime.replaceInputSourcesDetachingCold(replacements, storage)
      : this.ivmRuntime.replaceInputSources(replacements, storage);
    return this.finishInputSourceTick(tick, isInputSourcePreflightError);
  }

  replaceInputSourcesDetachingCold(this: Database, replacements: InputSourceReplacement[]) {
    return this.replaceInputSources(replacements, true);
  }

  applyInputSourceDeltas(this: Database, deltas: InputSourceDelta[], detachCold = false) {
    this.ensureNotPoisoned();
    const storage = this.ownedReadStorage();
    const tick = detachCold
      ? this.ivmRuntime.applyInputSourceDeltasDetachingCold(deltas, storage)
      : this.ivmRuntime.applyInputSourceDeltas(deltas, storage);
    return this.finishInputSourceTick(tick, isInputSourcePreflightError);
  }

  applyInputSourceDeltasDetachingCold(this: Database, deltas: InputSourceDelta[]) {
    return this.applyInputSourceDeltas(deltas, true);
  }

  /** Retract and permanently retire runtime-local input identities. */
  retireInputSources(this: Database, ids: InputSourceId[]) {
    this.ensureNotPoisoned();
    const tick = this.ivmRuntime.retireInputSources(ids, this.ownedReadStorage());
    return this.finishInputSourceTick(
      tick,
      (e: any) => e?.kind === "ForeignInputSource" || e?.kind === "InputSourceRetired",
    );
  }

  /** Preflight errors are recoverable; any other failure poisons (fail closed). */
  async finishInputSourceTick(
    this: Database,
    tick: Promise<TickMetrics>,
    isPreflight: (error: unknown) => boolean,
  ): Promise<TickMetrics> {
    let metrics: TickMetrics;
    try {
      metrics = await tick;
    } catch (error) {
      if (!isPreflight(error)) this.poisoned = true;
      throw new DbError("IvmRuntime", error);
    }
    this.lastTickMetrics = metrics;
    try {
      this.driveResidentProgressNow();
    } catch (error) {
      this.poisoned = true;
      throw error;
    }
    return metrics;
  }

  /** Drain IVM-scheduled continuations for resident work; stop at genuine storage waits. */
  driveResidentProgressNow(this: Database): void {
    while (this.ivmRuntime.hasResidentContinuation()) {
      const r = this.pollResidentProgress(noopWaker);
      if (r !== PENDING) return;
      if (!this.ivmRuntime.hasResidentContinuation()) return;
    }
  }

  drainSelfScheduledResidentProgress(this: Database) {
    this.driveResidentProgressNow();
  }

  hasPendingProgress(this: Database): boolean {
    return this.ivmRuntime.hasPendingIncremental();
  }

  subscriptionHasPendingProgress(this: Database, subscription: SubscriptionId): boolean {
    return this.ivmRuntime.subscriptionHasPendingProgress(subscription);
  }

  subscriptionHasPendingEvaluation(this: Database, subscription: SubscriptionId): boolean {
    return this.ivmRuntime.subscriptionHasPendingEvaluation(subscription);
  }

  /** Drive suspended incremental evaluation until quiescent or waiting for storage. */
  pollProgress(this: Database, cx: Waker): Poll<void> {
    this.ensureNotPoisoned();
    return this.finishProgress(() => this.ivmRuntime.pollPendingIncremental(cx));
  }

  /** Poll only explicitly runnable CPU continuations. */
  pollResidentProgress(this: Database, cx: Waker): Poll<void> {
    this.ensureNotPoisoned();
    return this.finishProgress(() => this.ivmRuntime.pollResidentIncremental(cx));
  }

  finishProgress(this: Database, progress: () => Poll<void>): Poll<void> {
    try {
      return progress();
    } catch (error) {
      this.poisoned = true;
      throw new DbError("IvmRuntime", error);
    } finally {
      this.refreshResidentWrites();
    }
  }

  driveProgress(this: Database): Promise<void> {
    return pollFn((cx) => this.pollProgress(cx));
  }

  driveReadyProgress(this: Database) {
    return this.driveReadyProgressWithWaker(undefined);
  }

  /** Drive runnable work without waiting for cold storage. */
  async driveReadyProgressWithWaker(
    this: Database,
    progressWaker: Waker | undefined,
  ): Promise<void> {
    if (progressWaker) this.ivmRuntime.retainOwnerProgressWaker(progressWaker);
    this.pollProgress(progressWaker ?? noopWaker);
  }

  /** Poll a subscription's next event, driving suspended evaluation once in between. */
  pollSubscriptionEvent<T>(
    this: Database,
    cx: Waker,
    next: (cx: Waker) => Poll<SubscriptionEvent<T>>,
  ): Poll<T> {
    const event = next(cx);
    if (event !== PENDING) return subscriptionEventResult(event as SubscriptionEvent<T>);
    this.pollProgress(cx);
    const again = next(cx);
    return again === PENDING ? PENDING : subscriptionEventResult(again as SubscriptionEvent<T>);
  }

  pollSubscription(this: Database, subscription: Subscription, cx: Waker): Poll<RecordDeltas> {
    const r = this.pollSubscriptionWithPublication(subscription, cx);
    return r === PENDING ? PENDING : (r as any).deltas;
  }
  nextSubscription(this: Database, subscription: Subscription): Promise<RecordDeltas> {
    return pollFn((cx) => this.pollSubscription(subscription, cx));
  }
  pollSubscriptionWithPublication(this: Database, subscription: Subscription, cx: Waker) {
    return this.pollSubscriptionEvent(cx, (c) => subscription.pollNextEvent(c));
  }
  nextSubscriptionWithPublication(this: Database, subscription: Subscription) {
    return pollFn((cx) => this.pollSubscriptionWithPublication(subscription, cx));
  }
  pollMultisinkSubscription(this: Database, subscription: MultisinkSubscription, cx: Waker) {
    const r = this.pollMultisinkSubscriptionWithPublication(subscription, cx);
    return r === PENDING ? PENDING : (r as any).deltas;
  }
  nextMultisinkSubscription(this: Database, subscription: MultisinkSubscription) {
    return pollFn((cx) => this.pollMultisinkSubscription(subscription, cx));
  }
  pollMultisinkSubscriptionWithPublication(
    this: Database,
    subscription: MultisinkSubscription,
    cx: Waker,
  ) {
    return this.pollSubscriptionEvent(cx, (c) => subscription.pollNextEvent(c));
  }
  nextMultisinkSubscriptionWithPublication(this: Database, subscription: MultisinkSubscription) {
    return pollFn((cx) => this.pollMultisinkSubscriptionWithPublication(subscription, cx));
  }

  /** Subscribe to an IVM graph: initial snapshot followed by committed deltas. */
  async subscribeOneSink(this: Database, graph: GraphBuilder): Promise<Subscription> {
    this.ensureNotPoisoned();
    const subscription = await fromIvm(
      this.ivmRuntime.subscribeOneSinkWithWaker(graph, this.ownedReadStorage(), undefined),
    );
    // Drain only self-scheduled cooperative slices; never await a cold read here.
    this.driveResidentProgressNow();
    return subscription;
  }

  /** Subscribe to several named IVM graph outputs as one logical stream. */
  subscribe(this: Database, sinks: [string, GraphBuilder][]): MultisinkSubscription {
    const subscription = this.subscribeWithWaker(sinks, undefined);
    this.driveResidentProgressNow();
    return subscription;
  }

  subscribeWithWaker(
    this: Database,
    sinks: [string, GraphBuilder][],
    progressWaker: Waker | undefined,
  ) {
    return this.subscribeWithLifetime(sinks, "Retained", progressWaker);
  }

  subscribeWithLifetime(
    this: Database,
    sinks: [string, GraphBuilder][],
    lifetime: string,
    progressWaker?: Waker,
  ) {
    return this.subscribeWithLifetimeAndRootValues(sinks, lifetime, "Materialize", progressWaker);
  }

  subscribeWithLifetimeAndRootValues(
    this: Database,
    sinks: [string, GraphBuilder][],
    lifetime: string,
    rootIndirectValues: string,
    progressWaker?: Waker,
  ): MultisinkSubscription {
    this.ensureNotPoisoned();
    return ivmSync(() =>
      this.ivmRuntime.subscribeWithLifetime(
        sinks,
        this.ownedReadStorage(),
        lifetime,
        rootIndirectValues,
        progressWaker,
      ),
    );
  }

  /** Subscribe to a SQL-ish query by letting the planner lower it into an IVM graph. */
  async subscribeQuery(this: Database, query: Query): Promise<Subscription> {
    const planned = ivmSync(() => planQuery(query, this.ivmRuntime.schema()));
    return this.subscribeOneSink(planned.graph);
  }

  /** Prepare a parameterized SQL-ish query shape once for many bindings. */
  async prepareQuery(this: Database, query: Query): Promise<PreparedShape> {
    this.ensureNotPoisoned();
    const planned = ivmSync(() => planPreparedShape(query, this.ivmRuntime.schema()));
    const output = new RecordDescriptor(
      planned.publicOutput.map((f: any) => [f.name, f.valueType]),
    );
    const shape = await this.prepareOneSink(
      planned.planned.graph,
      planned.shape,
      planned.bindingDescriptor,
      planned.outputKeyFields,
    );
    return new PreparedShape(shape.id(), planned.parameters, output);
  }

  /** Bind a prepared query shape by named parameter. */
  async bind(
    this: Database,
    prepared: PreparedShape,
    bindings: [string, Value][],
  ): Promise<Subscription> {
    this.ensureNotPoisoned();
    const values: Value[] = [];
    for (const parameter of prepared.parameters) {
      const matching = bindings.filter(([name]) => name === parameter.name);
      if (!matching.length) throw new DbError("MissingParameter", parameter.name);
      if (matching.length > 1) throw new DbError("DuplicateParameter", parameter.name);
      values.push(matching[0][1]);
    }
    for (const [name] of bindings)
      if (!prepared.parameters.some((p: any) => p.name === name))
        throw new DbError("UnknownParameter", name);
    const storage = this.ownedReadStorage();
    const live = await fromIvm(this.ivmRuntime.prepareLiveAttach(prepared.id, values, storage));
    const subscription = ivmSync(() =>
      this.ivmRuntime.bindShapeOneSinkWithOutputAndWaker(
        prepared.id,
        values,
        prepared.output,
        storage,
        undefined,
        live,
      ),
    );
    this.driveResidentProgressNow();
    return subscription;
  }

  /** Thin sugar over `prepare` with a single route-carrying terminal. */
  prepareOneSink(
    this: Database,
    graph: GraphBuilder,
    bindingSourceShape: string,
    bindingDescriptor: RecordDescriptor,
    outputKeyFields: string[],
  ) {
    this.ensureNotPoisoned();
    return fromIvm(
      this.ivmRuntime.prepareOneSink(
        graph,
        bindingSourceShape,
        bindingDescriptor,
        outputKeyFields,
        this.ownedReadStorage(),
      ),
    );
  }

  prepareOneSinkWithRouting(
    this: Database,
    outputGraph: GraphBuilder,
    routingGraph: GraphBuilder,
    bindingSourceShape: string,
    bindingDescriptor: RecordDescriptor,
    routingKeyFields: string[],
  ) {
    this.ensureNotPoisoned();
    return fromIvm(
      this.ivmRuntime.prepareOneSinkWithRouting(
        outputGraph,
        routingGraph,
        bindingSourceShape,
        bindingDescriptor,
        routingKeyFields,
        this.ownedReadStorage(),
      ),
    );
  }

  /** Prepare the canonical parameterized multisink shape. */
  prepare(
    this: Database,
    terminals: RoutedMultisinkTerminal[],
    bindingSourceShape: string,
    bindingDescriptor: RecordDescriptor,
  ) {
    this.ensureNotPoisoned();
    return fromIvm(
      this.ivmRuntime.prepare(
        terminals,
        bindingSourceShape,
        bindingDescriptor,
        this.ownedReadStorage(),
      ),
    );
  }

  /** Prepare a routed shape shared by callers with identical terminals. */
  prepareShared(
    this: Database,
    terminals: RoutedMultisinkTerminal[],
    bindingSourceShape: string,
    bindingDescriptor: RecordDescriptor,
  ) {
    this.ensureNotPoisoned();
    return fromIvm(
      this.ivmRuntime.prepareShared(
        terminals,
        bindingSourceShape,
        bindingDescriptor,
        this.ownedReadStorage(),
      ),
    );
  }

  releaseSharedPreparedShape(this: Database, shape: PreparedShapeId) {
    this.ivmRuntime.releaseSharedPreparedShape(shape);
  }

  async bindShapeOneSink(
    this: Database,
    shape: PreparedShapeId,
    bindingValues: Value[],
  ): Promise<Subscription> {
    this.ensureNotPoisoned();
    const storage = this.ownedReadStorage();
    const live = await fromIvm(this.ivmRuntime.prepareLiveAttach(shape, bindingValues, storage));
    const subscription = ivmSync(() =>
      this.ivmRuntime.bindShapeOneSinkWithWaker(shape, bindingValues, storage, undefined, live),
    );
    this.driveResidentProgressNow();
    return subscription;
  }

  async bindShapeOneSinkWithOutput(
    this: Database,
    shape: PreparedShapeId,
    bindingValues: Value[],
    publicOutput: RecordDescriptor,
  ): Promise<Subscription> {
    this.ensureNotPoisoned();
    const storage = this.ownedReadStorage();
    const live = await fromIvm(this.ivmRuntime.prepareLiveAttach(shape, bindingValues, storage));
    const subscription = ivmSync(() =>
      this.ivmRuntime.bindShapeOneSinkWithOutputAndWaker(
        shape,
        bindingValues,
        publicOutput,
        storage,
        undefined,
        live,
      ),
    );
    this.driveResidentProgressNow();
    return subscription;
  }

  bindShape(this: Database, shape: PreparedShapeId, bindingValues: Value[]) {
    return this.bindShapeWithRootValues(shape, bindingValues, "Materialize");
  }

  async bindShapeWithRootValues(
    this: Database,
    shape: PreparedShapeId,
    bindingValues: Value[],
    rootIndirectValues: string,
  ): Promise<MultisinkSubscription> {
    // Physical root values are only valid for a first result.
    const lifetime = rootIndirectValues === "Materialize" ? "Retained" : "FirstResult";
    const subscription = await this.bindShapeWithLifetimeAndRootValues(
      shape,
      bindingValues,
      lifetime,
      rootIndirectValues,
      undefined,
    );
    this.driveResidentProgressNow();
    return subscription;
  }

  bindShapeWithLifetime(
    this: Database,
    shape: PreparedShapeId,
    bindingValues: Value[],
    lifetime: string,
    progressWaker?: Waker,
  ) {
    return this.bindShapeWithLifetimeAndRootValues(
      shape,
      bindingValues,
      lifetime,
      "Materialize",
      progressWaker,
    );
  }

  async bindShapeWithLifetimeAndRootValues(
    this: Database,
    shape: PreparedShapeId,
    bindingValues: Value[],
    lifetime: string,
    rootIndirectValues: string,
    progressWaker: Waker | undefined,
  ): Promise<MultisinkSubscription> {
    this.ensureNotPoisoned();
    const storage = this.ownedReadStorage();
    const live =
      lifetime === "Retained"
        ? await fromIvm(this.ivmRuntime.prepareLiveAttach(shape, bindingValues, storage))
        : undefined;
    return ivmSync(() =>
      this.ivmRuntime.bindShapeWithLifetime(
        shape,
        bindingValues,
        storage,
        lifetime,
        rootIndirectValues,
        progressWaker,
        live,
      ),
    );
  }

  /** Bind a retained subscription and transfer shape ownership to its bindings. */
  async bindShapeOwnedWithRootValues(
    this: Database,
    shape: PreparedShapeId,
    bindingValues: Value[],
    rootIndirectValues: string,
    progressWaker?: Waker,
  ) {
    const subscription = await this.bindShapeWithLifetimeAndRootValues(
      shape,
      bindingValues,
      "Retained",
      rootIndirectValues,
      progressWaker,
    );
    this.ivmRuntime.transferPreparedShapeToBindings(shape);
    return subscription;
  }

  /** Run a one-shot SQL-ish query against the current storage snapshot. */
  async query(this: Database, query: Query): Promise<RecordDeltas> {
    const planned = ivmSync(() => planQuery(query, this.ivmRuntime.schema()));
    return this.queryGraph(planned.graph);
  }

  queryGraph(this: Database, graph: GraphBuilder) {
    return this.queryGraphWithRootValues(graph, "Materialize");
  }

  queryGraphWithRootValues(
    this: Database,
    graph: GraphBuilder,
    rootIndirectValues: string,
  ): Promise<RecordDeltas> {
    this.ensureNotPoisoned();
    return fromIvm(
      this.ivmRuntime.querySnapshotWithRootValues(
        graph,
        this.ownedReadStorage(),
        rootIndirectValues,
      ),
    );
  }

  queryGraphs(this: Database, sinks: [string, GraphBuilder][]) {
    this.ensureNotPoisoned();
    return fromIvm(this.ivmRuntime.querySnapshots(sinks, this.ownedReadStorage()));
  }

  unsubscribe(this: Database, subscriptionId: SubscriptionId): boolean {
    return this.ivmRuntime.unsubscribe(subscriptionId);
  }

  retirePreparedShape(this: Database, shape: PreparedShapeId) {
    this.ensureNotPoisoned();
    ivmSync(() => this.ivmRuntime.retirePreparedShape(shape));
  }

  /** Retire subscriptions whose receiving handles have already been dropped. */
  pruneDroppedSubscriptions(this: Database): Promise<number> {
    this.ensureNotPoisoned();
    return fromIvm(this.ivmRuntime.pruneDroppedSubscriptionsWithStorage(this.storage));
  }
}
