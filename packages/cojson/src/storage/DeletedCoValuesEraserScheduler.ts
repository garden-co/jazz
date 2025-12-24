export type DeletedCoValuesEraserSchedulerRunResult = {
  hasMore: boolean;
};

export type DeletedCoValuesEraserSchedulerOpts = {
  throttleMs: number;
  startupDelayMs: number;
  followUpDelayMs: number;
};

type SchedulerState =
  | "idle"
  | "startup_scheduled"
  | "throttle_scheduled"
  | "followup_scheduled"
  | "running"
  | "disposed";

export class DeletedCoValuesEraserScheduler {
  private readonly runOnce: () => Promise<DeletedCoValuesEraserSchedulerRunResult>;
  private readonly opts: DeletedCoValuesEraserSchedulerOpts;

  private state: SchedulerState = "idle";
  private disposed = false;

  private scheduledTimeout: ReturnType<typeof setTimeout> | undefined;

  constructor({
    runOnce,
    opts,
  }: {
    runOnce: () => Promise<DeletedCoValuesEraserSchedulerRunResult>;
    opts: DeletedCoValuesEraserSchedulerOpts;
  }) {
    this.runOnce = runOnce;
    this.opts = opts;
  }

  scheduleStartupDrain() {
    if (this.disposed) return;

    // Only schedule startup drain if nothing is already scheduled/running.
    if (this.state !== "idle") return;

    this.scheduleTimer("startup_scheduled", this.opts.startupDelayMs);
  }

  onEnqueueDeletedCoValue() {
    if (this.disposed) return;

    // While we're already draining (or have a follow-up scheduled), ignore enqueue
    // to avoid overlapping phases. The active drain loop will pick up new work.
    if (this.state !== "idle") return;

    // Only idle reaches here.
    this.scheduleTimer("throttle_scheduled", this.opts.throttleMs);
  }

  dispose() {
    if (this.disposed) return;
    this.disposed = true;
    this.state = "disposed";

    if (this.scheduledTimeout) clearTimeout(this.scheduledTimeout);
    this.scheduledTimeout = undefined;
  }

  private scheduleTimer(
    state: Exclude<SchedulerState, "idle" | "running" | "disposed">,
    delayMs: number,
  ) {
    if (this.disposed) return;
    if (this.scheduledTimeout) return;

    this.state = state;
    this.scheduledTimeout = setTimeout(() => {
      this.scheduledTimeout = undefined;
      void this.run();
    }, delayMs);
  }

  private async run() {
    if (this.disposed) return;

    // Clear any pre-run scheduled state and enter running state.
    this.state = "running";

    const result = await this.runOnce();

    if (this.disposed) return;

    if (result.hasMore) {
      // One follow-up phase at a time. Further enqueues while follow-up is scheduled
      // are ignored.
      this.scheduleTimer("followup_scheduled", this.opts.followUpDelayMs);
      return;
    }

    this.state = "idle";
  }
}
