import { logger } from "../logger.js";

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

export const DEFAULT_DELETE_SCHEDULE_OPTS = {
  throttleMs: 60_000,
  startupDelayMs: 1_000,
  followUpDelayMs: 1_000,
} satisfies DeletedCoValuesEraserSchedulerOpts;

export class DeletedCoValuesEraserScheduler {
  private readonly runCallback: () => Promise<DeletedCoValuesEraserSchedulerRunResult>;
  private readonly opts: DeletedCoValuesEraserSchedulerOpts;

  private state: SchedulerState = "idle";

  private isDisposed(): boolean {
    return this.state === "disposed";
  }

  private scheduledTimeout: ReturnType<typeof setTimeout> | undefined;

  constructor({
    run,
    opts,
  }: {
    run: () => Promise<DeletedCoValuesEraserSchedulerRunResult>;
    opts?: DeletedCoValuesEraserSchedulerOpts;
  }) {
    this.runCallback = run;
    this.opts = opts || DEFAULT_DELETE_SCHEDULE_OPTS;
  }

  scheduleStartupDrain() {
    if (this.isDisposed()) return;

    // Only schedule startup drain if nothing is already scheduled/running.
    if (this.state !== "idle") return;

    this.scheduleTimer("startup_scheduled", this.opts.startupDelayMs);
  }

  onEnqueueDeletedCoValue() {
    if (this.isDisposed()) return;

    // While we're already draining (or have a follow-up scheduled), ignore enqueue
    // to avoid overlapping phases. The active drain loop will pick up new work.
    if (this.state !== "idle") return;

    // Only idle reaches here.
    this.scheduleTimer("throttle_scheduled", this.opts.throttleMs);
  }

  dispose() {
    if (this.isDisposed()) return;
    this.state = "disposed";

    if (this.scheduledTimeout) clearTimeout(this.scheduledTimeout);
    this.scheduledTimeout = undefined;
  }

  private scheduleTimer(
    state: Exclude<SchedulerState, "idle" | "running" | "disposed">,
    delayMs: number,
  ) {
    if (this.isDisposed()) return;
    if (this.scheduledTimeout) return;

    this.state = state;
    this.scheduledTimeout = setTimeout(() => {
      this.scheduledTimeout = undefined;
      void this.run();
    }, delayMs);
  }

  private async run() {
    if (this.isDisposed()) return;

    // Clear any pre-run scheduled state and enter running state.
    this.state = "running";

    let result: DeletedCoValuesEraserSchedulerRunResult;
    try {
      result = await this.runCallback();
    } catch (error) {
      logger.error("Error running deleted co values eraser scheduler", {
        err: error,
      });
      // If the run callback fails, recover to idle so future enqueues/startup drains
      // can retry instead of getting stuck in "running".
      if (!this.isDisposed()) {
        this.state = "idle";
      }
      return;
    }

    if (this.isDisposed()) return;

    if (result.hasMore) {
      // One follow-up phase at a time. Further enqueues while follow-up is scheduled
      // are ignored.
      this.scheduleTimer("followup_scheduled", this.opts.followUpDelayMs);
      return;
    }

    this.state = "idle";
  }
}
