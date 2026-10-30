import type { Transport } from "./native-runtime-adapter.js";

/** One cancellable receive deadline per physical transport, outside semantic work. */
export class AuxiliaryReceiveDeadline {
  private timer: ReturnType<typeof setTimeout> | undefined;
  private deadline: number | undefined;
  private closed = false;

  constructor(
    private readonly transport: Transport,
    private readonly onError: (error: unknown) => void,
  ) {}

  refresh(): void {
    if (this.closed) return;
    try {
      const delay = this.transport.auxiliaryReceiveTimeoutMs?.();
      if (delay == null) {
        this.clear();
        return;
      }
      if (!Number.isFinite(delay) || delay < 0 || !this.transport.expireAuxiliaryReceive) {
        throw new Error("Invalid auxiliary receive deadline capability");
      }
      const deadline = performance.now() + delay;
      // Progress may extend the idle deadline. Keep an earlier wake and let
      // the core recompute its idle/absolute minimum when that wake fires.
      if (this.deadline !== undefined && this.deadline <= deadline) return;
      this.clear();
      this.deadline = deadline;
      this.timer = setTimeout(() => {
        this.timer = undefined;
        this.deadline = undefined;
        if (this.closed) return;
        try {
          this.transport.expireAuxiliaryReceive!();
          this.refresh();
        } catch (error) {
          this.fail(error);
        }
      }, delay);
    } catch (error) {
      this.fail(error);
    }
  }

  close(): void {
    this.closed = true;
    this.clear();
  }

  private clear(): void {
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
    this.deadline = undefined;
  }

  private fail(error: unknown): void {
    this.close();
    this.onError(error);
  }
}
