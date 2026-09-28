/** One task per notified continuation. CPU self-wakes yield to MessagePort,
 * storage, timers and rendering; an externally suspended read schedules nothing.
 * Each pending read owns its ports so completion/cancellation releases them. */
export class NativeReadWake {
  version = 0;
  private resume: (() => void) | undefined;
  private channel: MessageChannel | undefined;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private queued = false;
  private disposed = false;

  wake = (): void => {
    if (this.disposed) return;
    this.version += 1;
    this.schedule();
  };

  wait(observed: number): Promise<void> {
    return new Promise((resolve) => {
      this.resume = resolve;
      // Includes wakes fired synchronously inside poll(), before wait existed.
      if (this.version !== observed) this.schedule();
    });
  }

  private schedule(): void {
    if (!this.resume || this.queued || this.disposed) return;
    this.queued = true;
    if (typeof MessageChannel === "undefined") {
      this.timer = setTimeout(this.deliver, 0);
    } else {
      if (!this.channel) {
        this.channel = new MessageChannel();
        this.channel.port1.onmessage = this.deliver;
      }
      this.channel.port2.postMessage(null);
    }
  }

  private deliver = (): void => {
    this.queued = false;
    this.timer = undefined;
    const resume = this.resume;
    this.resume = undefined;
    resume?.();
  };

  dispose(): void {
    this.disposed = true;
    this.resume = undefined;
    if (this.timer !== undefined) clearTimeout(this.timer);
    if (this.channel) {
      this.channel.port1.onmessage = null;
      this.channel.port1.close();
      this.channel.port2.close();
    }
  }
}
