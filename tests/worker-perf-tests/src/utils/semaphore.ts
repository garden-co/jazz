/**
 * A simple semaphore for limiting concurrent operations.
 */
export class Semaphore {
  private permits: number;
  private waiters: (() => void)[] = [];

  constructor(permits: number) {
    this.permits = Math.max(1, Math.floor(permits));
  }

  async acquire(): Promise<void> {
    if (this.permits > 0) {
      this.permits--;
      return;
    }
    await new Promise<void>((resolve) => this.waiters.push(resolve));
    this.permits--;
  }

  release(): void {
    this.permits++;
    const w = this.waiters.shift();
    if (w) w();
  }
}
