import { LinkedList } from "./LinkedList.js";

export class Semaphore {
  private permits: number;
  private waitingQueue = new LinkedList<() => void>();

  constructor(permits: number) {
    this.permits = permits;
  }

  acquire(callback: () => void): void {
    if (this.permits > 0) {
      this.permits--;
      callback();
    } else {
      this.waitingQueue.push(callback);
    }
  }

  release(): void {
    const next = this.waitingQueue.shift();

    if (next) {
      next();
    } else {
      this.permits++;
    }
  }
}
