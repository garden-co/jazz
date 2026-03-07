export class ClockSync {
  private timeOrigin = performance.timeOrigin;
  socket: WebSocket;
  smoothedInterval: number | null = $state(null);
  alpha: number = 0.2;
  lastHeartbeat: number | null = $state(null);
  offset: number = $state(0);

  constructor(url: string) {
    this.socket = new WebSocket(url);

    this.lastHeartbeat = null;
    this.smoothedInterval = null;

    this.socket.addEventListener("message", (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        const nowMonotonic = performance.now();
        const nowEpochHighRes = this.timeOrigin + nowMonotonic;
        if (this.lastHeartbeat) {
          const dt = nowEpochHighRes - this.lastHeartbeat;
          this.smoothedInterval =
            this.smoothedInterval === null
              ? dt
              : this.alpha * dt + (1 - this.alpha) * this.smoothedInterval;
        }
        this.offset = msg.time - nowEpochHighRes;
        this.lastHeartbeat = nowEpochHighRes;
      } catch {
        // ignore parse errors
      }
    });
  }

  serverToLocal(serverTime: number): number {
    return serverTime - this.offset;
  }

  localToServer(localTime: number): number {
    return localTime + this.offset;
  }

  destroy(): void {
    this.socket.close();
  }
}
