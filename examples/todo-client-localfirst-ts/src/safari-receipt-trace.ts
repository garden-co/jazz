type TraceEvent = {
  phase: string;
  elapsedMs: number;
  direction?: "in" | "out";
  messageType?: string;
  frameCount?: number;
  frameBytes?: number;
  outcome?: "fulfilled" | "rejected";
};

type TraceSink = { record(event: Omit<TraceEvent, "elapsedMs">): void };

declare global {
  interface Window {
    __jazzSafariReceiptTrace__?: TraceSink;
  }
}

export function traceSafariReceipt(event: Omit<TraceEvent, "elapsedMs">): void {
  window.__jazzSafariReceiptTrace__?.record(event);
}
