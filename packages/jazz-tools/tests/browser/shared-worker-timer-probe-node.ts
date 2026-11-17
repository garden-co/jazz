// Chromium only: counts the timers a page's SharedWorker arms, through CDP.
// Playwright does not expose shared workers, so this attaches to the
// `shared_worker` target directly and evaluates in it.

type CdpSession = {
  send(method: string, params?: Record<string, unknown>): Promise<any>;
  on(event: string, handler: (payload: any) => void): void;
  off(event: string, handler: (payload: any) => void): void;
  detach(): Promise<void>;
};

type ProbePage = {
  context(): { browser(): { newBrowserCDPSession(): Promise<CdpSession> } | null };
};

export type SharedWorkerTimerCounts = {
  /** Every `setTimeout` the worker armed since the probe was installed. */
  timeouts: number;
  /** Those armed with the runtime's 16ms server-pump debounce. */
  serverPumpTimeouts: number;
  /** Microtasks queued with `queueMicrotask` (the pumps' scheduling). */
  microtasks: number;
  /** Messages the worker posted to its tabs. */
  posts: number;
};

// Installed once per worker; later calls only read the counters. The runtime
// calls the global `setTimeout` at each use, so wrapping it after load counts
// every timer it arms from then on.
const PROBE = `(() => {
  const g = globalThis;
  if (!g.__jazzTimerProbe) {
    const counts = { timeouts: 0, serverPumpTimeouts: 0, microtasks: 0, posts: 0 };
    const original = g.setTimeout;
    g.setTimeout = function (handler, delay, ...rest) {
      counts.timeouts += 1;
      if (delay === 16) counts.serverPumpTimeouts += 1;
      return original.call(this, handler, delay, ...rest);
    };
    const queue = g.queueMicrotask;
    g.queueMicrotask = function (callback) {
      counts.microtasks += 1;
      return queue.call(this, callback);
    };
    const post = g.MessagePort.prototype.postMessage;
    g.MessagePort.prototype.postMessage = function (...args) {
      counts.posts += 1;
      return post.apply(this, args);
    };
    g.__jazzTimerProbe = counts;
  }
  return JSON.stringify(g.__jazzTimerProbe);
})()`;

/**
 * Install the probe in (or read it from) the one SharedWorker whose name or
 * URL contains `key`, such as the database name the worker is named for.
 */
export async function sharedWorkerTimerCounts(
  page: unknown,
  key: string,
): Promise<SharedWorkerTimerCounts> {
  const browser = (page as ProbePage).context().browser();
  if (!browser) throw new Error("shared worker probe needs a browser-backed context");
  const session = await browser.newBrowserCDPSession();
  try {
    const { targetInfos } = (await session.send("Target.getTargets")) as {
      targetInfos: Array<{ targetId: string; type: string; title: string; url: string }>;
    };
    const workers = targetInfos.filter(
      (target) =>
        target.type === "shared_worker" && (target.title.includes(key) || target.url.includes(key)),
    );
    if (workers.length !== 1) {
      const seen = targetInfos
        .filter((target) => target.type === "shared_worker")
        .map((target) => `${target.title} ${target.url}`);
      throw new Error(
        `expected one shared worker matching ${key}, found ${workers.length}: ${JSON.stringify(seen)}`,
      );
    }
    const { sessionId } = (await session.send("Target.attachToTarget", {
      targetId: workers[0]!.targetId,
      flatten: false,
    })) as { sessionId: string };
    try {
      return JSON.parse(
        await evaluateInTarget(session, sessionId, PROBE),
      ) as SharedWorkerTimerCounts;
    } finally {
      await session.send("Target.detachFromTarget", { sessionId });
    }
  } finally {
    await session.detach();
  }
}

async function evaluateInTarget(
  session: CdpSession,
  sessionId: string,
  expression: string,
): Promise<string> {
  const id = 1;
  return await new Promise<string>((resolve, reject) => {
    const onMessage = (payload: { sessionId: string; message: string }) => {
      if (payload.sessionId !== sessionId) return;
      const message = JSON.parse(payload.message) as {
        id?: number;
        result?: { result?: { value?: unknown }; exceptionDetails?: unknown };
        error?: unknown;
      };
      if (message.id !== id) return;
      session.off("Target.receivedMessageFromTarget", onMessage);
      if (message.error || message.result?.exceptionDetails) {
        reject(new Error(JSON.stringify(message.error ?? message.result?.exceptionDetails)));
        return;
      }
      resolve(String(message.result?.result?.value));
    };
    session.on("Target.receivedMessageFromTarget", onMessage);
    session
      .send("Target.sendMessageToTarget", {
        sessionId,
        message: JSON.stringify({
          id,
          method: "Runtime.evaluate",
          params: { expression, returnByValue: true },
        }),
      })
      .catch((error: unknown) => {
        session.off("Target.receivedMessageFromTarget", onMessage);
        reject(error);
      });
  });
}
