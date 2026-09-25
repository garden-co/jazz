/**
 * Typing-composer device receipt (#3273 / #2964). A maintained subscription
 * watches one composer row while the app "types" into it, first one awaited
 * keystroke at a time, then as a same-turn burst. Each keystroke is an
 * ordinary public `db.update`. The receipt records how long each synchronous
 * write call held the JS thread, how long its local echo took to reach the
 * subscription callback, and whether the observed text ever dropped,
 * reordered or re-showed characters.
 */

export interface TypingComposerMetrics {
  keystrokes: number;
  burst: number;
  blockedP50Ms: number;
  blockedP95Ms: number;
  blockedMaxMs: number;
  echoP50Ms: number;
  echoP95Ms: number;
  echoMaxMs: number;
  burstEchoMs: number;
  dropped: number;
  reordered: number;
  reappeared: number;
}

export interface ComposerEchoAnalysis {
  /** Final text is shorter than the typed text (characters never arrived). */
  dropped: number;
  /** An observed text is not a prefix of the typed text. */
  reordered: number;
  /** An observed text got shorter and later longer again (a flicker). */
  reappeared: number;
}

/**
 * Typing only appends, so every observed composer text must be a prefix of
 * the typed text, observed lengths must never decrease, and the last one must
 * be the whole typed text.
 */
export function analyzeComposerEcho(
  typed: string,
  observed: readonly string[],
): ComposerEchoAnalysis {
  let reordered = 0;
  let reappeared = 0;
  let longest = 0;
  for (const text of observed) {
    if (!typed.startsWith(text)) reordered += 1;
    if (text.length < longest) reappeared += 1;
    longest = Math.max(longest, text.length);
  }
  const last = observed.at(-1) ?? "";
  return {
    dropped: typed.startsWith(last) ? typed.length - last.length : typed.length,
    reordered,
    reappeared,
  };
}

export function percentile(samples: readonly number[], fraction: number): number {
  if (samples.length === 0) return 0;
  const sorted = [...samples].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.ceil(fraction * sorted.length) - 1);
  return sorted[Math.max(0, index)];
}

export interface ComposerDriver {
  /** Start the composer row; returns its id. */
  open(): Promise<string>;
  /** One keystroke: synchronously issue the write for the whole new text. */
  type(id: string, text: string): void;
  /** Latest text the maintained subscription delivered for the row. */
  observedTexts(): readonly string[];
  now(): number;
  yieldTurn(): Promise<void>;
  /** Record which phase is running, so a device failure names it. */
  stage?(phase: "keystroke" | "burst" | "order"): void;
}

export interface ComposerEcho {
  /** Subscription callback: the latest full snapshot of the watched table. */
  onSnapshot(rows: readonly { id: string; title: string }[]): void;
  /** Start following the composer row once its insert has returned its id. */
  follow(id: string): void;
  readonly observed: string[];
}

/**
 * Record the composer row's text as the subscription reports it. A public
 * insert pumps ready subscription batches before it returns, so the snapshot
 * that first contains the row may precede `follow`; keep the latest snapshot
 * and re-check it once the id is known.
 */
export function composerEcho(): ComposerEcho {
  let composerId: string | undefined;
  let latest: readonly { id: string; title: string }[] = [];
  const observed: string[] = [];
  const record = () => {
    const composer = latest.find((row) => row.id === composerId);
    if (composer && observed.at(-1) !== composer.title) observed.push(composer.title);
  };
  return {
    onSnapshot(rows) {
      latest = rows;
      record();
    },
    follow(id) {
      composerId = id;
      record();
    },
    observed,
  };
}

const ECHO_DEADLINE_MS = 10_000;

async function waitForEcho(driver: ComposerDriver, text: string): Promise<number | null> {
  const started = driver.now();
  while (driver.now() - started < ECHO_DEADLINE_MS) {
    if (driver.observedTexts().at(-1) === text) return driver.now();
    await driver.yieldTurn();
  }
  return null;
}

/** Run the composer script against a driver and return its measurements. */
export async function measureTypingComposer(
  driver: ComposerDriver,
  single = "hello jazz ",
  burst = "fast typing burst!",
): Promise<TypingComposerMetrics> {
  const id = await driver.open();
  const blocked: number[] = [];
  const echo: number[] = [];
  let text = "";
  driver.stage?.("keystroke");
  for (const character of single) {
    text += character;
    const started = driver.now();
    driver.type(id, text);
    blocked.push(driver.now() - started);
    const echoedAt = await waitForEcho(driver, text);
    if (echoedAt === null) throw new Error("typing composer keystroke never echoed locally");
    echo.push(echoedAt - started);
  }
  driver.stage?.("burst");
  const burstStarted = driver.now();
  for (const character of burst) {
    text += character;
    const started = driver.now();
    driver.type(id, text);
    blocked.push(driver.now() - started);
  }
  const burstEchoedAt = await waitForEcho(driver, text);
  if (burstEchoedAt === null) throw new Error("typing composer burst never echoed locally");
  driver.stage?.("order");
  const analysis = analyzeComposerEcho(text, driver.observedTexts());
  const metrics: TypingComposerMetrics = {
    keystrokes: single.length + burst.length,
    burst: burst.length,
    blockedP50Ms: percentile(blocked, 0.5),
    blockedP95Ms: percentile(blocked, 0.95),
    blockedMaxMs: Math.max(...blocked),
    echoP50Ms: percentile(echo, 0.5),
    echoP95Ms: percentile(echo, 0.95),
    echoMaxMs: Math.max(...echo),
    burstEchoMs: burstEchoedAt - burstStarted,
    ...analysis,
  };
  if (analysis.dropped || analysis.reordered || analysis.reappeared) {
    throw new Error(
      `typing composer echo was not monotonic: dropped=${analysis.dropped} reordered=${analysis.reordered} reappeared=${analysis.reappeared}`,
    );
  }
  return metrics;
}

/** Round receipt metrics to 0.01 ms so the bounded receipt stays compact. */
export function receiptMetrics(metrics: TypingComposerMetrics): Record<string, number> {
  return Object.fromEntries(
    Object.entries(metrics).map(([key, value]) => [key, Math.round(value * 100) / 100]),
  );
}
