// Console + buffered trace-entry sink (Rust: tracing_subscriber Layer over wasm-bindgen console).
import { type Level, LEVEL_RANK, WasmLayerConfig } from "./config.js";
import { StringRecorder } from "./recorder.js";

type UnixNano = [number, number];

export type TraceEntry =
  | {
      kind: "span";
      sequence: number;
      name: string;
      target: string;
      level: string;
      startUnixNano: UnixNano;
      endUnixNano: UnixNano;
      fields: Record<string, string>;
    }
  | {
      kind: "log";
      sequence: number;
      target: string;
      level: string;
      timestampUnixNano: UnixNano;
      message: string;
      fields: Record<string, string>;
    }
  | { kind: "dropped"; count: number };

const MAX_TRACE_ENTRIES = 5_000;

let collectionEnabled = false;
let drainNotificationPending = false;
let sequence = 0;
let dropped = 0;
let entries: TraceEntry[] = [];
let drainCallback: (() => void) | undefined;

/** Enable or disable collection of buffered tracing entries for JavaScript drains. */
export function setTraceEntryCollectionEnabled(enabled: boolean): void {
  collectionEnabled = enabled;
  if (!enabled) clearTraceEntries();
}

/** Subscribe to notifications that buffered tracing entries are ready to drain. */
export function subscribeTraceEntries(callback: () => void): () => void {
  drainCallback = callback;
  drainNotificationPending = false;
  if (entries.length || dropped > 0) notifyTraceEntriesReady();
  return () => {
    drainCallback = undefined;
    drainNotificationPending = false;
  };
}

/** Drain buffered tracing entries. */
export function drainTraceEntries(): TraceEntry[] {
  const drained = entries;
  entries = [];
  const count = dropped;
  dropped = 0;
  drainNotificationPending = false;
  if (count > 0) drained.push({ kind: "dropped", count });
  return drained;
}

function pushTraceEntry(entry: TraceEntry): void {
  if (!collectionEnabled) return;
  if (entries.length >= MAX_TRACE_ENTRIES) dropped += 1;
  else entries.push(entry);
  notifyTraceEntriesReady();
}

function notifyTraceEntriesReady(): void {
  if (drainNotificationPending || !drainCallback) return;
  drainNotificationPending = true;
  try {
    drainCallback();
  } catch {
    drainNotificationPending = false;
  }
}

function clearTraceEntries(): void {
  entries = [];
  dropped = 0;
  sequence = 0;
  drainNotificationPending = false;
}

function unixNanoNow(): UnixNano {
  const ms = Date.now();
  let seconds = Math.floor(ms / 1000);
  let nanos = Math.round((ms - seconds * 1000) * 1_000_000);
  if (nanos >= 1_000_000_000) {
    nanos -= 1_000_000_000;
    seconds += 1;
  }
  return [seconds, nanos];
}

const LEVEL_COLOR: Record<Level, string> = {
  TRACE: "color: dodgerblue; background: #444",
  DEBUG: "color: lawngreen; background: #444",
  INFO: "color: whitesmoke; background: #444",
  WARN: "color: orange; background: #444",
  ERROR: "color: red; background: #444",
};

function logWithColor(message: string, level: Level): void {
  console.log(message, LEVEL_COLOR[level], "color: gray; font-style: italic", "color: inherit");
}

interface SpanMeta {
  name: string;
  target: string;
  level: Level;
}
interface SpanState {
  meta: SpanMeta;
  recorder: StringRecorder;
  start?: UnixNano;
}

/** Console-logging and trace-entry-collecting layer. */
export class WasmLayer {
  private stack: SpanState[] = [];
  constructor(private readonly config: WasmLayerConfig = new WasmLayerConfig()) {}

  enabled(level: Level): boolean {
    return LEVEL_RANK[level] <= LEVEL_RANK[this.config.maxLevel];
  }

  newSpan(meta: SpanMeta, fields: Record<string, unknown> = {}): SpanState {
    const recorder = new StringRecorder();
    if (this.config.enabled || collectionEnabled)
      for (const [k, v] of Object.entries(fields)) recorder.record(k, v);
    return { meta, recorder };
  }

  record(span: SpanState, fields: Record<string, unknown>): void {
    for (const [k, v] of Object.entries(fields)) span.recorder.record(k, v);
  }

  event(meta: SpanMeta & { file?: string; line?: number }, fields: Record<string, unknown>): void {
    if (!this.config.enabled && !collectionEnabled) return;
    const recorder = new StringRecorder();
    for (const [k, v] of Object.entries(fields)) recorder.record(k, v);
    const level = meta.level;
    if (collectionEnabled)
      pushTraceEntry({
        kind: "log",
        sequence: sequence++,
        target: meta.target,
        level,
        timestampUnixNano: unixNanoNow(),
        message: recorder.message ?? recorder.display.trim(),
        fields: Object.fromEntries(recorder.fields),
      });
    if (!this.config.enabled) return;
    if (collectionEnabled && LEVEL_RANK[level] > LEVEL_RANK.WARN) return;
    const origin = meta.file && meta.line !== undefined ? `${meta.file}:${meta.line}` : "";
    const current = this.stack[this.stack.length - 1];
    const spanFields = current
      ? [...current.recorder.fields].map(([k, v]) => `\n\t${k}: ${v}`).join("")
      : "";
    logWithColor(`%c${level}%c ${origin}%c${recorder}${spanFields}`, level);
  }

  enter(span: SpanState): void {
    span.start ??= unixNanoNow();
    this.stack.push(span);
    if (!this.config.consoleGroupSpans) return;
    const level = span.meta.level;
    if (collectionEnabled && LEVEL_RANK[level] > LEVEL_RANK.WARN) return;
    logWithColor(`%c${level}%c ▶ "${span.meta.name}"${span.recorder}`, level);
  }

  exit(span: SpanState): void {
    const i = this.stack.lastIndexOf(span);
    if (i >= 0) this.stack.splice(i, 1);
  }

  close(span: SpanState): void {
    if (!collectionEnabled || !span.start) return;
    pushTraceEntry({
      kind: "span",
      sequence: sequence++,
      name: span.meta.name,
      target: span.meta.target,
      level: span.meta.level,
      startUnixNano: span.start,
      endUnixNano: unixNanoNow(),
      fields: Object.fromEntries(span.recorder.fields),
    });
  }
}
