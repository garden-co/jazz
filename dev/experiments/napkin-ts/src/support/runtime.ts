// Small runtime helpers: errors, interning, tracing, async plumbing.
export class JazzError extends Error {
  constructor(
    readonly kind: string,
    message: string,
    readonly cause?: unknown,
  ) {
    super(message);
  }
}
export function assert(cond: unknown, message = "assertion failed"): asserts cond {
  if (!cond) throw new JazzError("Invariant", message);
}
export function unreachable(what: never | string = "unreachable"): never {
  throw new JazzError("Invariant", `internal error: entered unreachable code: ${String(what)}`);
}

/** Rust `internment::Intern<T>`: one canonical instance per structural key. */
export class Interner<T> {
  private readonly table = new Map<string, T>();
  constructor(private readonly key: (t: T) => string) {}
  intern(t: T): T {
    const k = this.key(t);
    const hit = this.table.get(k);
    if (hit !== undefined) return hit;
    this.table.set(k, t);
    return t;
  }
}

/** Tracing spans and counters: always present, ~0 cost when nothing listens. */
export type TraceSink = {
  span(name: string, fields?: Record<string, unknown>): () => void;
  count(name: string, n: number): void;
};
let sink: TraceSink | undefined;
export const setTraceSink = (s: TraceSink | undefined) => (sink = s);
export function span<T>(name: string, f: () => T, fields?: Record<string, unknown>): T {
  if (!sink) return f();
  const end = sink.span(name, fields);
  try {
    return f();
  } finally {
    end();
  }
}
export function counter(name: string, n = 1): void {
  sink?.count(name, n);
}

/** futures::channel::mpsc stand-in. */
export class Channel<T> {
  private queue: T[] = [];
  private waiters: ((v: T | undefined) => void)[] = [];
  private closed = false;
  send(v: T): boolean {
    if (this.closed) return false;
    const w = this.waiters.shift();
    if (w) w(v);
    else this.queue.push(v);
    return true;
  }
  recv(): Promise<T | undefined> {
    if (this.queue.length) return Promise.resolve(this.queue.shift());
    if (this.closed) return Promise.resolve(undefined);
    return new Promise((r) => this.waiters.push(r));
  }
  tryRecv(): T | undefined {
    return this.queue.shift();
  }
  close(): void {
    this.closed = true;
    for (const w of this.waiters.splice(0)) w(undefined);
  }
}
/** oneshot / waker stand-in. */
export function deferred<T>(): {
  promise: Promise<T>;
  resolve: (v: T) => void;
  reject: (e: unknown) => void;
} {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((a, b) => {
    resolve = a;
    reject = b;
  });
  return { promise, resolve, reject };
}
export const yieldNow = (): Promise<void> => new Promise((r) => setTimeout(r, 0));
export const nowMs = (): number => Date.now();
export const monotonicMs = (): number => performance.now();

/** Placeholder for a symbol owned by another port area (see CONVENTIONS.md). */
export function extern<T>(): T {
  return undefined as T;
}
