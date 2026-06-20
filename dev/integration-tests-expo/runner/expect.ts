// A tiny, Hermes-safe, Jest-style assertion shim. Only the matchers our
// integration tests use are implemented. No V8-only APIs (no
// Error.captureStackTrace), so it runs identically on Node and Hermes.

export interface Matchers {
  toBe(expected: unknown): void;
  toEqual(expected: unknown): void;
  toMatchObject(expected: Record<string, unknown>): void;
  toHaveLength(expected: number): void;
  toContain(expected: unknown): void;
  toBeGreaterThan(expected: number): void;
  toBeGreaterThanOrEqual(expected: number): void;
  toBeDefined(): void;
  toBeUndefined(): void;
  toBeNull(): void;
  toBeTruthy(): void;
  toBeFalsy(): void;
  readonly not: Matchers;
}

export type Expect = (actual: unknown) => Matchers;

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v) && !(v instanceof Uint8Array);
}

export function deepEqual(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true;
  if (a instanceof Uint8Array || b instanceof Uint8Array) {
    if (!(a instanceof Uint8Array) || !(b instanceof Uint8Array) || a.length !== b.length)
      return false;
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
    return true;
  }
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
    return a.every((x, i) => deepEqual(x, b[i]));
  }
  if (isPlainObject(a) && isPlainObject(b)) {
    const ak = Object.keys(a);
    const bk = Object.keys(b);
    if (ak.length !== bk.length) return false;
    return ak.every((k) => Object.prototype.hasOwnProperty.call(b, k) && deepEqual(a[k], b[k]));
  }
  return false;
}

function subsetMatch(actual: unknown, expected: Record<string, unknown>): boolean {
  if (!isPlainObject(actual)) return false;
  return Object.keys(expected).every((k) => {
    const ev = expected[k];
    const av = actual[k];
    if (isPlainObject(ev)) return subsetMatch(av, ev);
    return deepEqual(av, ev);
  });
}

function fmt(v: unknown): string {
  try {
    return JSON.stringify(v) ?? String(v);
  } catch {
    return String(v);
  }
}

function makeMatchers(actual: unknown, negate: boolean): Matchers {
  const assert = (pass: boolean, describe: string) => {
    if (pass === negate) {
      throw new Error(`expected ${fmt(actual)} ${negate ? "not " : ""}${describe}`);
    }
  };
  return {
    toBe(expected) {
      assert(Object.is(actual, expected), `to be ${fmt(expected)}`);
    },
    toEqual(expected) {
      assert(deepEqual(actual, expected), `to equal ${fmt(expected)}`);
    },
    toMatchObject(expected) {
      assert(subsetMatch(actual, expected), `to match object ${fmt(expected)}`);
    },
    toHaveLength(expected) {
      const len = (actual as { length?: number } | null | undefined)?.length;
      assert(len === expected, `to have length ${expected} (got ${fmt(len)})`);
    },
    toContain(expected) {
      let pass = false;
      if (Array.isArray(actual)) pass = actual.some((x) => deepEqual(x, expected));
      else if (typeof actual === "string") pass = actual.includes(String(expected));
      assert(pass, `to contain ${fmt(expected)}`);
    },
    toBeGreaterThan(expected) {
      assert(typeof actual === "number" && actual > expected, `to be greater than ${expected}`);
    },
    toBeGreaterThanOrEqual(expected) {
      assert(
        typeof actual === "number" && actual >= expected,
        `to be greater than or equal to ${expected}`,
      );
    },
    toBeDefined() {
      assert(actual !== undefined, `to be defined`);
    },
    toBeUndefined() {
      assert(actual === undefined, `to be undefined`);
    },
    toBeNull() {
      assert(actual === null, `to be null`);
    },
    toBeTruthy() {
      assert(Boolean(actual), `to be truthy`);
    },
    toBeFalsy() {
      assert(!actual, `to be falsy`);
    },
    get not() {
      return makeMatchers(actual, !negate);
    },
  };
}

export const expect: Expect = (actual) => makeMatchers(actual, false);
