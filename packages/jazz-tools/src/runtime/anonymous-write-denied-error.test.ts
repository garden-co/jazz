import { describe, it, expect } from "vitest";
import {
  AnonymousWriteDeniedError,
  isAnonymousWriteDenied,
  normalizeRuntimeWriteError,
} from "./anonymous-write-denied-error.js";

describe("AnonymousWriteDeniedError", () => {
  it("construction carries table + operation + stable name", () => {
    const err = new AnonymousWriteDeniedError({ table: "todos", operation: "insert" });
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe("JazzAnonymousWriteDeniedError");
    expect(err.table).toBe("todos");
    expect(err.operation).toBe("insert");
    expect(err.message).toMatch(/anonymous session cannot insert on table todos/);
  });

  it("isAnonymousWriteDenied detects WASM-side structured errors", () => {
    const raw = Object.assign(new Error("anonymous session cannot insert on table todos"), {
      name: "JazzAnonymousWriteDeniedError",
      table: "todos",
      operation: "insert",
    });
    expect(isAnonymousWriteDenied(raw)).toBe(true);
  });

  it("isAnonymousWriteDenied detects NAPI-side prefix-matched errors", () => {
    const raw = new Error("anonymous session cannot update on table todos");
    expect(isAnonymousWriteDenied(raw)).toBe(true);
  });

  it("isAnonymousWriteDenied detects NAPI-side uppercase operation errors", () => {
    const raw = new Error("anonymous session cannot UPDATE on table todos");
    expect(isAnonymousWriteDenied(raw)).toBe(true);
  });

  it("returns false for unrelated errors", () => {
    expect(isAnonymousWriteDenied(new Error("some other failure"))).toBe(false);
    expect(isAnonymousWriteDenied(null)).toBe(false);
  });

  it("normalizeRuntimeWriteError lifts a matching raw error into a typed class", () => {
    const raw = Object.assign(new Error("anonymous session cannot delete on table todos"), {
      name: "JazzAnonymousWriteDeniedError",
      table: "todos",
      operation: "delete",
    });
    const normalized = normalizeRuntimeWriteError(raw);
    expect(normalized).toBeInstanceOf(AnonymousWriteDeniedError);
    expect((normalized as AnonymousWriteDeniedError).table).toBe("todos");
    expect((normalized as AnonymousWriteDeniedError).operation).toBe("delete");
    expect((normalized as Error & { cause?: unknown }).cause).toBe(raw);
  });

  it("normalizeRuntimeWriteError parses NAPI uppercase operation from message", () => {
    const raw = new Error("anonymous session cannot INSERT on table todos");
    const normalized = normalizeRuntimeWriteError(raw);
    expect(normalized).toBeInstanceOf(AnonymousWriteDeniedError);
    expect((normalized as AnonymousWriteDeniedError).table).toBe("todos");
    expect((normalized as AnonymousWriteDeniedError).operation).toBe("insert");
  });

  it("normalizeRuntimeWriteError passes through unrelated errors unchanged", () => {
    const raw = new Error("Insert failed: ObjectNotFound(...)");
    expect(normalizeRuntimeWriteError(raw)).toBe(raw);
  });
});
