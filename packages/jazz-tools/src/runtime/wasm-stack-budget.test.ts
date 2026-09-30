import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { describe, expect, it } from "vitest";

const require = createRequire(import.meta.url);

function readUleb(bytes: Uint8Array, offset: number): [number, number] {
  let value = 0;
  let shift = 0;
  for (;;) {
    const byte = bytes[offset++]!;
    value += (byte & 0x7f) * 2 ** shift;
    shift += 7;
    if (byte < 0x80) return [value, offset];
  }
}

function readSleb32(bytes: Uint8Array, offset: number): [number, number] {
  let value = 0;
  let shift = 0;
  for (;;) {
    const byte = bytes[offset++]!;
    value |= (byte & 0x7f) << shift;
    shift += 7;
    if (byte < 0x80) {
      if (shift < 32 && byte & 0x40) value |= -1 << shift;
      return [value, offset];
    }
  }
}

/**
 * Initial value of the module's first defined global, which the Rust
 * toolchain's linker makes `__stack_pointer`. The stack is laid out first in
 * linear memory and grows down from this address, so it is the stack size.
 */
function stackPointerStart(wasm: Uint8Array): number {
  let offset = 8;
  while (offset < wasm.length) {
    const id = wasm[offset]!;
    const [size, body] = readUleb(wasm, offset + 1);
    if (id === 6) {
      let [, cursor] = readUleb(wasm, body);
      const valueType = wasm[cursor++];
      const mutable = wasm[cursor++];
      const opcode = wasm[cursor++];
      expect([valueType, mutable, opcode]).toEqual([0x7f, 1, 0x41]);
      return readSleb32(wasm, cursor)[0];
    }
    offset = body + size;
  }
  throw new Error("jazz-wasm module has no global section");
}

describe("jazz-wasm stack budget", () => {
  // wasm32 cannot switch to a fresh stack segment the way native builds do
  // (StackSafeFuture), so the module's single stack must be large enough for
  // the deepest owner turn. With the 1 MiB linker default, unoptimized builds
  // overflowed while compiling an include-deleted read, which surfaced as
  // "memory access out of bounds" and left the module unusable.
  it("reserves at least 8 MiB of stack", () => {
    const wasm = readFileSync(require.resolve("jazz-wasm/pkg/jazz_wasm_bg.wasm"));
    expect(stackPointerStart(wasm)).toBeGreaterThanOrEqual(8 * 1024 * 1024);
  });
});
