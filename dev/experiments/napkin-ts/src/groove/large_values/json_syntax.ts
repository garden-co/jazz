// Streaming JSON syntax validator with bounded state (no token contents retained).
import { MAX_JSON_NESTING_DEPTH } from "../large_values.js";

type ArrayState = "FirstValueOrEnd" | "Value" | "CommaOrEnd";
type ObjectState = "FirstKeyOrEnd" | "Key" | "Colon" | "Value" | "CommaOrEnd";
type Frame = { array: true; state: ArrayState } | { array: false; state: ObjectState };

type NumberState =
  | "Minus"
  | "Zero"
  | "Integer"
  | "Dot"
  | "Fraction"
  | "Exponent"
  | "ExponentSign"
  | "ExponentDigits";

type Token =
  | { kind: "None" }
  | {
      kind: "String";
      key: boolean;
      escaped: boolean;
      unicodeDigits: number;
      unicodeValue: number;
      highSurrogate: boolean;
    }
  | { kind: "Literal"; expected: string; offset: number }
  | { kind: "Number"; number: JsonNumber };

const SERDE_JSON_MAX_CONTAINERS = 127;
const U64_MAX = (1n << 64n) - 1n;
const I32_MAX = 2 ** 31 - 1;
const I32_MIN = -(2 ** 31);
const isDigit = (b: number) => b >= 0x30 && b <= 0x39;
const NONE: Token = { kind: "None" };

const newString = (key: boolean): Token => ({
  kind: "String",
  key,
  escaped: false,
  unicodeDigits: 0,
  unicodeValue: 0,
  highSurrogate: false,
});

export class StreamingJsonValidator {
  private stack: Frame[] = [];
  private token: Token = NONE;
  private rootDone = false;

  push(bytes: Uint8Array): boolean {
    for (const b of bytes) if (!this.consume(b)) return false;
    return true;
  }

  finish(): boolean {
    if (this.token.kind === "Number" && !this.finishNumber()) return false;
    return this.token.kind === "None" && this.stack.length === 0 && this.rootDone;
  }

  /**
   * Offset just past the first complete root value of `bytes` (serde_json's stream
   * deserializer), -1 when more input is needed, or null on a syntax error.
   */
  rootEnd(bytes: Uint8Array): number | null {
    for (let i = 0; i < bytes.length; i++) {
      const t = this.token;
      if (t.kind === "Number" && this.stack.length === 0 && t.number.advance(bytes[i]) === false)
        return this.finishNumber() ? i : null;
      if (!this.consume(bytes[i])) return null;
      if (this.rootDone && this.token.kind === "None") return i + 1;
    }
    if (this.token.kind === "Number" && this.stack.length === 0)
      return this.finishNumber() ? bytes.length : null;
    return -1;
  }

  private consume(byte: number): boolean {
    const t = this.token;
    switch (t.kind) {
      case "String": {
        if (t.unicodeDigits > 0) {
          let digit: number;
          if (isDigit(byte)) digit = byte - 0x30;
          else if (byte >= 0x61 && byte <= 0x66) digit = byte - 0x61 + 10;
          else if (byte >= 0x41 && byte <= 0x46) digit = byte - 0x41 + 10;
          else return false;
          t.unicodeValue = ((t.unicodeValue << 4) | digit) & 0xffff;
          t.unicodeDigits -= 1;
          if (t.unicodeDigits === 0) {
            const low = t.unicodeValue >= 0xdc00 && t.unicodeValue <= 0xdfff;
            if (t.highSurrogate) {
              if (!low) return false;
              t.highSurrogate = false;
            } else {
              if (low) return false;
              t.highSurrogate = t.unicodeValue >= 0xd800 && t.unicodeValue <= 0xdbff;
            }
          }
          return true;
        }
        if (t.escaped) {
          t.escaped = false;
          if (byte === 0x75) {
            t.unicodeDigits = 4;
            t.unicodeValue = 0;
          } else if (t.highSurrogate || !'"\\/bfnrt'.includes(String.fromCharCode(byte)))
            return false;
          return true;
        }
        if (t.highSurrogate && byte !== 0x5c) return false;
        if (byte === 0x5c) t.escaped = true;
        else if (byte === 0x22) {
          this.token = NONE;
          if (t.key) {
            const top = this.stack[this.stack.length - 1];
            if (!top || top.array || (top.state !== "FirstKeyOrEnd" && top.state !== "Key"))
              return false;
            top.state = "Colon";
          } else return this.valueComplete();
        } else if (byte <= 0x1f) return false;
        return true;
      }
      case "Literal":
        if (t.expected.charCodeAt(t.offset) !== byte) return false;
        t.offset += 1;
        if (t.offset === t.expected.length) {
          this.token = NONE;
          return this.valueComplete();
        }
        return true;
      case "Number": {
        const r = t.number.advance(byte);
        if (r === null) return false;
        if (r) return true;
        return this.finishNumber() && this.consumeIdle(byte);
      }
      case "None":
        return this.consumeIdle(byte);
    }
  }

  private consumeIdle(byte: number): boolean {
    if (byte === 0x20 || byte === 0x09 || byte === 0x0a || byte === 0x0d) return true;
    const top = this.stack[this.stack.length - 1];
    if (top?.array && top.state === "CommaOrEnd") {
      if (byte === 0x2c) return this.setState("Value");
      return byte === 0x5d && this.closeContainer(true);
    }
    if (top && !top.array) {
      if (top.state === "CommaOrEnd") {
        if (byte === 0x2c) return this.setState("Key");
        return byte === 0x7d && this.closeContainer(false);
      }
      if (top.state === "Colon") return byte === 0x3a && this.setState("Value");
      if (top.state === "FirstKeyOrEnd" || top.state === "Key") {
        if (byte === 0x7d && top.state === "FirstKeyOrEnd") return this.closeContainer(false);
        if (byte !== 0x22) return false;
        this.token = newString(true);
        return true;
      }
    }
    if (top?.array && top.state === "FirstValueOrEnd" && byte === 0x5d)
      return this.closeContainer(true);
    const valueSlot =
      top === undefined ||
      (top.array && (top.state === "FirstValueOrEnd" || top.state === "Value")) ||
      (!top.array && top.state === "Value");
    if (valueSlot && !this.rootDone) return this.startValue(byte);
    return false;
  }

  private startValue(byte: number): boolean {
    switch (byte) {
      case 0x7b:
      case 0x5b:
        if (this.stack.length >= Math.min(MAX_JSON_NESTING_DEPTH, SERDE_JSON_MAX_CONTAINERS))
          return false;
        this.stack.push(
          byte === 0x5b
            ? { array: true, state: "FirstValueOrEnd" }
            : { array: false, state: "FirstKeyOrEnd" },
        );
        return true;
      case 0x22:
        this.token = newString(false);
        return true;
      case 0x74:
        this.token = { kind: "Literal", expected: "rue", offset: 0 };
        return true;
      case 0x66:
        this.token = { kind: "Literal", expected: "alse", offset: 0 };
        return true;
      case 0x6e:
        this.token = { kind: "Literal", expected: "ull", offset: 0 };
        return true;
    }
    if (byte === 0x2d || isDigit(byte)) {
      this.token = { kind: "Number", number: new JsonNumber(byte) };
      return true;
    }
    return false;
  }

  private finishNumber(): boolean {
    if (this.token.kind !== "Number" || !this.token.number.finish()) return false;
    this.token = NONE;
    return this.valueComplete();
  }

  private valueComplete(): boolean {
    const top = this.stack[this.stack.length - 1];
    if (top?.array && (top.state === "FirstValueOrEnd" || top.state === "Value"))
      top.state = "CommaOrEnd";
    else if (top && !top.array && top.state === "Value") top.state = "CommaOrEnd";
    else if (!top && !this.rootDone) this.rootDone = true;
    else return false;
    return true;
  }

  private closeContainer(array: boolean): boolean {
    const top = this.stack.pop();
    return top !== undefined && top.array === array && this.valueComplete();
  }

  private setState(next: ArrayState | ObjectState): boolean {
    const top = this.stack[this.stack.length - 1];
    if (!top) return false;
    top.state = next as never;
    return true;
  }
}

/** Returns true when consumed, false when the byte ends the number, null on error. */
function advanceNumber(state: NumberState, byte: number): NumberState | false | null {
  const digit = isDigit(byte);
  if (state === "Minus" && byte === 0x30) return "Zero";
  if (state === "Minus" && digit) return "Integer";
  if ((state === "Zero" || state === "Integer") && byte === 0x2e) return "Dot";
  if (state === "Integer" && digit) return "Integer";
  if ((state === "Dot" || state === "Fraction") && digit) return "Fraction";
  if (
    (state === "Zero" || state === "Integer" || state === "Fraction") &&
    (byte === 0x65 || byte === 0x45)
  )
    return "Exponent";
  if (state === "Exponent" && (byte === 0x2b || byte === 0x2d)) return "ExponentSign";
  if ((state === "Exponent" || state === "ExponentSign" || state === "ExponentDigits") && digit)
    return "ExponentDigits";
  if (state === "Zero" || state === "Integer" || state === "Fraction" || state === "ExponentDigits")
    return false;
  return null;
}

/** serde_json's reduced number state: u64 significand plus decimal exponent. */
class JsonNumber {
  state: NumberState;
  significand: bigint;
  decimalExponent = 0;
  integerOverflow = false;
  fractionOverflow = false;
  exponent = 0;
  exponentNegative = false;
  exponentOverflow = false;

  constructor(byte: number) {
    this.state = byte === 0x2d ? "Minus" : byte === 0x30 ? "Zero" : "Integer";
    this.significand = byte === 0x2d ? 0n : BigInt(byte - 0x30);
  }

  advance(byte: number): boolean | null {
    const next = advanceNumber(this.state, byte);
    if (next === null || next === false) return next === false ? false : null;
    this.state = next;
    switch (next) {
      case "Zero":
      case "Integer":
        if (!this.integerOverflow) {
          const d = this.appendDigit(byte);
          if (d !== null) this.significand = d;
          else this.integerOverflow = true;
        }
        if (this.integerOverflow) {
          if (this.decimalExponent === I32_MAX) return null;
          this.decimalExponent += 1;
        }
        break;
      case "Fraction":
        if (!this.fractionOverflow) {
          const d = this.appendDigit(byte);
          if (d !== null) {
            this.significand = d;
            if (this.decimalExponent === I32_MIN) return null;
            this.decimalExponent -= 1;
          } else this.fractionOverflow = true;
        }
        break;
      case "ExponentSign":
        this.exponentNegative = byte === 0x2d;
        break;
      case "ExponentDigits":
        if (!this.exponentOverflow) {
          const e = this.exponent * 10 + (byte - 0x30);
          if (e > I32_MAX) this.exponentOverflow = true;
          else this.exponent = e;
        }
        break;
    }
    return true;
  }

  private appendDigit(byte: number): bigint | null {
    const v = this.significand * 10n + BigInt(byte - 0x30);
    return v > U64_MAX ? null : v;
  }

  finish(): boolean {
    if (!["Zero", "Integer", "Fraction", "ExponentDigits"].includes(this.state)) return false;
    if (this.exponentOverflow) return this.exponentNegative || this.significand === 0n;
    const sat = (v: number) => Math.max(I32_MIN, Math.min(I32_MAX, v));
    const exponent = sat(
      this.exponentNegative
        ? this.decimalExponent - this.exponent
        : this.decimalExponent + this.exponent,
    );
    if (exponent <= 0 || this.significand === 0n) return true;
    return Number.isFinite(Number(`${this.significand}e${exponent}`));
  }
}
