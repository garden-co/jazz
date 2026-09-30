/**
 * JSON values as the native validator sees them.
 *
 * The native runtime parses JSON with `serde_json`, which keeps integers that
 * fit `u64`/`i64` exact and turns every other number into an `f64`. `JSON.parse`
 * would round large integers to doubles, so this parser keeps them as `bigint`
 * when a double cannot hold them. It also marks numbers written with a
 * fraction or exponent whose value is whole (`1.0`, `1e3`): draft 4 does not
 * count those as integers. Objects get no prototype, so keys like `__proto__`
 * or `toString` are ordinary keys.
 */

/** A whole number written with a fraction or exponent, or too large for `i64`/`u64`. */
export class IntegralFloat {
  constructor(readonly value: number) {}
}

export type JsonNumber = number | bigint | IntegralFloat;

const U64_MAX = 2n ** 64n - 1n;
const I64_MIN = -(2n ** 63n);

const NUMBER = /-?(?:0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?/y;
const WHITESPACE = /[ \t\n\r]*/y;

/** Parse JSON text into values that keep the native validator's number semantics. */
export function parseJson(text: string): unknown {
  let index = 0;

  const fail = (): never => {
    throw new SyntaxError(`invalid JSON at position ${index}`);
  };

  const skipWhitespace = () => {
    WHITESPACE.lastIndex = index;
    WHITESPACE.test(text);
    index = WHITESPACE.lastIndex;
  };

  const parseString = (): string => {
    const start = index;
    index++;
    let escaped = false;
    while (index < text.length) {
      const char = text.charCodeAt(index);
      if (char === 0x22) {
        index++;
        const literal = text.slice(start, index);
        return escaped ? (JSON.parse(literal) as string) : literal.slice(1, -1);
      }
      if (char === 0x5c) {
        escaped = true;
        index++;
      } else if (char < 0x20) {
        fail();
      }
      index++;
    }
    return fail();
  };

  const parseNumber = (): JsonNumber => {
    NUMBER.lastIndex = index;
    const match = NUMBER.exec(text);
    if (!match) return fail();
    index = NUMBER.lastIndex;
    const literal = match[0];
    const value = Number(literal);
    if (match[1] !== undefined || match[2] !== undefined) {
      return Number.isInteger(value) ? new IntegralFloat(value) : value;
    }
    if (Number.isSafeInteger(value)) return value;
    const exact = BigInt(literal);
    if (exact < I64_MIN || exact > U64_MAX) return new IntegralFloat(value);
    return BigInt(value) === exact ? value : exact;
  };

  const parseValue = (): unknown => {
    skipWhitespace();
    const char = text[index];
    if (char === "{") {
      index++;
      const object: Record<string, unknown> = Object.create(null);
      skipWhitespace();
      if (text[index] === "}") {
        index++;
        return object;
      }
      for (;;) {
        skipWhitespace();
        if (text[index] !== '"') fail();
        const key = parseString();
        skipWhitespace();
        if (text[index] !== ":") fail();
        index++;
        object[key] = parseValue();
        skipWhitespace();
        if (text[index] === ",") {
          index++;
        } else if (text[index] === "}") {
          index++;
          return object;
        } else {
          fail();
        }
      }
    }
    if (char === "[") {
      index++;
      const array: unknown[] = [];
      skipWhitespace();
      if (text[index] === "]") {
        index++;
        return array;
      }
      for (;;) {
        array.push(parseValue());
        skipWhitespace();
        if (text[index] === ",") {
          index++;
        } else if (text[index] === "]") {
          index++;
          return array;
        } else {
          fail();
        }
      }
    }
    if (char === '"') return parseString();
    if (text.startsWith("true", index)) {
      index += 4;
      return true;
    }
    if (text.startsWith("false", index)) {
      index += 5;
      return false;
    }
    if (text.startsWith("null", index)) {
      index += 4;
      return null;
    }
    return parseNumber();
  };

  const value = parseValue();
  skipWhitespace();
  if (index !== text.length) fail();
  return value;
}

export function isJsonNumber(value: unknown): value is JsonNumber {
  return typeof value === "number" || typeof value === "bigint" || value instanceof IntegralFloat;
}

/** The number's value; `bigint`s are exact, and compare exactly with doubles. */
export function numericValue(value: JsonNumber): number | bigint {
  return value instanceof IntegralFloat ? value.value : value;
}

/** Whether `value` counts as an integer: from draft 6 on any whole number does. */
export function isJsonInteger(value: JsonNumber, draft4: boolean): boolean {
  if (typeof value === "bigint") return true;
  if (value instanceof IntegralFloat) return !draft4;
  return Number.isInteger(value);
}

/** Replace `IntegralFloat`s with their values (for schemas, once meta-validated). */
export function withPlainNumbers(value: unknown): unknown {
  if (value instanceof IntegralFloat) return value.value;
  if (Array.isArray(value)) return value.map(withPlainNumbers);
  if (typeof value !== "object" || value === null) return value;
  const object: Record<string, unknown> = Object.create(null);
  for (const [key, entry] of Object.entries(value)) object[key] = withPlainNumbers(entry);
  return object;
}

/** JSON Schema equality: numbers compare by value, objects ignore key order. */
export function jsonEqual(a: unknown, b: unknown): boolean {
  if (isJsonNumber(a)) {
    // `==` compares a `bigint` with a double exactly.
    // eslint-disable-next-line eqeqeq
    return isJsonNumber(b) && numericValue(a) == numericValue(b);
  }
  if (Array.isArray(a)) {
    if (!Array.isArray(b) || a.length !== b.length) return false;
    return a.every((entry, index) => jsonEqual(entry, b[index]));
  }
  if (typeof a === "object" && a !== null) {
    if (typeof b !== "object" || b === null || Array.isArray(b) || isJsonNumber(b)) return false;
    const aKeys = Object.keys(a);
    if (aKeys.length !== Object.keys(b).length) return false;
    return aKeys.every(
      (key) =>
        Object.prototype.hasOwnProperty.call(b, key) &&
        jsonEqual((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key]),
    );
  }
  return a === b;
}

/**
 * `multipleOf` as the native validator decides it. Both sides are taken as
 * doubles. A whole divisor needs a whole remainder-free instance; any other
 * divisor is compared as exact fractions of the decimal values, and (like the
 * native runtime) only instances that are zero or at least the divisor pass.
 */
export function isMultipleOf(value: JsonNumber, multiple: JsonNumber): boolean {
  const instance = Number(numericValue(value));
  const divisor = Number(numericValue(multiple));
  if (Number.isInteger(divisor)) {
    return Number.isInteger(instance) && instance % divisor === 0;
  }
  if (instance === 0) return true;
  if (instance < divisor) return false;
  const [instanceNumerator, instanceDenominator] = toFraction(instance);
  const [divisorNumerator, divisorDenominator] = toFraction(divisor);
  const denominator = instanceDenominator * divisorNumerator;
  return (instanceNumerator * divisorDenominator) % denominator === 0n;
}

/**
 * The fraction the native runtime's `fraction` crate makes of a double: the
 * double scaled by the first power of ten that makes it whole (within
 * `EPSILON`), over that power of ten.
 */
function toFraction(value: number): [bigint, bigint] {
  let power = 0;
  let scaled = value;
  while (!(Math.abs(Math.floor(scaled) - scaled) < Number.EPSILON)) {
    power++;
    scaled = value * powi(10, power);
    if (!Number.isFinite(scaled)) return decimalFraction(value);
  }
  const numerator = BigInt(Math.trunc(Math.abs(scaled)));
  return [value < 0 ? -numerator : numerator, BigInt(powi(10, power))];
}

/** `f64::powi` (compiler-rt `__powidf2`), so rounding matches the native runtime. */
function powi(base: number, exponent: number): number {
  let result = 1;
  let factor = base;
  for (let remaining = exponent; ; ) {
    if (remaining & 1) result *= factor;
    remaining = Math.trunc(remaining / 2);
    if (remaining === 0) break;
    factor *= factor;
  }
  return result;
}

/** The exact fraction of a double's shortest decimal representation. */
function decimalFraction(value: number): [bigint, bigint] {
  const [mantissa, exponentText] = Math.abs(value).toExponential().split("e");
  const [whole, fraction = ""] = mantissa!.split(".");
  const exponent = Number(exponentText) - fraction.length;
  let numerator = BigInt(whole! + fraction);
  let denominator = 1n;
  if (exponent >= 0) numerator *= 10n ** BigInt(exponent);
  else denominator = 10n ** BigInt(-exponent);
  return [value < 0 ? -numerator : numerator, denominator];
}
