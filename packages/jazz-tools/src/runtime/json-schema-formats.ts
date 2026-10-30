/**
 * The `format` checks of the native runtime (`jsonschema` 0.42), ported so the
 * browser accepts exactly the strings a native runtime accepts. Each check
 * follows the native implementation it names, including its quirks, rather
 * than the RFC it cites.
 *
 * `idn-email`, `idn-hostname`, `iri` and `iri-reference` need IDNA mapping
 * tables and are rejected as unsupported when a schema asserts them.
 */

import { isValidRegexSyntax } from "./json-schema-regex.js";

type FormatCheck = (value: string) => boolean;

function twoDigits(value: string, at: number): number | undefined {
  const tens = value.charCodeAt(at) - 48;
  const ones = value.charCodeAt(at + 1) - 48;
  if (tens < 0 || tens > 9 || ones < 0 || ones > 9) return undefined;
  return tens * 10 + ones;
}

function isLeapYear(year: number): boolean {
  return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
}

/** `is_valid_date`: `YYYY-MM-DD` with a day that exists in that month. */
function isDate(value: string): boolean {
  if (value.length !== 10 || value[4] !== "-" || value[7] !== "-") return false;
  if (!/^[0-9]{4}$/.test(value.slice(0, 4))) return false;
  const year = Number(value.slice(0, 4));
  const month = twoDigits(value, 5);
  const day = twoDigits(value, 8);
  if (month === undefined || month < 1 || month > 12) return false;
  if (day === undefined || day === 0) return false;
  if (month === 2) return day <= (isLeapYear(year) ? 29 : 28);
  return day <= ([4, 6, 9, 11].includes(month) ? 30 : 31);
}

/**
 * `is_valid_time`: `HH:MM:SS[.frac]` then `Z` or a `±HH:MM` offset (both
 * required); a leap second only at 23:59 UTC.
 */
function isTime(value: string): boolean {
  if (value.length < 9 || value[2] !== ":" || value[5] !== ":") return false;
  const hour = twoDigits(value, 0);
  const minute = twoDigits(value, 3);
  const second = twoDigits(value, 6);
  if (hour === undefined || minute === undefined || second === undefined) return false;
  if (hour > 23 || minute > 59 || second > 60) return false;
  let index = 8;
  if (value[index] === ".") {
    index++;
    const start = index;
    while (index < value.length && value.charCodeAt(index) >= 48 && value.charCodeAt(index) <= 57) {
      index++;
    }
    if (index === start) return false;
  }
  if (index === value.length) return false;
  const sign = value[index];
  if (sign === "Z" || sign === "z") {
    return index === value.length - 1 && (second !== 60 || (hour === 23 && minute === 59));
  }
  if (sign !== "+" && sign !== "-") return false;
  if (value.length - index !== 6 || value[index + 3] !== ":") return false;
  const offsetHour = twoDigits(value, index + 1);
  const offsetMinute = twoDigits(value, index + 4);
  if (offsetHour === undefined || offsetMinute === undefined) return false;
  if (offsetHour > 23 || offsetMinute > 59) return false;
  if (second !== 60) return true;
  const direction = sign === "+" ? -1 : 1;
  let utcHour = hour + direction * offsetHour;
  let utcMinute = minute + direction * offsetMinute;
  utcHour += Math.trunc(utcMinute / 60);
  utcMinute %= 60;
  if (utcMinute < 0) {
    utcMinute += 60;
    utcHour -= 1;
  }
  utcHour = (utcHour + 24) % 24;
  return utcHour === 23 && utcMinute === 59;
}

/** `is_valid_datetime`: a date and a time split at the first `T` or `t`. */
function isDateTime(value: string): boolean {
  const separator = value.search(/[Tt]/);
  if (separator < 0) return false;
  return isDate(value.slice(0, separator)) && isTime(value.slice(separator + 1));
}

const DEC_OCTET = "(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])";
const IPV4 = `${DEC_OCTET}(?:\\.${DEC_OCTET}){3}`;
const H16 = "[0-9A-Fa-f]{1,4}";
const LS32 = `(?:${H16}:${H16}|${IPV4})`;
/** RFC 3986 `IPv6address`, which both Rust's `Ipv6Addr` parser and `fluent-uri` implement. */
const IPV6 = [
  `(?:${H16}:){6}${LS32}`,
  `::(?:${H16}:){5}${LS32}`,
  `(?:${H16})?::(?:${H16}:){4}${LS32}`,
  `(?:(?:${H16}:){0,1}${H16})?::(?:${H16}:){3}${LS32}`,
  `(?:(?:${H16}:){0,2}${H16})?::(?:${H16}:){2}${LS32}`,
  `(?:(?:${H16}:){0,3}${H16})?::${H16}:${LS32}`,
  `(?:(?:${H16}:){0,4}${H16})?::${LS32}`,
  `(?:(?:${H16}:){0,5}${H16})?::${H16}`,
  `(?:(?:${H16}:){0,6}${H16})?::`,
]
  .map((alternative) => `(?:${alternative})`)
  .join("|");

const IPV4_ADDRESS = new RegExp(`^${IPV4}$`);
const IPV6_ADDRESS = new RegExp(`^(?:${IPV6})$`);

/** `Ipv4Addr::from_str`: four decimal octets without leading zeros. */
const isIpv4: FormatCheck = (value) => IPV4_ADDRESS.test(value);

/** `Ipv6Addr::from_str`: RFC 4291 text form, no zone. */
const isIpv6: FormatCheck = (value) => IPV6_ADDRESS.test(value);

/**
 * `is_valid_hostname`: dot-separated LDH labels of 1-63 characters, at most
 * 253 in all, no trailing dot, no `--` in positions 3-4 except for `xn--`
 * labels, which must decode to a valid Unicode label.
 */
function isHostname(value: string): boolean {
  if (value.length === 0 || value.length > 253 || value.endsWith(".")) return false;
  if (!/^[A-Za-z0-9.-]*$/.test(value)) return false;
  const labels = value.split(".");
  for (const label of labels) {
    if (label.length === 0 || label.length > 63) return false;
    if (label.startsWith("-") || label.endsWith("-")) return false;
    const punycode = label.startsWith("xn--");
    if (label.slice(2, 4) === "--" && !punycode) return false;
    if (punycode) {
      const decoded = decodePunycode(label.slice(4));
      if (decoded === undefined || !isValidUnicodeLabel(decoded)) return false;
    }
  }
  return true;
}

const PUNYCODE_BASE = 36;
const PUNYCODE_T_MIN = 1;
const PUNYCODE_T_MAX = 26;
const U32_MAX = 0xffffffff;

function adaptBias(delta: number, points: number, first: boolean): number {
  let scaled = Math.trunc(delta / (first ? 700 : 2));
  scaled += Math.trunc(scaled / points);
  let k = 0;
  while (scaled > ((PUNYCODE_BASE - PUNYCODE_T_MIN) * PUNYCODE_T_MAX) >> 1) {
    scaled = Math.trunc(scaled / (PUNYCODE_BASE - PUNYCODE_T_MIN));
    k += PUNYCODE_BASE;
  }
  return k + Math.trunc(((PUNYCODE_BASE - PUNYCODE_T_MIN + 1) * scaled) / (scaled + 38));
}

/** `idna::punycode::decode_to_string` for an ASCII payload, with its u32 overflow checks. */
function decodePunycode(input: string): string | undefined {
  const delimiter = input.lastIndexOf("-");
  const base = delimiter > 0 ? input.slice(0, delimiter) : "";
  // A leading `-` is not a delimiter; it then fails as a digit.
  const encoded = delimiter > 0 ? input.slice(delimiter + 1) : input;
  const output: number[] = [...base].map((char) => char.charCodeAt(0));
  let codePoint = 0x80;
  let bias = 72;
  let i = 0;
  let position = 0;
  while (position < encoded.length) {
    const previous = i;
    let weight = 1;
    for (let k = PUNYCODE_BASE; ; k += PUNYCODE_BASE) {
      if (position >= encoded.length) return undefined;
      const digit = punycodeDigit(encoded.charCodeAt(position++));
      if (digit === undefined) return undefined;
      i += digit * weight;
      if (digit * weight > U32_MAX || i > U32_MAX) return undefined;
      const t = k <= bias ? PUNYCODE_T_MIN : k >= bias + PUNYCODE_T_MAX ? PUNYCODE_T_MAX : k - bias;
      if (digit < t) break;
      weight *= PUNYCODE_BASE - t;
      if (weight > U32_MAX) return undefined;
    }
    const length = output.length + 1;
    bias = adaptBias(i - previous, length, previous === 0);
    codePoint += Math.trunc(i / length);
    if (codePoint > U32_MAX) return undefined;
    i %= length;
    if (codePoint > 0x10ffff || (codePoint >= 0xd800 && codePoint <= 0xdfff)) return undefined;
    output.splice(i, 0, codePoint);
    i++;
  }
  return String.fromCodePoint(...output);
}

function punycodeDigit(code: number): number | undefined {
  if (code >= 48 && code <= 57) return code - 48 + 26;
  if (code >= 65 && code <= 90) return code - 65;
  if (code >= 97 && code <= 122) return code - 97;
  return undefined;
}

/** Characters RFC 5892 allows after a ZERO WIDTH JOINER (virama-like marks). */
const ZWJ_PRECEDING = new Set(
  [
    0x094d, 0x09cd, 0x0a4d, 0x0acd, 0x0b4d, 0x0bcd, 0x0c4d, 0x0ccd, 0x0d4d, 0x0dca, 0x0e3a, 0x0f84,
    0x1039, 0x1714, 0x1734, 0x17d2, 0x1a60, 0x1b44, 0x1baa, 0x1bf2, 0x1bf3, 0x2d7f, 0xa806, 0xa8c4,
    0xa953, 0xabed, 0x10a3f, 0x11046, 0x1107f, 0x110b9, 0x11133, 0x111c0, 0x11235, 0x112ea, 0x1134d,
    0x11442, 0x114c2, 0x115bf, 0x1163f, 0x116b6, 0x1172b, 0x11839, 0x119e0, 0x11a34, 0x11a47,
    0x11a99, 0x11c3f, 0x11d44, 0x11d45, 0x11d97,
  ].map((code) => String.fromCodePoint(code)),
);

const DISALLOWED_IN_LABEL = new Set(
  [0x0640, 0x07fa, 0x302e, 0x302f, 0x3031, 0x3032, 0x3033, 0x3034, 0x3035, 0x303b].map((code) =>
    String.fromCodePoint(code),
  ),
);

/** `validate_unicode_label`: the RFC 5892 contextual rules the native runtime checks. */
function isValidUnicodeLabel(label: string): boolean {
  const chars = [...label];
  if (chars.length > 0 && /^\p{M}/u.test(chars[0]!)) return false;
  let katakanaMiddleDot = false;
  let hiraganaKatakanaHan = false;
  let arabicIndicDigits = false;
  let extendedArabicIndicDigits = false;
  for (let index = 0; index < chars.length; index++) {
    const char = chars[index]!;
    const previous = chars[index - 1];
    const next = chars[index + 1];
    const code = char.codePointAt(0)!;
    if (char === "‍") {
      if (previous === undefined || !ZWJ_PRECEDING.has(previous)) return false;
    } else if (char === "·") {
      if (previous !== "l" || next !== "l") return false;
    } else if (char === "͵") {
      const nextCode = next?.codePointAt(0);
      if (nextCode === undefined || nextCode < 0x0370 || nextCode > 0x03ff) return false;
    } else if (char === "׳" || char === "״") {
      const previousCode = previous?.codePointAt(0);
      if (previousCode === undefined || previousCode < 0x0590 || previousCode > 0x05ff) {
        return false;
      }
    } else if (char === "・") {
      katakanaMiddleDot = true;
    } else if (
      (code >= 0x3040 && code <= 0x309f) ||
      (code >= 0x30a0 && code <= 0x30ff) ||
      (code >= 0x4e00 && code <= 0x9fff)
    ) {
      hiraganaKatakanaHan = true;
    } else if (code >= 0x0660 && code <= 0x0669) {
      arabicIndicDigits = true;
    } else if (code >= 0x06f0 && code <= 0x06f9) {
      extendedArabicIndicDigits = true;
    } else if (DISALLOWED_IN_LABEL.has(char)) {
      return false;
    }
  }
  return !(
    (katakanaMiddleDot && !hiraganaKatakanaHan) ||
    (arabicIndicDigits && extendedArabicIndicDigits)
  );
}

const utf8 = new TextEncoder();

function utf8Length(value: string): number {
  return utf8.encode(value).length;
}

/**
 * Whether the code point's *value* (not its UTF-8 encoding) looks like a UTF-8
 * multi-byte sequence, which is what `email_address` 0.2 tests.
 */
function isUtf8NonAscii(char: string): boolean {
  const code = char.codePointAt(0)!;
  const b0 = code >>> 24;
  const b1 = (code >>> 16) & 0xff;
  const b2 = (code >>> 8) & 0xff;
  const b3 = code & 0xff;
  const tail = (byte: number) => byte >= 0x80 && byte <= 0xbf;
  if (b0 === 0 && b1 === 0) return b2 >= 0xc2 && b2 <= 0xdf && tail(b3);
  if (b0 === 0) {
    if (b1 === 0xe0) return b2 >= 0xa0 && b2 <= 0xbf && tail(b3);
    if (b1 >= 0xe1 && b1 <= 0xec) return tail(b2) && tail(b3);
    if (b1 === 0xed) return b2 >= 0x80 && b2 <= 0x9f && tail(b3);
    if (b1 === 0xee || b1 === 0xef) return tail(b2) && tail(b3);
    return false;
  }
  if (b0 === 0xf0) return b1 >= 0x90 && b1 <= 0xbf && tail(b2) && tail(b3);
  if (b0 >= 0xf1 && b0 <= 0xf3) return tail(b1) && tail(b2) && tail(b3);
  if (b0 === 0xf4) return b1 >= 0x80 && b1 <= 0x8f && tail(b2) && tail(b3);
  return false;
}

/** Rust's `char::is_alphanumeric`. */
const ALPHANUMERIC = /^[\p{Alphabetic}\p{N}]$/u;
const ATEXT_PUNCTUATION = new Set("!#$%&'*+-/=?^_`{|}~");

function isAtext(char: string): boolean {
  return ALPHANUMERIC.test(char) || ATEXT_PUNCTUATION.has(char) || isUtf8NonAscii(char);
}

function isAtom(value: string): boolean {
  return value.length > 0 && [...value].every(isAtext);
}

function isQcontent(value: string): boolean {
  const chars = [...value];
  for (let index = 0; index < chars.length; index++) {
    const char = chars[index]!;
    const code = char.codePointAt(0)!;
    if (char === "\\") {
      const escaped = chars[++index]?.codePointAt(0);
      if (escaped === undefined || escaped < 0x21 || escaped > 0x7e) return false;
    } else if (
      !(
        char === " " ||
        char === "\t" ||
        code === 0x21 ||
        (code >= 0x23 && code <= 0x5b) ||
        (code >= 0x5d && code <= 0x7e) ||
        isUtf8NonAscii(char)
      )
    ) {
      return false;
    }
  }
  return true;
}

function isDtext(char: string): boolean {
  const code = char.codePointAt(0)!;
  return (code >= 0x21 && code <= 0x5a) || (code >= 0x5e && code <= 0x7e) || isUtf8NonAscii(char);
}

/** Rust's `str::trim`: Unicode `White_Space` at both ends. */
function rustTrim(value: string): string {
  return value.replace(/^\p{White_Space}+|\p{White_Space}+$/gu, "");
}

/**
 * `email_address` 0.2 with default options (display names and domain
 * literals allowed), then the native runtime's own domain check: a hostname,
 * or an IPv4 / `IPv6:` literal in brackets.
 */
function isEmail(value: string): boolean {
  let address = value;
  let display = "";
  const displayStart = value.lastIndexOf(" <");
  if (displayStart >= 0) {
    const right = rustTrim(value.slice(displayStart + 2));
    if (!right.endsWith(">")) return false;
    address = right.slice(0, -1);
    display = rustTrim(value.slice(0, displayStart));
  }
  const at = address.lastIndexOf("@");
  if (at < 0) return false;
  const localPart = address.slice(0, at);
  const domain = address.slice(at + 1);
  if (display === "" && localPart.startsWith("<")) return false;

  if (localPart.length === 0 || utf8Length(localPart) > 64) return false;
  if (localPart.startsWith('"') && localPart.endsWith('"')) {
    if (localPart.length <= 2 || !isQcontent(localPart.slice(1, -1))) return false;
  } else if (!localPart.split(".").every(isAtom)) {
    return false;
  }

  if (domain.length === 0 || utf8Length(domain) > 254) return false;
  if (domain.startsWith("[") && domain.endsWith("]")) {
    const literal = domain.slice(1, -1);
    if (![...literal].every(isDtext)) return false;
    return literal.startsWith("IPv6:") ? isIpv6(literal.slice(5)) : isIpv4(literal);
  }
  for (const label of domain.split(".")) {
    const chars = [...label];
    if (chars.length === 0) return false;
    if (!ALPHANUMERIC.test(chars[0]!) || !ALPHANUMERIC.test(chars[chars.length - 1]!)) {
      return false;
    }
    if (utf8Length(label) > 63 || !isAtom(label)) return false;
  }
  return isHostname(domain);
}

const UNRESERVED = "A-Za-z0-9\\-._~";
const SUB_DELIMS = "!$&'()*+,;=";
const PCT_ENCODED = "%[0-9A-Fa-f]{2}";
const PCHAR = `(?:[${UNRESERVED}${SUB_DELIMS}:@]|${PCT_ENCODED})`;
const SEGMENT = `${PCHAR}*`;
const SEGMENT_NZ = `${PCHAR}+`;
const SEGMENT_NZ_NC = `(?:[${UNRESERVED}${SUB_DELIMS}@]|${PCT_ENCODED})+`;
const IP_LITERAL = `\\[(?:${IPV6}|[vV][0-9A-Fa-f]+\\.[${UNRESERVED}${SUB_DELIMS}:]+)\\]`;
const REG_NAME = `(?:[${UNRESERVED}${SUB_DELIMS}]|${PCT_ENCODED})*`;
const USERINFO = `(?:[${UNRESERVED}${SUB_DELIMS}:]|${PCT_ENCODED})*`;
const AUTHORITY = `(?:${USERINFO}@)?(?:${IP_LITERAL}|${REG_NAME})(?::[0-9]*)?`;
const PATH_ABEMPTY = `(?:/${SEGMENT})*`;
const PATH_ABSOLUTE = `/(?:${SEGMENT_NZ}(?:/${SEGMENT})*)?`;
const PATH_ROOTLESS = `${SEGMENT_NZ}(?:/${SEGMENT})*`;
const PATH_NOSCHEME = `${SEGMENT_NZ_NC}(?:/${SEGMENT})*`;
const QUERY_OR_FRAGMENT = `(?:${PCHAR}|[/?])*`;
const SCHEME = "[A-Za-z][A-Za-z0-9+\\-.]*";
const HIER_PART = `(?://${AUTHORITY}${PATH_ABEMPTY}|${PATH_ABSOLUTE}|${PATH_ROOTLESS}|)`;
const RELATIVE_PART = `(?://${AUTHORITY}${PATH_ABEMPTY}|${PATH_ABSOLUTE}|${PATH_NOSCHEME}|)`;
const TAIL = `(?:\\?${QUERY_OR_FRAGMENT})?(?:#${QUERY_OR_FRAGMENT})?`;

/** RFC 3986 `URI`, as `fluent_uri::Uri::parse` accepts it. */
const URI = new RegExp(`^${SCHEME}:${HIER_PART}${TAIL}$`);
/** RFC 3986 `URI-reference`, as `fluent_uri::UriRef::parse` accepts it. */
const URI_REFERENCE = new RegExp(`^(?:${SCHEME}:${HIER_PART}|${RELATIVE_PART})${TAIL}$`);

/** `is_valid_json_pointer`: empty, or `/`-prefixed with `~` only in `~0`/`~1`. */
function isJsonPointer(value: string): boolean {
  return value === "" || /^(?:\/(?:[^~/]|~[01])*)+$/.test(value);
}

/** `is_valid_relative_json_pointer`: a non-negative integer, then `#` or a JSON pointer. */
function isRelativeJsonPointer(value: string): boolean {
  const match = /^(0|[1-9][0-9]*)(.*)$/s.exec(value);
  if (!match) return false;
  const rest = match[2]!;
  return rest === "" || rest === "#" || (rest.startsWith("/") && isJsonPointer(rest));
}

const URI_TEMPLATE_OPERATORS = new Set("+#./;?&=,!@|");
const URI_TEMPLATE_LITERAL_EXCLUDED = new Set("\"'<>%\\^`{|}");

/** `is_valid_uri_template`: RFC 6570 level 4 syntax. */
function isUriTemplate(value: string): boolean {
  let index = 0;
  const isHex = (at: number) => /^[0-9A-Fa-f]$/.test(value[at] ?? "");
  const isVarcharStart = (at: number) => /^[A-Za-z0-9_]$/.test(value[at] ?? "");
  const parseVarchar = (): boolean => {
    const start = index;
    while (index < value.length) {
      if (isVarcharStart(index)) {
        index++;
      } else if (value[index] === "%") {
        if (index + 2 >= value.length || !isHex(index + 1) || !isHex(index + 2)) return false;
        index += 3;
      } else {
        break;
      }
    }
    return index > start;
  };
  const parseVarname = (): boolean => {
    if (!parseVarchar()) return false;
    while (index < value.length) {
      if (value[index] === ".") {
        index++;
        if (!parseVarchar()) return false;
      } else if (isVarcharStart(index) || value[index] === "%") {
        if (!parseVarchar()) return false;
      } else {
        break;
      }
    }
    return true;
  };
  const parseVarspec = (): boolean => {
    if (!parseVarname()) return false;
    if (value[index] === ":") {
      index++;
      if (!/^[1-9]$/.test(value[index] ?? "")) return false;
      index++;
      for (let digits = 1; digits < 4 && /^[0-9]$/.test(value[index] ?? ""); digits++) index++;
    } else if (value[index] === "*") {
      index++;
    }
    return true;
  };
  while (index < value.length) {
    const char = value[index]!;
    if (char === "{") {
      index++;
      if (index >= value.length) return false;
      if (URI_TEMPLATE_OPERATORS.has(value[index]!)) {
        index++;
        if (index >= value.length) return false;
      }
      if (!parseVarspec()) return false;
      while (value[index] === ",") {
        index++;
        if (!parseVarspec()) return false;
      }
      if (value[index] !== "}") return false;
      index++;
    } else if (char === "}") {
      return false;
    } else if (char === "%") {
      if (index + 2 >= value.length || !isHex(index + 1) || !isHex(index + 2)) return false;
      index += 3;
    } else {
      const code = value.charCodeAt(index);
      if (code <= 0x20 || code === 0x7f || URI_TEMPLATE_LITERAL_EXCLUDED.has(char)) return false;
      index++;
    }
  }
  return true;
}

/** Checks for the `format`s the browser asserts, by name. */
export const FORMAT_CHECKS: Record<string, FormatCheck> = {
  date: isDate,
  "date-time": isDateTime,
  email: isEmail,
  hostname: isHostname,
  ipv4: isIpv4,
  ipv6: isIpv6,
  "json-pointer": isJsonPointer,
  regex: isValidRegexSyntax,
  "relative-json-pointer": isRelativeJsonPointer,
  time: isTime,
  uri: (value) => URI.test(value),
  "uri-reference": (value) => URI_REFERENCE.test(value),
  "uri-template": isUriTemplate,
};
