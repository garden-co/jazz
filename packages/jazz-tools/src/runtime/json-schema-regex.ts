/**
 * Regular expressions for `pattern`, `patternProperties` and the `regex`
 * format, with the native runtime's syntax and semantics.
 *
 * The native runtime (`jsonschema` 0.42) parses patterns with `regex-syntax`
 * after rewriting `\cX`, maps `\d`, `\w` and `\s` to fixed ASCII-ish sets, and
 * compiles the result with `fancy-regex`; a pattern with a lookaround or a
 * backreference is compiled untranslated instead, so its classes stay
 * Unicode-aware. This module parses the same syntax and emits an equivalent
 * JS regular expression (`u` flag), so a pattern means the same thing here:
 * `\-`, `[]a]`, `\z` or `(?m)` work as they do natively, and syntax the
 * native runtime rejects is rejected here too.
 *
 * Constructs with no JS equivalent are rejected as unsupported rather than
 * approximated: `(?x)`, `(?R)`, `(?-u)`, and `(?i)` anywhere but at the very
 * start of the pattern.
 */

/** The pattern is invalid for the native runtime too. */
export class RegexSyntaxError extends Error {}

/** The pattern is valid natively, but the browser runtime cannot evaluate it. */
export class UnsupportedRegexError extends Error {}

interface Flags {
  multiLine: boolean;
  dotAll: boolean;
  swapGreed: boolean;
}

type Assertion =
  | "start"
  | "end"
  | "startText"
  | "endText"
  | "wordBoundary"
  | "notWordBoundary"
  | "wordStart"
  | "wordEnd"
  | "wordStartHalf"
  | "wordEndHalf";

type PerlKind = "d" | "s" | "w";
type AsciiKind =
  | "alnum"
  | "alpha"
  | "ascii"
  | "blank"
  | "cntrl"
  | "digit"
  | "graph"
  | "lower"
  | "print"
  | "punct"
  | "space"
  | "upper"
  | "word"
  | "xdigit";

type ClassItem =
  | { kind: "literal"; char: string }
  | { kind: "range"; start: string; end: string }
  | { kind: "perl"; perl: PerlKind; negated: boolean }
  | { kind: "unicode"; name: string; negated: boolean }
  | { kind: "ascii"; ascii: AsciiKind; negated: boolean }
  | { kind: "bracket"; negated: boolean; set: ClassSet };

type ClassSet =
  | { kind: "union"; items: ClassItem[] }
  | { kind: "op"; op: "&&" | "--" | "~~"; lhs: ClassSet; rhs: ClassSet };

type Node =
  | { kind: "flags" }
  | { kind: "literal"; char: string }
  | { kind: "dot"; dotAll: boolean }
  | { kind: "assertion"; assertion: Assertion; multiLine: boolean }
  | { kind: "class"; item: ClassItem }
  | { kind: "group"; capture: boolean; body: Node }
  | { kind: "look"; op: "=" | "!" | "<=" | "<!"; body: Node }
  | { kind: "backref"; index: number }
  | { kind: "repeat"; body: Node; min: number; max: number; greedy: boolean }
  | { kind: "concat"; items: Node[] }
  | { kind: "alternation"; items: Node[] };

interface Parsed {
  node: Node;
  /** Whether the native runtime compiles the pattern untranslated (lookaround, backreference). */
  fancy: boolean;
  caseInsensitive: boolean;
  captures: number;
  unsupported: string[];
  /** Why `fancy-regex`, which parses the translated pattern natively, rejects it. */
  fancyInvalid: string[];
}

const META_CHARACTERS = new Set("\\.+*?()|[]{}^$#&-~");
const FLAG_CHARACTERS = new Set("imsUuRx");
const ASCII_KINDS = new Set<string>([
  "alnum",
  "alpha",
  "ascii",
  "blank",
  "cntrl",
  "digit",
  "graph",
  "lower",
  "print",
  "punct",
  "space",
  "upper",
  "word",
  "xdigit",
]);

/** `regex_syntax::is_escapeable_character`. */
function isEscapeable(char: string): boolean {
  if (META_CHARACTERS.has(char)) return true;
  const code = char.codePointAt(0)!;
  if (code > 0x7f) return false;
  return !/[0-9A-Za-z<>]/.test(char);
}

class Parser {
  private readonly chars: string[];
  private index = 0;
  private flags: Flags = { multiLine: false, dotAll: false, swapGreed: false };
  private fancy = false;
  private caseInsensitive = false;
  private captures = 0;
  private readonly names = new Set<string>();
  private readonly unsupported: string[] = [];
  private readonly fancyInvalid: string[] = [];
  private bellOutsideClass = false;

  constructor(private readonly pattern: string) {
    this.chars = [...pattern];
  }

  parse(): Parsed {
    let node: Node;
    try {
      node = this.parseAlternation(0);
      if (!this.eof()) this.fail("unopened group");
    } catch (error) {
      // A lookaround or backreference before the first syntax error sends the
      // native runtime down its untranslated path, which parses on its own.
      if (error instanceof RegexSyntaxError && this.fancy) {
        throw new UnsupportedRegexError(error.message);
      }
      throw error;
    }
    if (!this.fancy && this.bellOutsideClass) this.fail("`\\a` is not supported");
    return {
      node,
      fancy: this.fancy,
      caseInsensitive: this.caseInsensitive,
      captures: this.captures,
      unsupported: this.unsupported,
      fancyInvalid: this.fancyInvalid,
    };
  }

  private eof(): boolean {
    return this.index >= this.chars.length;
  }

  private char(): string {
    return this.chars[this.index]!;
  }

  private peek(offset = 1): string | undefined {
    return this.chars[this.index + offset];
  }

  private startsWith(text: string): boolean {
    return this.chars.slice(this.index, this.index + text.length).join("") === text;
  }

  private fail(message: string): never {
    throw new RegexSyntaxError(message);
  }

  private parseAlternation(depth: number): Node {
    const branches = [this.parseConcat(depth)];
    while (!this.eof() && this.char() === "|") {
      this.index++;
      branches.push(this.parseConcat(depth));
    }
    return branches.length === 1 ? branches[0]! : { kind: "alternation", items: branches };
  }

  private parseConcat(depth: number): Node {
    const items: Node[] = [];
    // Quantifiers in a row: `regex-syntax` nests them, but `fancy-regex`
    // (which parses the pattern again) reads a `+` after a quantifier as
    // possessive, a `{` as a literal, and rejects anything else.
    let quantifiers = 0;
    while (!this.eof()) {
      const char = this.char();
      if (char === "|" || char === ")") break;
      if (char === "?" || char === "*" || char === "+" || char === "{") {
        quantifiers++;
        if (quantifiers === 2 && char === "+") {
          this.unsupported.push("possessive quantifiers");
        } else if (quantifiers === 2 && char === "{") {
          this.unsupported.push("a counted repetition right after another quantifier");
        } else if (quantifiers >= 2) {
          this.fancyInvalid.push("a quantifier right after another quantifier");
        }
      } else {
        quantifiers = 0;
      }
      if (char === "(") {
        items.push(this.parseGroup(depth));
      } else if (char === "[") {
        items.push({ kind: "class", item: this.parseBracketed() });
      } else if (char === "?" || char === "*" || char === "+") {
        this.index++;
        const [min, max] = char === "?" ? [0, 1] : char === "*" ? [0, Infinity] : [1, Infinity];
        let greedy = true;
        if (!this.eof() && this.char() === "?") {
          greedy = false;
          this.index++;
        }
        this.repeatLast(items, min, max, greedy);
      } else if (char === "{") {
        this.parseCountedRepetition(items);
      } else {
        items.push(this.parsePrimitive());
      }
    }
    return items.length === 1 ? items[0]! : { kind: "concat", items };
  }

  private repeatLast(items: Node[], min: number, max: number, greedy: boolean): void {
    const body = items.pop();
    if (body === undefined || body.kind === "flags")
      this.fail("repetition operator missing expression");
    if (body.kind === "look") this.fancyInvalid.push("a quantifier on a lookaround");
    items.push({ kind: "repeat", body, min, max, greedy: greedy !== this.flags.swapGreed });
  }

  private skipWhitespaceInRepetition(): void {
    let skipped = false;
    while (!this.eof() && /\p{White_Space}/u.test(this.char())) {
      this.index++;
      skipped = true;
    }
    // `fancy-regex` reads `{` as a literal when a count has spaces.
    if (skipped) this.unsupported.push("spaces inside a counted repetition");
  }

  private parseDecimal(): number | undefined {
    this.skipWhitespaceInRepetition();
    let digits = "";
    while (!this.eof() && /[0-9]/.test(this.char())) digits += this.chars[this.index++];
    this.skipWhitespaceInRepetition();
    if (digits === "") return undefined;
    const value = Number(digits);
    if (value > 0xffffffff) this.fail("repetition count is too large");
    return value;
  }

  private parseCountedRepetition(items: Node[]): void {
    const last = items[items.length - 1];
    if (last === undefined || last.kind === "flags")
      this.fail("repetition operator missing expression");
    this.index++;
    if (this.eof()) this.fail("unclosed counted repetition");
    const min = this.parseDecimal();
    if (this.eof()) this.fail("unclosed counted repetition");
    let max: number;
    if (this.char() === ",") {
      this.index++;
      if (this.eof()) this.fail("unclosed counted repetition");
      if (this.char() !== "}") {
        const end = this.parseDecimal();
        if (min === undefined || end === undefined) this.fail("repetition count is empty");
        max = end;
      } else {
        if (min === undefined) this.fail("repetition count is empty");
        max = Infinity;
      }
    } else {
      if (min === undefined) this.fail("repetition count is empty");
      max = min;
    }
    if (this.eof() || this.char() !== "}") this.fail("unclosed counted repetition");
    this.index++;
    let greedy = true;
    if (!this.eof() && this.char() === "?") {
      greedy = false;
      this.index++;
    }
    if (min > max) this.fail("invalid repetition range");
    this.repeatLast(items, min, max, greedy);
  }

  private parseGroup(depth: number): Node {
    const atStart = this.index === 0;
    this.index++;
    for (const op of ["=", "!", "<=", "<!"] as const) {
      if (this.startsWith(`?${op}`)) {
        this.fancy = true;
        this.index += op.length + 1;
        const body = this.parseGroupBody(depth);
        return { kind: "look", op, body };
      }
    }
    if (this.startsWith("?P<") || this.startsWith("?<")) {
      this.index += this.startsWith("?P<") ? 3 : 2;
      this.captures++;
      this.parseCaptureName();
      return { kind: "group", capture: true, body: this.parseGroupBody(depth) };
    }
    if (!this.eof() && this.char() === "?") {
      this.index++;
      if (this.eof()) this.fail("unclosed group");
      const saved = this.flags;
      const setsCaseInsensitive = this.parseFlags();
      const end = this.char();
      this.index++;
      if (end === ")") {
        // `(?flags)` applies to the rest of the enclosing group.
        if (setsCaseInsensitive !== undefined) {
          if (setsCaseInsensitive && atStart) {
            this.caseInsensitive = true;
          } else {
            this.unsupported.push("`(?i)` anywhere but at the start of the pattern");
          }
        }
        return { kind: "flags" };
      }
      if (setsCaseInsensitive !== undefined) {
        this.unsupported.push("`(?i)` anywhere but at the start of the pattern");
      }
      const body = this.parseGroupBody(depth);
      this.flags = saved;
      return { kind: "group", capture: false, body };
    }
    this.captures++;
    return { kind: "group", capture: true, body: this.parseGroupBody(depth) };
  }

  private parseGroupBody(depth: number): Node {
    const saved = this.flags;
    const body = this.parseAlternation(depth + 1);
    if (this.eof()) this.fail("unclosed group");
    this.index++;
    this.flags = saved;
    return body;
  }

  private parseCaptureName(): void {
    if (this.eof()) this.fail("unexpected end of group name");
    let name = "";
    while (!this.eof() && this.char() !== ">") {
      const char = this.char();
      const valid =
        name === ""
          ? char === "_" || /\p{Alphabetic}/u.test(char)
          : char === "_" ||
            char === "." ||
            char === "[" ||
            char === "]" ||
            /[\p{Alphabetic}\p{N}]/u.test(char);
      if (!valid) this.fail("invalid group name");
      name += char;
      this.index++;
    }
    if (this.eof()) this.fail("unexpected end of group name");
    this.index++;
    if (name === "") this.fail("empty group name");
    if (this.names.has(name)) this.fail(`duplicate group name "${name}"`);
    this.names.add(name);
  }

  /**
   * Parse `imsUuRx` flags up to `:` or `)`, applying them to the current
   * flags. Returns whether they set or clear case-insensitivity, if they touch it.
   */
  private parseFlags(): boolean | undefined {
    const seen = new Set<string>();
    let negated = false;
    let lastWasNegation = false;
    let caseInsensitive: boolean | undefined;
    const flags = { ...this.flags };
    while (this.char() !== ":" && this.char() !== ")") {
      const char = this.char();
      if (char === "-") {
        if (negated) this.fail("repeated negation in flags");
        negated = true;
        lastWasNegation = true;
      } else {
        lastWasNegation = false;
        if (!FLAG_CHARACTERS.has(char)) this.fail(`unrecognized flag "${char}"`);
        if (seen.has(char)) this.fail(`duplicate flag "${char}"`);
        seen.add(char);
        const on = !negated;
        if (char === "i") caseInsensitive = on;
        else if (char === "m") flags.multiLine = on;
        else if (char === "s") flags.dotAll = on;
        else if (char === "U") flags.swapGreed = on;
        else if (char === "x") this.unsupported.push("the `x` flag");
        else if (char === "R") this.unsupported.push("the `R` flag");
        else if (char === "u" && !on) this.unsupported.push("turning off the `u` flag");
      }
      this.index++;
      if (this.eof()) this.fail("unexpected end of flags");
    }
    if (lastWasNegation) this.fail("dangling flag negation");
    if (seen.size === 0 && this.char() === ")") this.fail("repetition operator missing expression");
    this.flags = flags;
    return caseInsensitive;
  }

  private parsePrimitive(): Node {
    const char = this.char();
    if (char === "\\") return this.parseEscape(false);
    this.index++;
    if (char === ".") return { kind: "dot", dotAll: this.flags.dotAll };
    if (char === "^") return this.assertion("start");
    if (char === "$") return this.assertion("end");
    return { kind: "literal", char };
  }

  private assertion(assertion: Assertion): Node {
    return { kind: "assertion", assertion, multiLine: this.flags.multiLine };
  }

  /** `\` escapes, inside a class or outside one. */
  private parseEscape(inClass: boolean): Node {
    this.index++;
    if (this.eof()) this.fail("incomplete escape");
    const char = this.char();
    if (/[0-9]/.test(char)) {
      // `regex-syntax` rejects these as backreferences, sending the native
      // runtime down its untranslated path.
      this.fancy = true;
      let digits = "";
      while (!this.eof() && /[0-9]/.test(this.char())) digits += this.chars[this.index++];
      if (inClass || digits.startsWith("0")) this.unsupported.push(`\`\\${digits}\``);
      return { kind: "backref", index: Number(digits) };
    }
    if (char === "x" || char === "u" || char === "U") {
      return { kind: "literal", char: this.parseHex(char) };
    }
    if (char === "p" || char === "P") {
      return { kind: "class", item: this.parseUnicodeClass(char === "P") };
    }
    if ("dswDSW".includes(char)) {
      this.index++;
      const perl = char.toLowerCase() as PerlKind;
      return { kind: "class", item: { kind: "perl", perl, negated: char !== perl } };
    }
    this.index++;
    if (isEscapeable(char)) return { kind: "literal", char };
    const special: Record<string, string> = { f: "\f", t: "\t", n: "\n", r: "\r", v: "\v" };
    if (special[char] !== undefined) return { kind: "literal", char: special[char] };
    if (char === "a") {
      // The native translator rejects `\a` outside character classes.
      if (!inClass) this.bellOutsideClass = true;
      return { kind: "literal", char: "\x07" };
    }
    if (char === "c") {
      // Rewritten to the control character before parsing, as natively.
      const letter = this.eof() ? undefined : this.char();
      if (letter === undefined || !/[A-Za-z]/.test(letter)) this.fail("invalid `\\c` escape");
      this.index++;
      return { kind: "literal", char: String.fromCharCode(letter.charCodeAt(0) % 32) };
    }
    if (inClass && "AzbB<>".includes(char)) this.fail(`\`\\${char}\` is not allowed in a class`);
    if (char === "A") return this.assertion("startText");
    if (char === "z") return this.assertion("endText");
    if (char === "B") return this.assertion("notWordBoundary");
    if (char === "<") return this.assertion("wordStart");
    if (char === ">") return this.assertion("wordEnd");
    if (char === "b") return this.assertion(this.parseSpecialWordBoundary());
    return this.fail(`unrecognized escape \`\\${char}\``);
  }

  private parseSpecialWordBoundary(): Assertion {
    if (this.eof() || this.char() !== "{") return "wordBoundary";
    const start = this.index;
    this.index++;
    if (this.eof()) this.fail("unexpected end of special word boundary");
    if (!/[A-Za-z-]/.test(this.char())) {
      // Not a special word boundary: `{` starts a counted repetition.
      this.index = start;
      return "wordBoundary";
    }
    let name = "";
    while (!this.eof() && /[A-Za-z-]/.test(this.char())) name += this.chars[this.index++];
    if (this.eof() || this.char() !== "}") this.fail("unclosed special word boundary");
    this.index++;
    const kinds: Record<string, Assertion> = {
      start: "wordStart",
      end: "wordEnd",
      "start-half": "wordStartHalf",
      "end-half": "wordEndHalf",
    };
    return kinds[name] ?? this.fail(`unrecognized special word boundary "${name}"`);
  }

  private parseHex(kind: string): string {
    this.index++;
    if (this.eof()) this.fail("incomplete hex escape");
    let digits = "";
    if (this.char() === "{") {
      this.index++;
      while (!this.eof() && this.char() !== "}") {
        if (!/[0-9A-Fa-f]/.test(this.char())) this.fail("invalid hex digit");
        digits += this.chars[this.index++];
      }
      if (this.eof()) this.fail("incomplete hex escape");
      this.index++;
      if (digits === "") this.fail("empty hex escape");
    } else {
      const length = kind === "x" ? 2 : kind === "u" ? 4 : 8;
      for (let i = 0; i < length; i++) {
        if (this.eof()) this.fail("incomplete hex escape");
        if (!/[0-9A-Fa-f]/.test(this.char())) this.fail("invalid hex digit");
        digits += this.chars[this.index++];
      }
    }
    const code = parseInt(digits, 16);
    if (digits.length > 8 || code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff)) {
      this.fail("invalid Unicode scalar value");
    }
    return String.fromCodePoint(code);
  }

  private parseUnicodeClass(negated: boolean): ClassItem {
    this.index++;
    if (this.eof()) this.fail("incomplete Unicode class");
    if (this.char() === "{") {
      this.index++;
      let body = "";
      while (!this.eof() && this.char() !== "}") body += this.chars[this.index++];
      if (this.eof()) this.fail("incomplete Unicode class");
      this.index++;
      const notEqual = body.indexOf("!=");
      if (notEqual >= 0) {
        return {
          kind: "unicode",
          name: `${body.slice(0, notEqual)}=${body.slice(notEqual + 2)}`,
          negated: !negated,
        };
      }
      return { kind: "unicode", name: body.replace(":", "="), negated };
    }
    const letter = this.char();
    if (letter === "\\") this.fail("invalid Unicode class");
    this.index++;
    return { kind: "unicode", name: letter, negated };
  }

  private parseBracketed(): ClassItem {
    this.index++;
    if (this.eof()) this.fail("unclosed class");
    let negated = false;
    if (this.char() === "^") {
      negated = true;
      this.index++;
      if (this.eof()) this.fail("unclosed class");
    }
    let union: ClassItem[] = [];
    while (this.char() === "-") {
      union.push({ kind: "literal", char: "-" });
      this.index++;
      if (this.eof()) this.fail("unclosed class");
    }
    if (union.length === 0 && this.char() === "]") {
      union.push({ kind: "literal", char: "]" });
      this.index++;
      if (this.eof()) this.fail("unclosed class");
    }
    let lhs: ClassSet | undefined;
    let op: "&&" | "--" | "~~" | undefined;
    const combine = (): ClassSet => {
      const rhs: ClassSet = { kind: "union", items: union };
      return lhs === undefined ? rhs : { kind: "op", op: op!, lhs, rhs };
    };
    for (;;) {
      if (this.eof()) this.fail("unclosed class");
      const char = this.char();
      if (char === "[") {
        const ascii = this.maybeParseAsciiClass();
        union.push(ascii ?? this.parseBracketed());
      } else if (char === "]") {
        this.index++;
        return { kind: "bracket", negated, set: combine() };
      } else if ((char === "&" || char === "-" || char === "~") && this.peek() === char) {
        lhs = combine();
        op = `${char}${char}` as "&&" | "--" | "~~";
        union = [];
        this.index += 2;
      } else {
        union.push(this.parseClassRange());
      }
    }
  }

  private maybeParseAsciiClass(): ClassItem | undefined {
    const match = /^\[:(\^?)([^:]*):\]/.exec(this.chars.slice(this.index).join(""));
    if (!match || !ASCII_KINDS.has(match[2]!)) return undefined;
    this.index += [...match[0]].length;
    return { kind: "ascii", ascii: match[2] as AsciiKind, negated: match[1] === "^" };
  }

  private parseClassItem(): Node {
    if (this.char() === "\\") return this.parseEscape(true);
    return { kind: "literal", char: this.chars[this.index++]! };
  }

  private parseClassRange(): ClassItem {
    const first = this.parseClassItem();
    if (this.eof()) this.fail("unclosed class");
    if (this.char() !== "-" || this.peek() === "]" || this.peek() === "-") {
      return this.toClassItem(first);
    }
    this.index++;
    if (this.eof()) this.fail("unclosed class");
    const second = this.parseClassItem();
    if (first.kind !== "literal" || second.kind !== "literal") {
      this.fail("class ranges need literal endpoints");
    }
    if (first.char.codePointAt(0)! > second.char.codePointAt(0)!) this.fail("invalid class range");
    return { kind: "range", start: first.char, end: second.char };
  }

  private toClassItem(node: Node): ClassItem {
    if (node.kind === "literal") return { kind: "literal", char: node.char };
    if (node.kind === "class") return node.item;
    // A backreference-like escape; already recorded as unsupported.
    return { kind: "literal", char: "\0" };
  }
}

/** Code point ranges of the ASCII classes, and of `\d`, `\w`, `\s` as the native runtime translates them. */
const RANGES: Record<AsciiKind | PerlKind, [number, number][]> = {
  alnum: [
    [0x30, 0x39],
    [0x41, 0x5a],
    [0x61, 0x7a],
  ],
  alpha: [
    [0x41, 0x5a],
    [0x61, 0x7a],
  ],
  ascii: [[0x00, 0x7f]],
  blank: [
    [0x09, 0x09],
    [0x20, 0x20],
  ],
  cntrl: [
    [0x00, 0x1f],
    [0x7f, 0x7f],
  ],
  digit: [[0x30, 0x39]],
  graph: [[0x21, 0x7e]],
  lower: [[0x61, 0x7a]],
  print: [[0x20, 0x7e]],
  punct: [
    [0x21, 0x2f],
    [0x3a, 0x40],
    [0x5b, 0x60],
    [0x7b, 0x7e],
  ],
  space: [
    [0x09, 0x0d],
    [0x20, 0x20],
  ],
  upper: [[0x41, 0x5a]],
  word: [
    [0x30, 0x39],
    [0x41, 0x5a],
    [0x5f, 0x5f],
    [0x61, 0x7a],
  ],
  xdigit: [
    [0x30, 0x39],
    [0x41, 0x46],
    [0x61, 0x66],
  ],
  d: [[0x30, 0x39]],
  w: [
    [0x30, 0x39],
    [0x41, 0x5a],
    [0x5f, 0x5f],
    [0x61, 0x7a],
  ],
  // `jsonschema`'s ECMA translation of `\s` (sic).
  s: [
    [0x09, 0x0d],
    [0x20, 0x20],
    [0xa0, 0xa0],
    [0x2003, 0x2003],
    [0x2029, 0x2029],
    [0xfeff, 0xfeff],
  ],
};

/** Unicode `\w`, which `\b` and untranslated patterns use natively. */
const UNICODE_WORD = "\\p{Alphabetic}\\p{M}\\p{Nd}\\p{Pc}\\p{Join_Control}";
const UNICODE_PERL: Record<PerlKind, string> = {
  d: "\\p{Nd}",
  s: "\\p{White_Space}",
  w: UNICODE_WORD,
};

function escapeChar(char: string): string {
  return /^[A-Za-z0-9]$/.test(char) ? char : `\\u{${char.codePointAt(0)!.toString(16)}}`;
}

function rangesFragment(ranges: [number, number][]): string {
  return ranges
    .map(([start, end]) => {
      const first = escapeChar(String.fromCodePoint(start));
      return start === end ? first : `${first}-${escapeChar(String.fromCodePoint(end))}`;
    })
    .join("");
}

function complement(ranges: [number, number][]): [number, number][] {
  const result: [number, number][] = [];
  let next = 0;
  for (const [start, end] of ranges) {
    if (start > next) result.push([next, start - 1]);
    next = end + 1;
  }
  if (next <= 0x10ffff) result.push([next, 0x10ffff]);
  return result;
}

/** A JS class body (`frag`) when one exists, and an expression matching one code point. */
interface Emitted {
  fragment?: string;
  atom: string;
}

class Emitter {
  constructor(private readonly parsed: Parsed) {}

  emit(node: Node): string {
    switch (node.kind) {
      case "flags":
        return "";
      case "literal":
        return escapeChar(node.char);
      case "dot":
        return node.dotAll ? "[^]" : "[^\\n]";
      case "assertion":
        return this.assertion(node.assertion, node.multiLine);
      case "class":
        return this.item(node.item).atom;
      case "group":
        return `(${node.capture && this.parsed.fancy ? "" : "?:"}${this.emit(node.body)})`;
      case "look":
        return `(?${node.op}${this.emit(node.body)})`;
      case "backref":
        if (node.index === 0 || node.index > this.parsed.captures) {
          throw new UnsupportedRegexError(`backreference \`\\${node.index}\``);
        }
        return `\\${node.index}(?:)`;
      case "repeat": {
        const { min, max } = node;
        const quantifier =
          max === Infinity
            ? min === 0
              ? "*"
              : min === 1
                ? "+"
                : `{${min},}`
            : min === 0 && max === 1
              ? "?"
              : min === max
                ? `{${min}}`
                : `{${min},${max}}`;
        return `(?:${this.emit(node.body)})${quantifier}${node.greedy ? "" : "?"}`;
      }
      case "concat":
        return node.items.map((item) => this.emit(item)).join("");
      case "alternation":
        return node.items.map((item) => this.emit(item)).join("|");
    }
  }

  private assertion(assertion: Assertion, multiLine: boolean): string {
    const word = `[${UNICODE_WORD}]`;
    switch (assertion) {
      case "start":
        return multiLine ? "(?<![^\\n])" : "^";
      case "end":
        return multiLine ? "(?![^\\n])" : "$";
      case "startText":
        return "^";
      case "endText":
        return "$";
      case "wordBoundary":
        return `(?:(?<=${word})(?!${word})|(?<!${word})(?=${word}))`;
      case "notWordBoundary":
        return `(?:(?<=${word})(?=${word})|(?<!${word})(?!${word}))`;
      case "wordStart":
        return `(?<!${word})(?=${word})`;
      case "wordEnd":
        return `(?<=${word})(?!${word})`;
      case "wordStartHalf":
        return `(?<!${word})`;
      case "wordEndHalf":
        return `(?!${word})`;
    }
  }

  private item(item: ClassItem): Emitted {
    switch (item.kind) {
      case "literal": {
        const fragment = escapeChar(item.char);
        return { fragment, atom: `[${fragment}]` };
      }
      case "range": {
        const fragment = `${escapeChar(item.start)}-${escapeChar(item.end)}`;
        return { fragment, atom: `[${fragment}]` };
      }
      case "perl":
        if (this.parsed.fancy) {
          const fragment = UNICODE_PERL[item.perl];
          return item.negated
            ? item.perl === "w"
              ? { atom: `[^${fragment}]` }
              : {
                  fragment: fragment.replace("\\p", "\\P"),
                  atom: `[${fragment.replace("\\p", "\\P")}]`,
                }
            : { fragment, atom: `[${fragment}]` };
        }
        return this.ranges(RANGES[item.perl], item.negated);
      case "ascii":
        return this.ranges(RANGES[item.ascii], item.negated);
      case "unicode": {
        const fragment = unicodeProperty(item.name, item.negated);
        return { fragment, atom: `[${fragment}]` };
      }
      case "bracket": {
        const inner = this.set(item.set);
        if (!item.negated) return inner;
        return inner.fragment !== undefined
          ? { atom: `[^${inner.fragment}]` }
          : { atom: `(?:(?!${inner.atom})[^])` };
      }
    }
  }

  private ranges(ranges: [number, number][], negated: boolean): Emitted {
    const fragment = rangesFragment(negated ? complement(ranges) : ranges);
    return { fragment, atom: `[${fragment}]` };
  }

  private set(set: ClassSet): Emitted {
    if (set.kind === "union") {
      const items = set.items.map((item) => this.item(item));
      if (items.every((item) => item.fragment !== undefined)) {
        const fragment = items.map((item) => item.fragment).join("");
        return { fragment, atom: `[${fragment}]` };
      }
      return { atom: `(?:${items.map((item) => item.atom).join("|")})` };
    }
    const lhs = this.set(set.lhs).atom;
    const rhs = this.set(set.rhs).atom;
    switch (set.op) {
      case "&&":
        return { atom: `(?:(?=${lhs})${rhs})` };
      case "--":
        return { atom: `(?:(?!${rhs})${lhs})` };
      case "~~":
        return { atom: `(?:(?!${rhs})${lhs}|(?!${lhs})${rhs})` };
    }
  }
}

/** A JS `\p{…}`/`\P{…}` for a native Unicode class name, if JS knows the name. */
function unicodeProperty(name: string, negated: boolean): string {
  const escape = negated ? "\\P" : "\\p";
  for (const candidate of [name, `Script=${name}`]) {
    try {
      new RegExp(`${escape}{${candidate}}`, "u");
      return `${escape}{${candidate}}`;
    } catch {
      // Try the next spelling.
    }
  }
  throw new UnsupportedRegexError(`the Unicode class \`${name}\``);
}

/** Whether the native runtime accepts `pattern` as a regular expression (the `regex` format). */
export function isValidRegexSyntax(pattern: string): boolean {
  try {
    new Parser(pattern).parse();
    return true;
  } catch (error) {
    if (error instanceof UnsupportedRegexError) return true;
    return false;
  }
}

const compiled = new Map<string, RegExp>();
const MAX_COMPILED = 1024;

/**
 * Compile `pattern` with the native runtime's meaning. Throws
 * `RegexSyntaxError` for patterns the native runtime rejects and
 * `UnsupportedRegexError` for ones the browser cannot evaluate.
 */
export function compileRegex(pattern: string): RegExp {
  let regex = compiled.get(pattern);
  if (regex) return regex;
  const parsed = new Parser(pattern).parse();
  if (parsed.fancyInvalid.length > 0) throw new RegexSyntaxError(parsed.fancyInvalid[0]!);
  if (parsed.unsupported.length > 0) {
    throw new UnsupportedRegexError(parsed.unsupported[0]!);
  }
  const source = new Emitter(parsed).emit(parsed.node);
  regex = new RegExp(source, parsed.caseInsensitive ? "iu" : "u");
  if (compiled.size >= MAX_COMPILED) compiled.clear();
  compiled.set(pattern, regex);
  return regex;
}
