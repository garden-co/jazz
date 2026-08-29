// Deliberately small Rust lexer for policy gates. It skips comments and all
// string literal forms, canonicalizes raw identifiers, and retains locations.
export function rustTokens(source) {
  const tokens = [];
  let index = 0;
  let line = 1;
  let column = 1;
  const advance = (text) => {
    for (const character of text) {
      if (character === "\n") {
        line += 1;
        column = 1;
      } else {
        column += 1;
      }
    }
    index += text.length;
  };
  const add = (kind, value, start, startLine, startColumn) => {
    tokens.push({
      kind,
      text: kind === "ident" ? value.replace(/^r#/, "") : value,
      start,
      end: index,
      line: startLine,
      column: startColumn,
      endLine: line,
      endColumn: column,
    });
  };
  while (index < source.length) {
    if (/\s/.test(source[index])) {
      advance(source[index]);
      continue;
    }
    if (source.startsWith("//", index)) {
      const end = source.indexOf("\n", index);
      advance(source.slice(index, end === -1 ? source.length : end));
      continue;
    }
    if (source.startsWith("/*", index)) {
      let depth = 0;
      do {
        if (source.startsWith("/*", index)) {
          depth += 1;
          advance("/*");
        } else if (source.startsWith("*/", index)) {
          depth -= 1;
          advance("*/");
        } else {
          advance(source[index]);
        }
      } while (index < source.length && depth);
      continue;
    }
    const raw = source.slice(index).match(/^(?:br|rb|r)(#{0,255})"/);
    if (raw) {
      const end = source.indexOf('"' + raw[1], index + raw[0].length);
      advance(source.slice(index, end === -1 ? source.length : end + raw[1].length + 1));
      continue;
    }
    if (
      source[index] === '"' ||
      ((source[index] === "b" || source[index] === "c") && source[index + 1] === '"')
    ) {
      advance(source[index]);
      if (source[index] === '"') advance(source[index]);
      while (index < source.length) {
        const character = source[index];
        advance(character);
        if (character === "\\" && index < source.length) advance(source[index]);
        else if (character === '"') break;
      }
      continue;
    }
    if (source[index] === "'") {
      const character = source.slice(index).match(/^'(?:\\.|[^'\\\n])'/);
      if (character) {
        advance(character[0]);
        continue;
      }
    }
    const start = index;
    const startLine = line;
    const startColumn = column;
    const ident = source.slice(index).match(/^(?:r#)?[A-Za-z_][A-Za-z0-9_]*/);
    if (ident) {
      advance(ident[0]);
      add("ident", ident[0], start, startLine, startColumn);
      continue;
    }
    const punctuation =
      source.startsWith("::", index) ||
      source.startsWith("=>", index) ||
      source.startsWith("..", index)
        ? source.slice(index, index + 2)
        : source[index];
    advance(punctuation);
    add("punct", punctuation, start, startLine, startColumn);
  }
  return tokens;
}

export function serializerReferences(tokens, roots) {
  const references = [];
  for (let index = 0; index < tokens.length; index += 1) {
    if (tokens[index].kind !== "ident" || !roots.has(tokens[index].text)) continue;
    // A local named postcard is not a crate path. A crate root is followed by
    // a path separator or macro delimiter.
    if (tokens[index + 1]?.text !== "::" && tokens[index + 1]?.text !== "!") continue;
    const parts = [tokens[index].text];
    let cursor = index + 1;
    while (tokens[cursor]?.text === "::" && tokens[cursor + 1]?.kind === "ident") {
      parts.push(tokens[cursor + 1].text);
      cursor += 2;
    }
    references.push({ index, endIndex: cursor - 1, canonicalPath: parts.join("::") });
    continue;
  }
  // The branch above continues after every root candidate. This second pass
  // intentionally tracks macro-token nesting independently, including roots
  // in macro arguments that are not syntactically paths until expansion. Rust
  // macro token trees nest every delimiter kind; tracking only the delimiter
  // immediately after `!` used to let `outer!({ inner!(postcard) })` escape.
  const bareMacroReferences = [];
  const stack = [];
  for (let index = 0; index < tokens.length; index += 1) {
    const text = tokens[index].text;
    if (["(", "[", "{"].includes(text)) {
      if (stack.length || tokens[index - 1]?.text === "!")
        stack.push({ close: { "(": ")", "[": "]", "{": "}" }[text] });
      continue;
    }
    if (stack.length && text === stack.at(-1).close) {
      stack.pop();
      continue;
    }
    if (
      stack.length &&
      tokens[index].kind === "ident" &&
      roots.has(tokens[index].text) &&
      tokens[index + 1]?.text !== "::" &&
      tokens[index + 1]?.text !== "!"
    ) {
      bareMacroReferences.push({ index, endIndex: index, canonicalPath: tokens[index].text });
    }
  }
  return [...references, ...bareMacroReferences].sort((left, right) => left.index - right.index);
}

export function serializerImports(tokens, roots) {
  const imports = [];
  for (let index = 0; index < tokens.length; index += 1) {
    if (tokens[index].text !== "use") continue;
    for (
      let cursor = index + 1;
      cursor < tokens.length && tokens[cursor].text !== ";";
      cursor += 1
    ) {
      if (tokens[cursor].kind === "ident" && roots.has(tokens[cursor].text)) {
        imports.push({ root: tokens[cursor].text, line: tokens[cursor].line });
      }
    }
  }
  return imports;
}

export function serializerExternCrates(tokens, roots) {
  const crates = [];
  for (let index = 0; index + 2 < tokens.length; index += 1) {
    if (
      tokens[index].text === "extern" &&
      tokens[index + 1].text === "crate" &&
      roots.has(tokens[index + 2].text)
    ) {
      const alias =
        tokens[index + 3]?.text === "as" && tokens[index + 4]?.kind === "ident"
          ? tokens[index + 4].text
          : tokens[index + 2].text;
      crates.push({ root: tokens[index + 2].text, alias, line: tokens[index + 2].line });
    }
  }
  return crates;
}

export function describeEndpoint(tokens, reference, relative) {
  const token = tokens[reference.index];
  const end = tokens[reference.endIndex];
  return {
    path: relative,
    canonicalPath: reference.canonicalPath,
    location: { line: token.line, column: token.column },
    span: {
      start: { line: token.line, column: token.column },
      end: { line: end.endLine, column: end.endColumn },
    },
    enclosing: enclosing(tokens, reference.index),
    boundary: boundary(tokens, reference.index),
  };
}

function enclosing(tokens, target) {
  const modules = [];
  const braces = [];
  let pending = null;
  let closurePipes = 0;
  const itemKeywords = new Set([
    "mod", "fn", "impl", "trait", "struct", "enum", "union", "const", "static", "type", "use",
    "extern", "macro", "macro_rules",
  ]);
  for (let index = 0; index < target; index += 1) {
    const token = tokens[index];
    if (itemKeywords.has(token.text)) {
      let name = tokens[index + 1]?.kind === "ident" ? tokens[index + 1].text : "<anonymous>";
      if (token.text === "impl") {
        // The implementation target (and optional trait) is part of the
        // endpoint identity: moving a serializer call between impls must not
        // preserve its allowance.
        const parts = [];
        for (let cursor = index + 1; cursor < target; cursor += 1) {
          if (["{", ";"].includes(tokens[cursor].text)) break;
          parts.push(tokens[cursor].text);
        }
        name = parts.join(" ") || "<anonymous>";
      }
      pending = { kind: token.text, name };
    }
    if (token.text === "|") closurePipes += 1;
    if (token.text === "{") {
      if (!pending && closurePipes % 2 === 0 && index > 0 && tokens[index - 1]?.text === "|")
        pending = { kind: "closure", name: "<closure>" };
      braces.push(pending);
      if (pending?.kind === "mod") modules.push(pending.name);
      pending = null;
    }
    if (token.text === "}") {
      const closed = braces.pop();
      if (closed?.kind === "mod") modules.pop();
    }
    if (pending && token.text === ";") pending = null;
  }
  const items = braces
    .filter((entry) => entry && entry.kind !== "mod")
    .map((entry) => entry.kind + " " + entry.name);
  return { modules, items, item: items.at(-1) ?? "<module>" };
}

function attributeText(tokens, start) {
  let text = "";
  let depth = 0;
  for (let index = start; index < tokens.length; index += 1) {
    text += tokens[index].text;
    if (tokens[index].text === "[") depth += 1;
    if (tokens[index].text === "]" && --depth === 0) break;
  }
  return text;
}

function boundary(tokens, target) {
  const braces = [];
  let pendingAttributes = [];
  for (let index = 0; index < target; index += 1) {
    if (tokens[index].text === "#" && tokens[index + 1]?.text === "[") {
      pendingAttributes.push(attributeText(tokens, index));
      continue;
    }
    // An attribute belongs to the next Rust item, including semicolon items
    // (`use`, `const`, `static`, `type`, macro declarations). Do not leave it
    // pending until a later block: that accidentally classified production
    // code after `#[cfg(test)] use ...;` as test-only.
    if (
      [
        "mod", "fn", "impl", "trait", "struct", "enum", "union", "const", "static", "type", "use",
        "extern", "macro", "macro_rules",
      ].includes(tokens[index].text)
    ) {
      const attributes = pendingAttributes;
      pendingAttributes = [];
      let cursor = index + 1;
      while (cursor < tokens.length && tokens[cursor].text !== "{" && tokens[cursor].text !== ";")
        cursor += 1;
      if (tokens[cursor]?.text === "{")
        braces.push(attributes.some((attribute) => /cfg\(test\)/.test(attribute)));
      // A semicolon item consumed the attributes even though it introduces no
      // lexical scope. This explicit branch is deliberately boring: it keeps
      // the state machine correct for all item kinds rather than relying on
      // the next brace to reset it.
      continue;
    }
    if (tokens[index].text === "}") braces.pop();
  }
  return braces.some(Boolean) ? "test" : "production";
}
