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
  // in macro arguments that are not syntactically paths until expansion.
  const bareMacroReferences = [];
  const stack = [];
  for (let index = 0; index < tokens.length; index += 1) {
    if (["(", "[", "{"].includes(tokens[index].text) && tokens[index - 1]?.text === "!") {
      stack.push({ close: { "(": ")", "[": "]", "{": "}" }[tokens[index].text] });
      continue;
    }
    if (stack.length && tokens[index].text === stack.at(-1).close) {
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
      crates.push({ root: tokens[index + 2].text, line: tokens[index + 2].line });
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
  let item = "<module>";
  for (let index = 0; index < target; index += 1) {
    if (
      ["mod", "fn", "impl", "trait", "struct", "enum", "const", "static", "type"].includes(
        tokens[index].text,
      )
    ) {
      pending = {
        kind: tokens[index].text,
        name: tokens[index + 1]?.kind === "ident" ? tokens[index + 1].text : "<anonymous>",
      };
    }
    if (tokens[index].text === "{") {
      braces.push(pending);
      if (pending?.kind === "mod") modules.push(pending.name);
      pending = null;
    }
    if (tokens[index].text === "}") {
      const closed = braces.pop();
      if (closed?.kind === "mod") modules.pop();
    }
    if (pending && tokens[index].text === ";") pending = null;
    const current = braces.at(-1);
    if (current && current.kind !== "mod") item = current.kind + " " + current.name;
  }
  return { modules, item };
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
    if (["mod", "fn", "impl", "trait", "struct", "enum"].includes(tokens[index].text)) {
      const attributes = pendingAttributes;
      pendingAttributes = [];
      let cursor = index + 1;
      while (cursor < tokens.length && tokens[cursor].text !== "{" && tokens[cursor].text !== ";")
        cursor += 1;
      if (tokens[cursor]?.text === "{")
        braces.push(attributes.some((attribute) => /cfg\(test\)/.test(attribute)));
      continue;
    }
    if (tokens[index].text === "}") braces.pop();
  }
  return braces.some(Boolean) ? "test" : "production";
}
