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
    ...structuralContext(tokens, reference.index),
  };
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

const itemKeywords = new Set([
  "mod", "fn", "impl", "trait", "struct", "enum", "union", "const", "static", "type", "use",
  "extern", "macro", "macro_rules",
]);

/**
 * Return the complete declaration context for a serializer root.  This is
 * intentionally a small structural Rust parser rather than a brace-only
 * heuristic: `type`, `const`, `static`, `use`, tuple/unit structs, and
 * associated items end at `;`, yet still own both an identity and attributes.
 * A registry allowance consequently cannot cross a cfg(test) declaration
 * boundary while preserving its path line/column.
 */
function structuralContext(tokens, target) {
  const items = itemSpans(tokens);
  const parents = items
    .filter((item) => item.start <= target && target <= item.end)
    .sort((left, right) => left.start - right.start || right.end - left.end);
  const modules = parents.filter((item) => item.kind === "mod").map((item) => item.name);
  const enclosingItems = parents
    .filter((item) => item.kind !== "mod")
    .map((item) => item.kind + " " + item.name);
  const field = fieldContext(tokens, target, parents);
  if (field) enclosingItems.push(field.identity);
  const test = parents.some((item) => item.cfgTest) || field?.cfgTest;
  return {
    enclosing: {
      modules,
      items: enclosingItems,
      item: enclosingItems.at(-1) ?? "<module>",
    },
    boundary: test ? "test" : "production",
  };
}

function itemSpans(tokens) {
  const spans = [];
  for (let index = 0; index < tokens.length; index += 1) {
    if (!itemKeywords.has(tokens[index].text)) continue;
    const end = declarationEnd(tokens, index);
    if (end === undefined) continue;
    const attributes = itemAttributes(tokens, index);
    spans.push({
      start: index,
      end,
      kind: tokens[index].text,
      name: itemName(tokens, index, end),
      cfgTest: attributes.some((attribute) => /cfg\s*\(\s*test\s*\)/.test(attribute.text)),
      openBrace: declarationOpenBrace(tokens, index, end),
    });
  }
  return spans;
}

function declarationOpenBrace(tokens, start, end) {
  const stack = [];
  for (let cursor = start + 1; cursor <= end; cursor += 1) {
    const text = tokens[cursor].text;
    if (["(", "["].includes(text)) stack.push({ "(": ")", "[": "]" }[text]);
    else if (stack.length && text === stack.at(-1)) stack.pop();
    else if (!stack.length && text === "{") return cursor;
  }
  return undefined;
}

function declarationEnd(tokens, start) {
  // Item headers have nested parameter/type delimiters.  A top-level `{` or
  // `;` closes the header; a braced item extends through its matching `}`.
  const stack = [];
  for (let cursor = start + 1; cursor < tokens.length; cursor += 1) {
    const text = tokens[cursor].text;
    if (["(", "["].includes(text)) stack.push({ "(": ")", "[": "]" }[text]);
    else if (stack.length && text === stack.at(-1)) stack.pop();
    else if (!stack.length && text === ";") return cursor;
    else if (!stack.length && text === "{") {
      let depth = 1;
      for (let close = cursor + 1; close < tokens.length; close += 1) {
        if (tokens[close].text === "{") depth += 1;
        if (tokens[close].text === "}" && --depth === 0) return close;
      }
      return undefined;
    }
  }
  return undefined;
}

function itemAttributes(tokens, itemStart) {
  // Attributes may be separated from an item keyword by visibility, `unsafe`,
  // or ABI tokens.  A declaration/field separator is a hard ownership fence.
  const attributes = [];
  for (let cursor = itemStart - 1; cursor >= 1; cursor -= 1) {
    if ([";", "{", "}", ","].includes(tokens[cursor].text)) break;
    if (tokens[cursor].text !== "]") continue;
    let depth = 0;
    for (let open = cursor; open >= 1; open -= 1) {
      if (tokens[open].text === "]") depth += 1;
      if (tokens[open].text === "[" && --depth === 0 && tokens[open - 1]?.text === "#") {
        attributes.push({ start: open - 1, end: cursor, text: attributeText(tokens, open - 1) });
        cursor = open - 1;
        break;
      }
    }
  }
  return attributes;
}

function itemName(tokens, start, end) {
  const kind = tokens[start].text;
  if (kind === "impl") {
    const parts = [];
    for (let cursor = start + 1; cursor <= end; cursor += 1) {
      if (["{", ";"].includes(tokens[cursor]?.text)) break;
      parts.push(tokens[cursor].text);
    }
    return parts.join(" ") || "<anonymous>";
  }
  if (kind === "macro_rules") {
    const bang = tokens.findIndex((token, index) => index > start && token.text === "!");
    return tokens[bang + 1]?.kind === "ident" ? tokens[bang + 1].text : "<anonymous>";
  }
  // `extern crate foo`, `extern "C" { ... }`, and ordinary declarations.
  for (let cursor = start + 1; cursor <= end; cursor += 1) {
    if (tokens[cursor]?.kind !== "ident") continue;
    if (kind === "extern" && tokens[cursor].text === "crate") continue;
    return tokens[cursor].text;
  }
  return "<anonymous>";
}

function fieldContext(tokens, target, parents) {
  const owner = [...parents]
    .reverse()
    .find((item) => ["struct", "union", "enum"].includes(item.kind) && item.openBrace !== undefined);
  if (!owner || target <= owner.openBrace || target >= owner.end) return undefined;
  let start = owner.openBrace + 1;
  let depth = 0;
  for (let cursor = owner.openBrace + 1; cursor < target; cursor += 1) {
    const text = tokens[cursor].text;
    if (["(", "[", "{"].includes(text)) depth += 1;
    else if ([")", "]", "}"].includes(text)) depth -= 1;
    else if (text === "," && depth === 0) start = cursor + 1;
  }
  const segment = tokens.slice(start, target);
  // A `name: Type` field gets a stable field identity. Tuple fields receive a
  // positional identity; both retain a local cfg boundary when present.
  const colon = segment.findIndex((token) => token.text === ":");
  const name =
    colon === -1
      ? "#" + countTupleFields(tokens, owner.openBrace + 1, start)
      : [...segment.slice(0, colon)].reverse().find((token) => token.kind === "ident")?.text ?? "<anonymous>";
  const cfgTest = segment.some(
    (token, index) => token.text === "#" && /cfg\s*\(\s*test\s*\)/.test(attributeText(segment, index)),
  );
  return { identity: "field " + name, cfgTest };
}

function countTupleFields(tokens, bodyStart, segmentStart) {
  let count = 0;
  let depth = 0;
  for (let cursor = bodyStart; cursor < segmentStart; cursor += 1) {
    const text = tokens[cursor].text;
    if (["(", "[", "{"].includes(text)) depth += 1;
    else if ([")", "]", "}"].includes(text)) depth -= 1;
    else if (text === "," && depth === 0) count += 1;
  }
  return count;
}
