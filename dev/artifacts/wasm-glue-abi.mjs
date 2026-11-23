/**
 * wasm-bindgen emits the JavaScript import object and its companion `.wasm`
 * together. A worker bundle may copy the binary separately, so verify both
 * the generated import-object shape and (at bundle time) a real instantiation
 * before we publish either artifact.
 */
function matchingBrace(source, opening) {
  let depth = 0;
  let quote = null;
  for (let index = opening; index < source.length; index++) {
    const character = source[index];
    const next = source[index + 1];
    if (quote) {
      if (character === "\\") index++;
      else if (character === quote) quote = null;
      continue;
    }
    if (character === '"' || character === "'" || character === "`") {
      quote = character;
      continue;
    }
    if (character === "/" && next === "/") {
      index = source.indexOf("\n", index + 2);
      if (index === -1) return -1;
      continue;
    }
    if (character === "/" && next === "*") {
      index = source.indexOf("*/", index + 2);
      if (index === -1) return -1;
      index++;
      continue;
    }
    if (character === "{") depth++;
    else if (character === "}" && --depth === 0) return index;
  }
  return -1;
}

/**
 * The `import0` literal in glue whose identifiers a minifier renamed (the
 * published broker worker). Its keys survive minification, and wasm-bindgen
 * opens it with `__proto__: null` followed by its first import.
 */
function minifiedImportObjects(glueSource) {
  const objects = [];
  for (const match of glueSource.matchAll(/\{\s*__proto__\s*:\s*null\s*,\s*(?=__wbg)/g)) {
    const closing = matchingBrace(glueSource, match.index);
    if (closing !== -1) objects.push(glueSource.slice(match.index + 1, closing));
  }
  return objects;
}

function generatedImportObject(glueSource) {
  const functionStart = glueSource.indexOf("function __wbg_get_imports()");
  if (functionStart === -1) {
    const objects = minifiedImportObjects(glueSource);
    return objects.length === 1 ? objects[0] : null;
  }
  const functionOpening = glueSource.indexOf("{", functionStart);
  const functionEnd = matchingBrace(glueSource, functionOpening);
  if (functionEnd === -1) return null;
  const body = glueSource.slice(functionOpening + 1, functionEnd);
  const objectMatch = /(?:const|let|var)\s+import0\s*=\s*{/.exec(body);
  if (!objectMatch) return null;
  const opening = functionOpening + 1 + objectMatch.index + objectMatch[0].lastIndexOf("{");
  const closing = matchingBrace(glueSource, opening);
  return closing === -1 ? null : glueSource.slice(opening + 1, closing);
}

function callableImportNames(importObject) {
  const names = new Set();
  const callableProperty =
    /([A-Za-z_$][\w$]*)\s*:\s*(?:async\s+)?(?:function\b|(?:\([^)]*\)|[A-Za-z_$][\w$]*)\s*=>)/y;
  // wasm-bindgen emits object properties, not assignments. Scan only
  // top-level properties of its `import0` literal: a name in a comment, a
  // nested dead object, or a longer property name must not satisfy the ABI.
  let depth = 0;
  let quote = null;
  for (let index = 0; index < importObject.length; index++) {
    const character = importObject[index];
    const next = importObject[index + 1];
    if (quote) {
      if (character === "\\") index++;
      else if (character === quote) quote = null;
      continue;
    }
    if (character === '"' || character === "'" || character === "`") {
      quote = character;
      continue;
    }
    if (character === "/" && next === "/") {
      index = importObject.indexOf("\n", index + 2);
      if (index === -1) break;
      continue;
    }
    if (character === "/" && next === "*") {
      index = importObject.indexOf("*/", index + 2);
      if (index === -1) break;
      index++;
      continue;
    }
    if ("{([".includes(character)) {
      depth++;
      continue;
    }
    if ("})]".includes(character)) {
      depth--;
      continue;
    }
    if (depth === 0 && (index === 0 || !/[\w$]/.test(importObject[index - 1]))) {
      callableProperty.lastIndex = index;
      const match = callableProperty.exec(importObject);
      if (match) names.add(match[1]);
    }
  }
  return names;
}

export function verifyWasmGlueAbi(wasm, glueSource) {
  let imports;
  try {
    imports = WebAssembly.Module.imports(
      wasm instanceof WebAssembly.Module ? wasm : new WebAssembly.Module(wasm),
    );
  } catch (error) {
    return `invalid WASM binary: ${error.message}`;
  }

  const importObject = generatedImportObject(glueSource);
  if (!importObject) return "WASM glue does not expose wasm-bindgen's generated import object";

  const callable = callableImportNames(importObject);
  const missing = imports
    .filter(
      (entry) =>
        entry.kind === "function" &&
        (entry.module === "wbg" || entry.module === "./jazz_wasm_bg.js") &&
        !callable.has(entry.name),
    )
    .map((entry) => `${entry.module}.${entry.name}`);
  return missing.length ? `WASM glue is missing required imports: ${missing.join(", ")}` : null;
}

export function assertWasmGlueAbi(wasm, glueSource) {
  const problem = verifyWasmGlueAbi(wasm, glueSource);
  if (problem) throw new Error(problem);
}

/**
 * Evaluate the emitted self-contained worker as ESM, retrieve the exact
 * wasm-bindgen import object it constructs, and instantiate its adjacent
 * binary. This is intentionally a build-time check: the source was just
 * emitted by esbuild and no application code has run yet.
 */
export async function assertWasmGlueInstantiates(wasm, glueSource) {
  assertWasmGlueAbi(wasm, glueSource);
  try {
    const source = `${glueSource}\nexport { __wbg_get_imports as __jazz_wasm_imports };`;
    const url = `data:text/javascript;base64,${Buffer.from(source).toString("base64")}`;
    const worker = await import(url);
    await WebAssembly.instantiate(wasm, worker.__jazz_wasm_imports());
  } catch (error) {
    throw new Error(`WASM glue could not instantiate its binary: ${error.message}`);
  }
}
