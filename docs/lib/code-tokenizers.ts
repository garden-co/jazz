/**
 * Syntax tokenizers for languages Astryx `CodeBlock` does not highlight
 * itself (Rust, Svelte, Vue). Each returns flat tokens with absolute offsets,
 * the shape `CodeBlock`'s `tokenizer` prop takes, and uses Astryx's token
 * types so the theme's syntax colours apply.
 */

type Token = { type: string; start: number; end: number };
type Pattern = { type: string; regex: RegExp };

function compile(patterns: Pattern[]) {
  return patterns.map((p) => ({
    type: p.type,
    regex: new RegExp(p.regex.source, p.regex.flags.replace(/[gy]/g, "") + "y"),
  }));
}

function tokenizerFor(patterns: Pattern[]) {
  const compiled = compile(patterns);
  return (code: string): Token[] => {
    const tokens: Token[] = [];
    let pos = 0;
    while (pos < code.length) {
      let matched = false;
      for (const { type, regex } of compiled) {
        regex.lastIndex = pos;
        const match = regex.exec(code);
        if (match && match[0].length > 0) {
          if (type !== "plain") tokens.push({ type, start: pos, end: pos + match[0].length });
          pos += match[0].length;
          matched = true;
          break;
        }
      }
      if (!matched) pos++;
    }
    return tokens;
  };
}

const RUST_KEYWORDS =
  /\b(as|async|await|break|const|continue|crate|dyn|else|enum|extern|false|fn|for|if|impl|in|let|loop|match|mod|move|mut|pub|ref|return|self|Self|static|struct|super|trait|true|type|unsafe|use|where|while)\b/;

const rust = tokenizerFor([
  { type: "comment", regex: /\/\*[\s\S]*?\*\// },
  { type: "comment", regex: /\/\/[^\n]*/ },
  { type: "string", regex: /b?r#*"[\s\S]*?"#*/ },
  { type: "string", regex: /b?"(?:[^"\\]|\\.)*"/ },
  { type: "string", regex: /b?'(?:[^'\\]|\\.)'/ },
  { type: "constant", regex: /#!?\[[^\]]*\]/ },
  {
    type: "number",
    regex: /\b\d[\d_]*(?:\.[\d_]+)?(?:[eE][+-]?\d+)?(?:[iu](?:8|16|32|64|128|size)|f32|f64)?\b/,
  },
  { type: "keyword", regex: RUST_KEYWORDS },
  { type: "function", regex: /\b[a-z_][\w]*!/ },
  { type: "function", regex: /\b[a-z_][\w]*(?=\s*(?:::<[^>]*>)?\()/ },
  { type: "type", regex: /\b[A-Z][A-Za-z0-9_]*\b/ },
  { type: "operator", regex: /[+\-*/%=!<>&|^~?:]+/ },
  { type: "punctuation", regex: /[{}()[\];,.]/ },
  { type: "plain", regex: /\b[A-Za-z_][\w]*\b/ },
]);

const JS_KEYWORDS =
  /\b(const|let|var|function|class|if|else|for|while|return|import|export|from|default|async|await|try|catch|throw|new|typeof|instanceof|interface|type|enum|extends|implements|switch|case|break|continue|do|in|of|void|null|undefined|true|false|this|super|yield|as|keyof)\b/;

// Svelte and Vue single-file components: markup tags and attributes, block
// syntax ({#if}, v-*), and the script's TypeScript, all in one pass.
const component = tokenizerFor([
  { type: "comment", regex: /<!--[\s\S]*?-->/ },
  { type: "comment", regex: /\/\*[\s\S]*?\*\// },
  { type: "comment", regex: /\/\/[^\n]*/ },
  { type: "tag", regex: /<\/[a-zA-Z][\w.:-]*\s*>/ },
  { type: "tag", regex: /<[a-zA-Z][\w.:-]*/ },
  { type: "tag", regex: /\/?>/ },
  { type: "keyword", regex: /\{[#:/@][a-z]+/ },
  { type: "string", regex: /`(?:[^`\\]|\\.)*`/ },
  { type: "string", regex: /"(?:[^"\\]|\\.)*"/ },
  { type: "string", regex: /'(?:[^'\\]|\\.)*'/ },
  { type: "attribute", regex: /(?:[:@]|v-|on:|bind:)?[a-zA-Z_][\w:.-]*(?==)/ },
  { type: "number", regex: /\b\d[\d_]*\.?[\d_]*\b/ },
  { type: "keyword", regex: JS_KEYWORDS },
  { type: "function", regex: /\b[a-zA-Z_$][\w$]*(?=\s*\()/ },
  { type: "type", regex: /\b[A-Z][a-zA-Z0-9_]*\b/ },
  { type: "operator", regex: /[+\-*/%=!&|^~?]+/ },
  { type: "punctuation", regex: /[{}()[\];,.]/ },
  { type: "plain", regex: /\b[a-zA-Z_$][\w$]*\b/ },
]);

const tokenizers: Record<string, (code: string) => Token[]> = {
  rs: rust,
  rust,
  svelte: component,
  vue: component,
};

/** Normalises fence languages to names Astryx `CodeBlock` knows. */
export function codeLanguage(language: string | undefined): string {
  switch (language) {
    case undefined:
    case "":
    case "text":
    case "txt":
    case "env":
      return "plaintext";
    case "rs":
      return "rust";
    default:
      return language;
  }
}

/** A tokenizer for `language` when Astryx has none of its own. */
export function customTokenizer(language: string) {
  return tokenizers[language];
}
