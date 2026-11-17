/**
 * The names `jazz.generated.ts` gives a table's types, as jazz-codegen
 * derives them (`crates/jazz-codegen/src/names.rs`): PascalCase singular for
 * the row (`chatMembers` → `ChatMember`), `<Row>Insert` for inserts and
 * `<Row><Column>` for an enum column's type. upgrade reads the generated
 * module when it could be generated and falls back to these rules when the
 * app file still has TODOs to resolve.
 */

/** PascalCase of an authored name: `chat_members` and `chatMembers` both give `ChatMembers`. */
export function pascal(name: string): string {
  return name
    .split("_")
    .filter((part) => part.length > 0)
    .map((part) => part[0]!.toUpperCase() + part.slice(1))
    .join("");
}

const S_SINGULARS = new Set([
  "alias",
  "atlas",
  "bias",
  "bonus",
  "bus",
  "campus",
  "canvas",
  "census",
  "focus",
  "gas",
  "lens",
  "radius",
  "status",
  "virus",
]);

const IRREGULAR: Record<string, string> = {
  people: "person",
  children: "child",
  men: "man",
  women: "woman",
  mice: "mouse",
  geese: "goose",
  feet: "foot",
  teeth: "tooth",
};

/** The singular of the last word of a (plural) table name. */
export function singular(name: string): string {
  let split = 0;
  for (let i = name.length - 1; i > 0; i--) {
    const c = name[i]!;
    if (c === "_") {
      split = i + 1;
      break;
    }
    if (c >= "A" && c <= "Z") {
      split = i;
      break;
    }
  }
  const head = name.slice(0, split);
  const word = name.slice(split);
  const lower = word.toLowerCase();
  const keepCase = (one: string): string => {
    const first = word[0];
    return first && first >= "A" && first <= "Z" ? first + one.slice(1) : one;
  };
  let one: string;
  if (IRREGULAR[lower] !== undefined) one = keepCase(IRREGULAR[lower]!);
  else if (lower.length > 3 && lower.endsWith("ies")) one = `${word.slice(0, -3)}y`;
  else if (lower.endsWith("ses") && S_SINGULARS.has(lower.slice(0, -2))) one = word.slice(0, -2);
  else if (["sses", "ches", "shes", "xes", "zes"].some((end) => lower.endsWith(end)))
    one = word.slice(0, -2);
  else if (
    lower.length > 1 &&
    lower.endsWith("s") &&
    !lower.endsWith("ss") &&
    !lower.endsWith("us") &&
    !lower.endsWith("is")
  ) {
    one = word.slice(0, -1);
  } else one = word;
  return `${head}${one}`;
}

/** The generated row interface of a table. */
export function rowTypeName(table: string): string {
  return pascal(singular(table));
}

/** The generated names of a table's types, by role. */
export interface TableTypeNames {
  row: string;
  insert: string;
}

/**
 * The table → type names a generated module declares, read from its
 * `interface Types` (`chats: { row: Chat; insert: ChatInsert; … }`), and
 * every name it exports.
 */
export function readGeneratedNames(text: string): {
  tables: Record<string, TableTypeNames>;
  exports: Set<string>;
} {
  const tables: Record<string, TableTypeNames> = {};
  const re =
    /^\s{2}(?:"([^"]+)"|([A-Za-z_$][\w$]*)): \{\s*\n?\s*row: ([A-Za-z_$][\w$]*);\s*\n?\s*insert: ([A-Za-z_$][\w$]*);/gm;
  for (const match of text.matchAll(re)) {
    tables[match[1] ?? match[2]!] = { row: match[3]!, insert: match[4]! };
  }
  const exports = new Set<string>();
  for (const match of text.matchAll(
    /^export (?:declare )?(?:interface|type|const|function|class|enum) ([A-Za-z_$][\w$]*)/gm,
  )) {
    exports.add(match[1]!);
  }
  return { tables, exports };
}
