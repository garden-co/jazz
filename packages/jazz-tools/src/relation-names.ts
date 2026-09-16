/** Bounded identifier inflection, shared by runtime relations and their types.
 * This is a naming convention, not an English-language inflector.
 */
const irregular = {
  person: "people",
  child: "children",
  mouse: "mice",
  goose: "geese",
  tooth: "teeth",
  foot: "feet",
  analysis: "analyses",
  status: "statuses",
  alias: "aliases",
  bus: "buses",
} as const;
const uncountable = {
  equipment: "equipment",
  news: "news",
  information: "information",
  software: "software",
  data: "data",
  media: "media",
  series: "series",
  species: "species",
  fish: "fish",
  sheep: "sheep",
} as const;
export const relationInflectionWords = { ...irregular, ...uncountable };
type Words = typeof relationInflectionWords;
type Word = keyof Words;

type DictionaryMatch<S extends string> = {
  [K in Word]: S extends `${infer P}${K}`
    ? `${P}${Words[K]}`
    : S extends `${infer P}${Capitalize<K>}`
      ? `${P}${Capitalize<Words[K]>}`
      : S extends `${infer P}${Uppercase<K>}`
        ? `${P}${Uppercase<Words[K]>}`
        : never;
}[Word];
type PluralWord = Words[Word];
type ExistingPlural<S extends string> =
  S extends `${string}${PluralWord | Capitalize<PluralWord> | Uppercase<PluralWord>}`
    ? true
    : false;
type Consonant =
  | "b"
  | "c"
  | "d"
  | "f"
  | "g"
  | "h"
  | "j"
  | "k"
  | "l"
  | "m"
  | "n"
  | "p"
  | "q"
  | "r"
  | "s"
  | "t"
  | "v"
  | "w"
  | "x"
  | "z";
type Ending<S extends string, E extends string> = S extends Uppercase<S> ? Uppercase<E> : E;
type RegularPlural<S extends string> =
  Lowercase<S> extends `${string}${Consonant}y`
    ? S extends `${infer Stem}${"y" | "Y"}`
      ? `${Stem}${Ending<S, "ies">}`
      : never
    : Lowercase<S> extends `${string}${"ss" | "sh" | "ch" | "x" | "z"}`
      ? `${S}${Ending<S, "es">}`
      : Lowercase<S> extends `${string}s`
        ? S
        : `${S}${Ending<S, "s">}`;
export type PluralRelationName<S extends string> = string extends S
  ? string
  : S extends ""
    ? ""
    : ExistingPlural<S> extends true
      ? S
      : [DictionaryMatch<S>] extends [never]
        ? RegularPlural<S>
        : DictionaryMatch<S>;

export type StripRefSuffix<S extends string> = S extends `${infer P}_ids`
  ? P
  : S extends `${infer P}Ids`
    ? P
    : S extends `${infer P}_id`
      ? P
      : S extends `${infer P}Id`
        ? P
        : S;
export type ForwardRelationName<S extends string> = string extends S
  ? string
  : S extends `${string}_ids` | `${string}Ids`
    ? PluralRelationName<StripRefSuffix<S>>
    : StripRefSuffix<S>;

const capitalize = (value: string): string => value.charAt(0).toUpperCase() + value.slice(1);
const dictionary = Object.entries(relationInflectionWords).flatMap(
  ([singular, plural]) =>
    [
      [singular, plural],
      [capitalize(singular), capitalize(plural)],
      [singular.toUpperCase(), plural.toUpperCase()],
    ] as const,
);

export function pluralRelationName<S extends string>(name: S): PluralRelationName<S>;
export function pluralRelationName(name: string): string {
  if (!name || dictionary.some(([, plural]) => name.endsWith(plural))) return name;
  for (const [singular, plural] of dictionary) {
    if (name.endsWith(singular)) return name.slice(0, -singular.length) + plural;
  }
  const lower = name.toLowerCase();
  const ending = (value: string) => (name === name.toUpperCase() ? value.toUpperCase() : value);
  if (
    lower.endsWith("y") &&
    lower.length > 1 &&
    "bcdfghjklmnpqrstvwxz".includes(lower.slice(-2, -1))
  )
    return name.slice(0, -1) + ending("ies");
  if (["ss", "sh", "ch", "x", "z"].some((suffix) => lower.endsWith(suffix)))
    return name + ending("es");
  return lower.endsWith("s") ? name : name + ending("s");
}

export function forwardRelationName<S extends string>(column: S): ForwardRelationName<S>;
export function forwardRelationName(column: string): string {
  if (column.endsWith("_ids")) return pluralRelationName(column.slice(0, -4));
  if (column.endsWith("Ids")) return pluralRelationName(column.slice(0, -3));
  if (column.endsWith("_id")) return column.slice(0, -3);
  if (column.endsWith("Id")) return column.slice(0, -2);
  return column;
}
