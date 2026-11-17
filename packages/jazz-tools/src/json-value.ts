/** A JSON value: what a `json` column without a JSON Schema holds (`jazz.generated.ts` imports it). */
export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | { [key: string]: JsonValue } | JsonValue[];
