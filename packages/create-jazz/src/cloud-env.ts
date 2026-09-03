import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const DEFAULT_HOSTED_KEYS = [
  "NEXT_PUBLIC_JAZZ_APP_ID",
  "NEXT_PUBLIC_JAZZ_SERVER_URL",
  "JAZZ_ADMIN_SECRET",
  "BACKEND_SECRET",
] as const;

type DefaultHostedKey = (typeof DEFAULT_HOSTED_KEYS)[number];

const TODO_COMMENT = "# TODO: provision at https://v2.dashboard.jazz.tools";

function parseEnv(content: string): Map<string, string> {
  const map = new Map<string, string>();
  for (let line of content.split("\n")) {
    if (line.endsWith("\r")) line = line.slice(0, -1);
    if (!line || line.startsWith("#")) continue;
    const eq = line.indexOf("=");
    if (eq === -1) continue;
    map.set(line.slice(0, eq), line.slice(eq + 1));
  }
  return map;
}

export function writeHostedEnv({
  dir,
  values,
  keys = DEFAULT_HOSTED_KEYS as unknown as string[],
}: {
  dir: string;
  values:
    | {
        NEXT_PUBLIC_JAZZ_APP_ID?: string;
        NEXT_PUBLIC_JAZZ_SERVER_URL?: string;
        JAZZ_ADMIN_SECRET?: string;
        BACKEND_SECRET?: string;
      }
    | Record<string, string | undefined>;
  keys?: string[];
}): void {
  for (const [key, value] of Object.entries(values)) {
    if (value && /[\n\r]/.test(value)) {
      throw new Error(
        `Refusing to write hosted env: value for ${key} contains an illegal newline character.`,
      );
    }
  }

  const envPath = join(dir, ".env");
  const existing = existsSync(envPath)
    ? readFileSync(envPath, "utf8").replace(/\r\n/g, "\n").replace(/\r/g, "\n")
    : "";
  const parsed = parseEnv(existing);
  const vals = values as Record<string, string | undefined>;

  // Empty hosted values are placeholders, so a successful retry may replace them.
  const replacements: Record<string, string> = {};
  for (const key of keys) {
    const supplied = vals[key];
    if (parsed.get(key) === "" && supplied) replacements[key] = supplied;
  }
  const skippedWithDifferentValue = keys.filter((key) => {
    const supplied = vals[key];
    const existingValue = parsed.get(key);
    return parsed.has(key) && existingValue !== "" && supplied && supplied !== existingValue;
  });

  if (skippedWithDifferentValue.length > 0) {
    console.warn(
      `Skipping ${skippedWithDifferentValue.join(", ")} in .env — entries already present. Edit the file by hand if you need to update them.`,
    );
  }

  const missing = keys.filter((key) => !parsed.has(key));
  const lines = existing.split("\n").map((line) => {
    const eq = line.indexOf("=");
    if (eq === -1) return line;
    const key = line.slice(0, eq);
    const replacement = replacements[key];
    return replacement !== undefined && line.slice(eq + 1) === "" ? `${key}=${replacement}` : line;
  });
  let base = lines.join("\n");

  // Remove the managed placeholder note once every hosted placeholder is filled.
  const needsTodo = keys.some(
    (key) => (replacements[key] ?? (parsed.has(key) ? parsed.get(key) : (vals[key] ?? ""))) === "",
  );
  if (Object.keys(replacements).length > 0 && !needsTodo) {
    base = base
      .split("\n")
      .filter((line) => line !== TODO_COMMENT)
      .join("\n");
  }

  if (missing.length === 0) {
    if (!base.endsWith("\n")) writeFileSync(envPath, base + "\n");
    else if (base !== existing) writeFileSync(envPath, base);
    return;
  }

  const additions = missing.map((key) => `${key}=${vals[key] ?? ""}`);
  if (base && !base.endsWith("\n")) base += "\n";
  const additionBlock = additions.join("\n") + "\n";
  const content =
    needsTodo && !base.includes(TODO_COMMENT)
      ? base + TODO_COMMENT + "\n" + additionBlock
      : base + additionBlock;
  writeFileSync(envPath, content);
}
