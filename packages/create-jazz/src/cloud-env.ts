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

  // A key is missing only when it is entirely absent from the file
  const missing = keys.filter((k) => !parsed.has(k));

  const skippedWithDifferentValue = keys.filter((k) => {
    const supplied = vals[k];
    return parsed.has(k) && supplied && supplied !== parsed.get(k);
  });

  if (skippedWithDifferentValue.length > 0) {
    console.warn(
      `Skipping ${skippedWithDifferentValue.join(", ")} in .env — entries already present. Edit the file by hand if you need to update them.`,
    );
  }

  if (missing.length === 0) {
    if (!existing.endsWith("\n")) {
      writeFileSync(envPath, existing + "\n");
    }
    return;
  }

  const additions: string[] = [];
  for (const key of missing) {
    additions.push(`${key}=${vals[key] ?? ""}`);
  }

  // TODO comment is needed when any final value (existing or newly added) is empty
  const needsTodo =
    additions.some((line) => line.endsWith("=")) ||
    keys.some((k) => parsed.has(k) && parsed.get(k) === "");

  let base = existing;
  if (base && !base.endsWith("\n")) base += "\n";

  const additionBlock = additions.join("\n") + "\n";
  const content = needsTodo ? base + TODO_COMMENT + "\n" + additionBlock : base + additionBlock;

  writeFileSync(envPath, content);
}
