import { parseArgs as nodeParseArgs } from "node:util";

type FlagValue = string | boolean;

export type ParsedArgs = {
  _: string[];
  flags: Record<string, FlagValue | FlagValue[]>;
};

/**
 * Thin wrapper around Node’s built-in `node:util` `parseArgs`.
 * We keep the existing `ParsedArgs` shape to avoid churn elsewhere.
 */
export function parseArgs(argv: string[]): ParsedArgs {
  // Strip bare `--` separators injected by package managers (pnpm/npm)
  // when forwarding flags (e.g. `pnpm batch -- --storage fjall`).
  // Node's parseArgs treats `--` as end-of-options, which would cause
  // all subsequent flags to be silently ignored.
  const filtered = argv.filter((a) => a !== "--");

  const res = nodeParseArgs({
    args: filtered,
    allowPositionals: true,
    strict: true,
    options: {
      help: { type: "boolean" },

      db: { type: "string" },
      pdf: { type: "string" },

      items: { type: "string" },
      limit: { type: "string" },

      workers: { type: "string" },
      durationMs: { type: "string" },
      inflight: { type: "string" },

      host: { type: "string" },
      port: { type: "string" },
      peer: { type: "string" },
      "random-port": { type: "boolean" },

      mix: { type: "string" },
      mixMode: { type: "string" },

      // Scenario selection
      scenario: { type: "string" },

      // Storage engine
      storage: { type: "string" },

      // Batch benchmark scenario options
      maps: { type: "string" },
      runs: { type: "string" },
      minSize: { type: "string" },
      maxSize: { type: "string" },
    },
  });

  const flags: ParsedArgs["flags"] = {};
  for (const [k, v] of Object.entries(res.values)) {
    if (v === undefined) continue;
    flags[k] = v as FlagValue | FlagValue[];
  }

  return { _: res.positionals, flags };
}

export function getFlagString(
  args: ParsedArgs,
  key: string,
): string | undefined {
  const v = args.flags[key];
  if (v === undefined) return;
  if (Array.isArray(v)) return String(v[v.length - 1]);
  if (typeof v === "boolean") return v ? "true" : "false";
  return String(v);
}

export function getFlagNumber(
  args: ParsedArgs,
  key: string,
): number | undefined {
  const s = getFlagString(args, key);
  if (s === undefined) return;
  const n = Number(s);
  if (!Number.isFinite(n)) return;
  return n;
}

export type StorageEngine = "sqlite" | "fjall";

/**
 * Parse and validate the --storage flag.
 * Defaults to "sqlite" if not provided.
 */
export function getStorageEngine(args: ParsedArgs): StorageEngine {
  const raw = getFlagString(args, "storage") ?? "sqlite";
  if (raw !== "sqlite" && raw !== "fjall") {
    throw new Error(
      `Invalid --storage value "${raw}". Must be "sqlite" or "fjall".`,
    );
  }
  return raw;
}

export function getFlagBoolean(args: ParsedArgs, key: string): boolean {
  const v = args.flags[key];
  if (v === undefined) return false;
  if (Array.isArray(v)) return Boolean(v[v.length - 1]);
  if (typeof v === "boolean") return v;
  return v !== "false";
}
