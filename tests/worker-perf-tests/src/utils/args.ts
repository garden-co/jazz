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
  const res = nodeParseArgs({
    args: argv,
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

export function getFlagBoolean(args: ParsedArgs, key: string): boolean {
  const v = args.flags[key];
  if (v === undefined) return false;
  if (Array.isArray(v)) return Boolean(v[v.length - 1]);
  if (typeof v === "boolean") return v;
  return v !== "false";
}
