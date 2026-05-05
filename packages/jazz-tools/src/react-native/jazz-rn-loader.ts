import { isModuleNotFoundError } from "../runtime/peer-dep-error.js";
import { importJazzRn } from "./jazz-rn-importer.js";

type JazzRnDefault = (typeof import("jazz-rn"))["default"];

let cached: JazzRnDefault | null = null;

function makeMissingError(cause: unknown): Error {
  return new Error(
    `[jazz-tools] The "jazz-rn" peer dependency is required to use jazz-tools/react-native but is not installed.\n` +
      `Install it alongside jazz-tools, e.g.:\n` +
      `  npm install jazz-rn\n` +
      `  pnpm add jazz-rn\n` +
      `  yarn add jazz-rn`,
    { cause },
  );
}

export async function loadJazzRn(): Promise<JazzRnDefault> {
  if (cached) return cached;
  let mod: typeof import("jazz-rn");
  try {
    mod = await importJazzRn();
  } catch (err) {
    if (!isModuleNotFoundError(err, "jazz-rn")) throw err;
    throw makeMissingError(err);
  }
  cached = mod.default;
  return cached;
}

export function getJazzRnSync(): JazzRnDefault {
  if (!cached) {
    throw new Error(
      `[jazz-tools] jazz-rn was accessed before it was loaded. ` +
        `Use createDb()/createJazzClient() (which load jazz-rn before returning), or await loadJazzRn() first.`,
    );
  }
  return cached;
}
