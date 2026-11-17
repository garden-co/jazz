/**
 * TypeScript programs over the project, with `jazz-tools` resolved to a
 * chosen package: the DSL-era one while reading schema.ts, this one (the
 * new runtime) for the closing typecheck.
 */
import { mkdirSync, mkdtempSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import type * as TS from "typescript";

type Ts = typeof TS;

export interface ProgramHandle {
  program: TS.Program;
  dispose(): void;
}

export function createProgram(
  ts: Ts,
  rootNames: string[],
  options: TS.CompilerOptions,
  jazzTools: string | undefined,
): ProgramHandle {
  const host = ts.createCompilerHost(options, true);
  let scratch: string | undefined;
  if (jazzTools) {
    // Resolve `jazz-tools` (and its subpaths, through its exports) as if
    // installed beside a scratch file whose node_modules links to it.
    scratch = mkdtempSync(join(tmpdir(), "jazz-upgrade-"));
    mkdirSync(join(scratch, "node_modules"));
    symlinkSync(jazzTools, join(scratch, "node_modules", "jazz-tools"), "dir");
    const anchor = join(scratch, "anchor.ts");
    host.resolveModuleNameLiterals = (literals, containingFile, redirected, compilerOptions) =>
      literals.map((literal) => {
        const name = literal.text;
        const from =
          name === "jazz-tools" || name.startsWith("jazz-tools/") ? anchor : containingFile;
        return ts.resolveModuleName(name, from, compilerOptions, host, undefined, redirected);
      });
  }
  const program = ts.createProgram({ rootNames, options, host });
  return {
    program,
    dispose: () => {
      if (scratch) rmSync(scratch, { recursive: true, force: true });
    },
  };
}

/** An error in one of the project's own files. */
export interface TypeError {
  /** `file:line:col: message`. */
  text: string;
  /** The error without its position: the same error before and after a rewrite moved its line. */
  key: string;
}

/** Errors in the project's own files. */
export function typeErrors(ts: Ts, program: TS.Program, root: string): TypeError[] {
  const out: TypeError[] = [];
  for (const diagnostic of ts.getPreEmitDiagnostics(program)) {
    if (diagnostic.category !== ts.DiagnosticCategory.Error) continue;
    const message = ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n").split("\n")[0]!;
    if (!diagnostic.file) {
      out.push({ text: message, key: `TS${diagnostic.code} ${message}` });
      continue;
    }
    const path = relative(root, diagnostic.file.fileName);
    if (path.startsWith("..") || path.includes("node_modules")) continue;
    const { line, character } = diagnostic.file.getLineAndCharacterOfPosition(
      diagnostic.start ?? 0,
    );
    out.push({
      text: `${path}:${line + 1}:${character + 1}: ${message}`,
      key: `${path} TS${diagnostic.code} ${message}`,
    });
  }
  return out;
}
