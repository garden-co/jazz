// Copy the inspector overlay's embedded build into jazz-tools' own dist so the
// published package is self-contained (no jazz-inspector dependency). The dev
// server's resolveEmbeddedDir() reads it from this exact spot at runtime —
// keeping the destination here, next to that module, means the two agree.
// Run from the workspace (`pnpm --filter jazz-tools run stage:inspector-overlay`)
// after the inspector's embedded build to reproduce the published layout locally.
import { access, cp, mkdir, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url)); // packages/jazz-tools/scripts
const src = join(here, "../../inspector/dist-embedded");
const dest = join(here, "../dist/dev/inspector-overlay/embedded");

await rm(dest, { recursive: true, force: true });
await mkdir(dest, { recursive: true });
await cp(src, dest, { recursive: true });
await access(join(dest, "embedded.html"));
console.log(`Staged inspector overlay assets → ${dest}`);
