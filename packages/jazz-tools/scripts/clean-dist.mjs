import { rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const distDir = fileURLToPath(new URL("../dist", import.meta.url));

await rm(distDir, { recursive: true, force: true });
