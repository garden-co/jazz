// `pnpm seed`: reset the local deployment and write fixtures/stage-plan.mjs.
// Needs the backend running (`pnpm backend` in another terminal).
import { execFileSync } from "node:child_process";
execFileSync("npx", ["convex", "run", "seed:run"], { stdio: "inherit" });
