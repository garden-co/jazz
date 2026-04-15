import { execFileSync } from "node:child_process";

function readEffectiveHooksPath() {
  try {
    return execFileSync("git", ["config", "--get", "core.hooksPath"], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    }).trim();
  } catch {
    return "";
  }
}

const hooksPath = readEffectiveHooksPath();

if (hooksPath) {
  console.log(
    `Skipping lefthook install because core.hooksPath is already managed externally: ${hooksPath}`,
  );
  process.exit(0);
}

execFileSync("lefthook", ["install"], {
  stdio: "inherit",
});
