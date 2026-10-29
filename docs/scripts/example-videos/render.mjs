// Renders walkthrough videos from their storyboards. See README.md.
//
//   pnpm render:walkthroughs                 # every walkthrough
//   pnpm render:walkthroughs jamazon band-chat
//   pnpm render:walkthroughs --list          # the ids
//
// Walkthroughs render one after another; a failed one doesn't stop the rest,
// and the command exits non-zero if any failed.
import { render, walkthroughIds } from "./walkthrough.mjs";

const args = process.argv.slice(2);
const all = await walkthroughIds();
if (args.includes("--list")) {
  console.log(all.join("\n"));
  process.exit(0);
}
const unknown = args.filter((id) => !all.includes(id));
if (unknown.length) {
  console.error(`Unknown walkthrough: ${unknown.join(", ")}. Known: ${all.join(", ")}`);
  process.exit(2);
}

const failed = [];
for (const id of args.length ? args : all) {
  const started = Date.now();
  console.log(`\n▶ ${id}`);
  try {
    await render(id);
    console.log(`✓ ${id} in ${Math.round((Date.now() - started) / 1000)} s`);
  } catch (error) {
    console.error(error);
    console.error(`✗ ${id} failed after ${Math.round((Date.now() - started) / 1000)} s`);
    failed.push(id);
  }
}
if (failed.length) {
  console.error(`\nFailed: ${failed.join(", ")}`);
  process.exit(1);
}
