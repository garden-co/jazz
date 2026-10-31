import { BRIEF_URL, fetchBrief, renderBrief, renderFetchError } from "./brief.js";

// Arguments are ignored: `npm create jazz my-app` and friends all print the
// same brief. JAZZ_AGENT_BRIEF_URL points at a docs preview for testing.
const url = process.env.JAZZ_AGENT_BRIEF_URL || BRIEF_URL;

try {
  const brief = await fetchBrief(url);
  process.stdout.write(renderBrief(brief, process.stdout.isTTY === true));
} catch (err) {
  process.stderr.write(renderFetchError(url, err));
  process.exitCode = 1;
}
