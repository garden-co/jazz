import { waitForPublication } from "./publication-wait.ts";

/**
 * The seed foreground may acknowledge recovery only after its original
 * subscription and its public local read both contain Core's exact marker.
 */
export async function requireCoreRecoveryMarker(
  observed: () => boolean,
  readTitles: () => Promise<readonly string[]>,
  title: string,
  waitForMarker: (predicate: () => boolean) => Promise<boolean> = waitForPublication,
): Promise<void> {
  if (!(await waitForMarker(observed))) {
    throw new Error("original installed subscription did not receive Core's post-recovery marker");
  }
  if (!(await readTitles()).includes(`${title}:recovered-by-core`)) {
    throw new Error("original installed foreground did not read Core's post-recovery marker");
  }
}
