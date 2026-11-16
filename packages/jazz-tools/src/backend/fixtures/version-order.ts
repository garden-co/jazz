/**
 * `[a, b, whether a is newer than b]`, shared by the TypeScript
 * `newerVersion` test and the start script's `newer` test, so both order
 * core versions the same way.
 */
export const VERSION_ORDER: ReadonlyArray<readonly [string, string, boolean]> = [
  ["2.0.0-alpha.60", "2.0.0-alpha.59", true],
  ["2.0.0-alpha.100", "2.0.0-alpha.99", true],
  ["2.0.0", "2.0.0-alpha.60", true],
  ["2.0.0-alpha.60", "2.0.0", false],
  ["2.0.0", "2.0.0", false],
  // A release is newer than its candidates, never the other way round.
  ["2.0.0-alpha.64", "2.0.0-alpha.64-rc.1", true],
  ["2.0.0-alpha.64-rc.1", "2.0.0-alpha.64", false],
  ["2.0.0-alpha.65", "2.0.0-alpha.65-rc.1", true],
  ["2.0.0-alpha.65", "2.0.0-alpha.64-rc.1", true],
  // Candidates order by their release first, numerically, then by number.
  ["2.0.0-alpha.100-rc.1", "2.0.0-alpha.99-rc.1", true],
  ["2.0.0-alpha.99-rc.1", "2.0.0-alpha.100-rc.1", false],
  ["2.0.0-alpha.65-rc.2", "2.0.0-alpha.65-rc.1", true],
  ["2.0.0-alpha.65-rc.10", "2.0.0-alpha.65-rc.9", true],
  ["2.0.0-alpha.65-rc.1", "2.0.0-alpha.64-rc.1", true],
  ["2.0.0-alpha.65-rc.1", "2.0.0-alpha.64", true],
  ["2.0.0-alpha.64-rc.1", "2.0.0-alpha.64-rc.1", false],
];
