/**
 * Why `jazz-tools upgrade` stopped before changing anything: the project
 * isn't in a state it can convert, or uses something the new format can't
 * express yet. `location` is `file:line` when there is one.
 */
export class UpgradeRefusal extends Error {
  readonly location: string | undefined;

  constructor(message: string, location?: string) {
    super(location ? `${location}: ${message}` : message);
    this.name = "UpgradeRefusal";
    this.location = location;
  }
}

/** What the format (or upgrade) can't express yet, named exactly. */
export function notSupportedYet(what: string, location?: string, hint?: string): UpgradeRefusal {
  return new UpgradeRefusal(
    `${what} is not supported yet by \`jazz-tools upgrade\`${hint ? `: ${hint}` : "."}`,
    location,
  );
}
