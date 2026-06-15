export interface Locked {
  readonly __jazzLocked: true;
}

export const Locked: Locked = Object.freeze({ __jazzLocked: true });

export function isLocked(value: unknown): value is Locked {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { __jazzLocked?: unknown }).__jazzLocked === true
  );
}
