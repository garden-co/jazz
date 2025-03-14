export const SelfReferenceSymbol = "SelfReference" as const;

export type SelfReference = {
  [SelfReferenceSymbol]: true;
  isOptional: true;
};

export type markSelfReferenceAsOptional<T> = T extends SelfReference
  ? undefined
  : never;

export function self(): SelfReference {
  return {
    [SelfReferenceSymbol]: true,
    isOptional: true,
  };
}

export function isSelfReference(value: unknown): value is SelfReference {
  return (
    typeof value === "object" && value !== null && SelfReferenceSymbol in value
  );
}
