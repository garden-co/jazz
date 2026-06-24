import { adjectives, animals, uniqueNamesGenerator } from "unique-names-generator";

const displayNameKey = "collab-editor:displayName";

export function getDisplayName(): string {
  const stored = localStorage.getItem(displayNameKey);
  if (stored) return stored;

  const displayName = uniqueNamesGenerator({
    dictionaries: [adjectives, animals],
    separator: "-",
    length: 2,
  });
  localStorage.setItem(displayNameKey, displayName);
  return displayName;
}
