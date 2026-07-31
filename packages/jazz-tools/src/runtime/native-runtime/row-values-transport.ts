export function setNamedRowValuesEnumerable(value: unknown, enumerable: boolean): void {
  visit(value, enumerable, new WeakSet<object>());
}

function visit(value: unknown, enumerable: boolean, seen: WeakSet<object>): void {
  if (typeof value !== "object" || value === null || seen.has(value)) return;
  seen.add(value);

  if (value instanceof Map) {
    for (const entry of value.values()) visit(entry, enumerable, seen);
    return;
  }

  const descriptor = Object.getOwnPropertyDescriptor(value, "valuesByColumn");
  if (descriptor?.value instanceof Map) {
    Object.defineProperty(value, "valuesByColumn", { ...descriptor, enumerable });
    visit(descriptor.value, enumerable, seen);
  }

  for (const entry of Object.values(value)) visit(entry, enumerable, seen);
}
