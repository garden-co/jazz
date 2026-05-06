// Indirection so tests can mock the dynamic import to simulate a missing
// peer dependency or other resolution failures.
export function importJazzRn(): Promise<typeof import("jazz-rn")> {
  return import("jazz-rn");
}
