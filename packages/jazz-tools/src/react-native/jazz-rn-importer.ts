/** Isolated so missing-peer behavior can be tested without loading a native module. */
export function importJazzRn(): Promise<typeof import("jazz-rn")> {
  return import("jazz-rn");
}
