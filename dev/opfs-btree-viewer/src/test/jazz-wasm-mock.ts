export interface MockRawEntry {
  key: string;
  keyBytes: Uint8Array;
  value: Uint8Array;
}

let mockEntries: MockRawEntry[] = [
  {
    key: "raw:debug:alpha",
    keyBytes: new TextEncoder().encode("raw:debug:alpha"),
    value: new TextEncoder().encode("one"),
  },
];

export function setMockEntries(entries: MockRawEntry[]): void {
  mockEntries = entries;
}

export default async function init(): Promise<void> {}

export function scanOpfsBTreeEntriesFromFileBytes(): MockRawEntry[] {
  return mockEntries;
}
