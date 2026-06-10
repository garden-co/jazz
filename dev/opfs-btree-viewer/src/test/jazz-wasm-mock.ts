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

export class OpfsBTreeEntryScanner {
  private cursor = 0;

  nextBatch(limit: number): { entries: MockRawEntry[]; done: boolean } {
    const end = Math.min(this.cursor + Math.max(limit, 1), mockEntries.length);
    const entries = mockEntries.slice(this.cursor, end);
    this.cursor = end;
    return { entries, done: this.cursor >= mockEntries.length };
  }
}
