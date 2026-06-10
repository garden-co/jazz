declare module "jazz-wasm" {
  export interface OpfsBTreeRawEntry {
    key: string;
    keyBytes: Uint8Array;
    value: Uint8Array;
  }

  export interface OpfsBTreeRawEntryBatch {
    entries: OpfsBTreeRawEntry[];
    done: boolean;
  }

  export default function init(input?: unknown): Promise<void>;
  export function scanOpfsBTreeEntriesFromFileBytes(fileBytes: Uint8Array): OpfsBTreeRawEntry[];
  export class OpfsBTreeEntryScanner {
    constructor(fileBytes: Uint8Array);
    nextBatch(limit: number): OpfsBTreeRawEntryBatch;
  }
}
