declare module "jazz-wasm" {
  export default function init(input?: unknown): Promise<void>;
  export function scanOpfsBTreeEntriesFromFileBytes(fileBytes: Uint8Array): Array<{
    key: string;
    keyBytes: Uint8Array;
    value: Uint8Array;
  }>;
}
