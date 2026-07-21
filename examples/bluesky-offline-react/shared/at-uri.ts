// Jazz retains the authoritative ATProto URI; reconciliation extracts its PDS record key.
export function parseAtRecordUri(uri: string | null | undefined) {
  const match = uri?.match(/^at:\/\/([^/]+)\/([^/]+)\/([^/]+)$/);
  return match
    ? { repo: match[1], collection: match[2], rkey: match[3] }
    : undefined;
}
