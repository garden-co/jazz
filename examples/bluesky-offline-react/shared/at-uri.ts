export function parseAtUri(uri: string | null | undefined) {
  const match = uri?.match(/^at:\/\/([^/]+)\/([^/]+)\/([^/]+)$/);
  return match ? { repository: match[1], collection: match[2], recordKey: match[3] } : undefined;
}
