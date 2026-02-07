const SEP = "|";

function padIdx(idx: number): string {
  return idx.toString().padStart(10, "0");
}

export const Keys = {
  coValue(id: string): string {
    return `cv${SEP}${id}`;
  },
  coValuePrefix(): string {
    return `cv${SEP}`;
  },

  session(coValueId: string, sessionID: string): string {
    return `se${SEP}${coValueId}${SEP}${sessionID}`;
  },
  sessionPrefix(coValueId: string): string {
    return `se${SEP}${coValueId}${SEP}`;
  },

  transaction(coValueId: string, sessionID: string, idx: number): string {
    return `tx${SEP}${coValueId}${SEP}${sessionID}${SEP}${padIdx(idx)}`;
  },
  transactionPrefix(coValueId: string, sessionID: string): string {
    return `tx${SEP}${coValueId}${SEP}${sessionID}${SEP}`;
  },

  signature(coValueId: string, sessionID: string, idx: number): string {
    return `si${SEP}${coValueId}${SEP}${sessionID}${SEP}${padIdx(idx)}`;
  },
  signaturePrefix(coValueId: string, sessionID: string): string {
    return `si${SEP}${coValueId}${SEP}${sessionID}${SEP}`;
  },

  deleted(id: string): string {
    return `de${SEP}${id}`;
  },
  deletedPrefix(): string {
    return `de${SEP}`;
  },

  unsynced(coValueId: string, peerId: string): string {
    return `us${SEP}${coValueId}${SEP}${peerId}`;
  },
  unsyncedPrefix(coValueId: string): string {
    return `us${SEP}${coValueId}${SEP}`;
  },
  allUnsyncedPrefix(): string {
    return `us${SEP}`;
  },
} as const;
