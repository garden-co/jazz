// Legacy acceptedHistoryV1 encoding remains readable as data, not authority.
// Provisional writes use separately versioned, bound private-key selections in
// retained-content-cache.ts; local observations never stand in for complete reads.
export {
  decodeAcceptedHistoryReads,
  encodeAcceptedHistoryReads,
} from "./accepted-history-format.js";
