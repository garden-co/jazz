// The public database API error with stable machine-readable codes.
import { NodeError } from "./index.js";

export enum ErrorCode {
  Schema = "Schema",
  Query = "Query",
  WriteRejected = "WriteRejected",
  TransactionConflict = "TransactionConflict",
  Storage = "Storage",
  Protocol = "Protocol",
  Backpressure = "Backpressure",
  NotObserved = "NotObserved",
  HistoricalReadRequiresServer = "HistoricalReadRequiresServer",
}

/** The stable, snake_case name of a code: a durable contract exposed to JavaScript. */
export function errorCodeStr(code: ErrorCode): string {
  switch (code) {
    case ErrorCode.Schema:
      return "schema";
    case ErrorCode.Query:
      return "query";
    case ErrorCode.WriteRejected:
      return "write_rejected";
    case ErrorCode.TransactionConflict:
      return "transaction_conflict";
    case ErrorCode.Storage:
      return "storage";
    case ErrorCode.Protocol:
      return "protocol";
    case ErrorCode.Backpressure:
      return "backpressure";
    case ErrorCode.NotObserved:
      return "not_observed";
    case ErrorCode.HistoricalReadRequiresServer:
      return "historical_read_requires_server";
  }
}

/** Public API error with stable machine-readable codes. */
export class ApiError extends Error {
  constructor(
    readonly code: ErrorCode,
    readonly detail: string,
  ) {
    super(
      code === ErrorCode.TransactionConflict
        ? `(transaction_conflict): ${detail}`
        : `${code}: ${detail}`,
    );
  }

  static fromNode(error: NodeError): ApiError {
    let code: ErrorCode;
    switch (error.kind) {
      case "HistoricalReadRequiresServer":
        code = ErrorCode.HistoricalReadRequiresServer;
        break;
      case "Storage":
      case "Groove":
        code = ErrorCode.Storage;
        break;
      case "Query":
      case "UnsupportedExclusiveRead":
        code = ErrorCode.Query;
        break;
      case "TransactionConflict":
        code = ErrorCode.TransactionConflict;
        break;
      case "TableNotFound":
      case "UnsupportedColumnType":
      case "InvalidMergeableCommit":
        code = ErrorCode.Schema;
        break;
      default:
        code = ErrorCode.Protocol;
    }
    return new ApiError(code, error.message);
  }

  static fromQuery(error: Error): ApiError {
    return new ApiError(ErrorCode.Query, error.message);
  }
}
