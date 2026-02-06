/** Every request sent from main thread → worker */
export type WorkerRequest = {
  reqId: number;
  method: string;
  args: unknown[];
};

/** Every response sent from worker → main thread */
export type WorkerResponse = {
  reqId: number;
  result?: unknown;
  error?: string;
};

/** Initialization message sent once on startup */
export type WorkerInitRequest = {
  type: "init";
  dbName: string;
  cacheSizeBytes: number;
};

/** Initialization response */
export type WorkerInitResponse =
  | { type: "ready" }
  | { type: "error"; message: string };

/** All message types the worker can receive */
export type WorkerIncoming = WorkerInitRequest | WorkerRequest;
