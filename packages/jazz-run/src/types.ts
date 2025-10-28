import { type Server } from "node:http";
import { type LocalNode } from "cojson";

export type SyncServer = Server & { localNode: LocalNode };
