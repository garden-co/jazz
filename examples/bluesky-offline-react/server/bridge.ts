import type { Operation } from "../operations.js";
import { OperationError, type SessionFetcher } from "./bluesky.js";
import { createProjector, type Projector } from "./projector.js";
import { createReconciler, type Reconciler } from "./reconciler.js";

export class BlueskyJazzBridge {
  constructor(
    private readonly projector: Projector,
    private readonly reconciler: Reconciler,
  ) {}

  projectTimelinePage(did: string, session: SessionFetcher, cursor?: string) {
    return this.projector.projectTimelinePage(did, session, cursor);
  }

  projectThread(did: string, session: SessionFetcher, uri: string) {
    return this.projector.projectThread(did, session, uri);
  }

  getTimelineProjectionStatus(did: string) {
    return this.projector.getTimelineProjectionStatus(did);
  }

  reconcileOperations(did: string, session: SessionFetcher, operations: Operation[]) {
    if (operations.some((operation) => operation.ownerDid !== did)) {
      throw new OperationError("operation owner mismatch", 400);
    }
    return this.reconciler.reconcileOperations(did, session, operations);
  }
}

const bridge = new BlueskyJazzBridge(createProjector(), createReconciler());

export const projectTimelinePage = bridge.projectTimelinePage.bind(bridge);
export const projectThread = bridge.projectThread.bind(bridge);
export const getTimelineProjectionStatus = bridge.getTimelineProjectionStatus.bind(bridge);
export const reconcileOperations = bridge.reconcileOperations.bind(bridge);

export { OperationError } from "./bluesky.js";
