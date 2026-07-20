import { createProjector } from "./projector.js";
import { createReconciler } from "./reconciler.js";

const projector = createProjector();
const reconciler = createReconciler();

export const projectTimelinePage = projector.projectTimelinePage;
export const projectThread = projector.projectThread;
export const reconcileOperations = reconciler.reconcileOperations;
