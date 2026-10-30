// StagePlan on the examples page: server and actions for stage-plan.storyboard.ts.
import { stagePlan } from "./stage-plan.shared.mjs";

export { app } from "./stage-plan.shared.mjs";
export const { server, deviceOptions, actions } = stagePlan(5392);
