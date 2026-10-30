// StagePlan on the homepage: server and actions for stage-plan-two-devices.storyboard.ts.
import { stagePlan } from "./stage-plan.shared.mjs";

export { app } from "./stage-plan.shared.mjs";
export const { server, deviceOptions, actions } = stagePlan(
  Number(process.env.EXAMPLE_PORT ?? 5199),
);
