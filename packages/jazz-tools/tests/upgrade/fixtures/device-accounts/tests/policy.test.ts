import { createPolicyTestApp } from "jazz-tools/testing";
import { app } from "../jazz.generated";

const testApp = createPolicyTestApp(app);
export const device = testApp.as({ authMode: "local-first", user: "u1" });
