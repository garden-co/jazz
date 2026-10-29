import { defineLoadTimeBenchmarks } from "../bench.mjs";

defineLoadTimeBenchmarks("stage-plan", () => import("../scenarios/stage-plan.mjs"));
