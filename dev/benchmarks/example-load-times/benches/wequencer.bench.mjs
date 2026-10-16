import { defineLoadTimeBenchmarks } from "../bench.mjs";

defineLoadTimeBenchmarks("wequencer", () => import("../scenarios/wequencer.mjs"));
