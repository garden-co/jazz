import { defineLoadTimeBenchmarks } from "../bench.mjs";

defineLoadTimeBenchmarks("big-label", () => import("../scenarios/big-label.mjs"));
