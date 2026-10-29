import { defineLoadTimeBenchmarks } from "../bench.mjs";

defineLoadTimeBenchmarks("world-tour", () => import("../scenarios/world-tour.mjs"));
