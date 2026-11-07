import { defineLoadTimeBenchmarks } from "../bench.mjs";

defineLoadTimeBenchmarks("jamazon", () => import("../scenarios/jamazon.mjs"));
