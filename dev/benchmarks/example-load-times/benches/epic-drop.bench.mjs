import { defineLoadTimeBenchmarks } from "../bench.mjs";

defineLoadTimeBenchmarks("epic-drop", () => import("../scenarios/epic-drop.mjs"));
