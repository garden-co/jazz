import { defineLoadTimeBenchmarks } from "../bench.mjs";

defineLoadTimeBenchmarks("record-player", () => import("../scenarios/record-player.mjs"));
