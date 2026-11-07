import { defineLoadTimeBenchmarks } from "../bench.mjs";

defineLoadTimeBenchmarks("band-chat", () => import("../scenarios/band-chat.mjs"));
