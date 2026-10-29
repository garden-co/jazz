import { defineLoadTimeBenchmarks } from "../bench.mjs";

defineLoadTimeBenchmarks("poster-shop", () => import("../scenarios/poster-shop.mjs"));
