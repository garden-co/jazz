import { defineLoadTimeBenchmarks } from "../bench.mjs";

defineLoadTimeBenchmarks("band-book", () => import("../scenarios/band-book.mjs"));
