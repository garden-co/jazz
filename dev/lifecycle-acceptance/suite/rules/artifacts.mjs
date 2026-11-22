// What runs is what was fetched.
import { readFileSync } from "node:fs";
import { sha256 } from "../../lib/common.mjs";
import { equal, value, Broken } from "../round.mjs";

export default [
  {
    id: "artifacts.loaded",
    title: "the running process has the candidate's core loaded",
    basis: "brief",
    rounds: 1,
    async run(r) {
      // Rust links it and Kotlin's is inside the jar: both are checked when built.
      const proxy = await r.proxy();
      const process = await r.process();
      const key = value(await process.call("genkey"), "genkey");
      value(await r.open(process, { dir: r.dir(), proxy, key }), "open");
      const maps = readFileSync(`/proc/${process.pid}/maps`, "utf8").split("\n");
      const loaded = new Set(maps.filter((line) => /jazz_ffi|jazz-ffi/.test(line)).map((line) => line.slice(line.indexOf("/"))));
      if (r.driver.loads) {
        if (loaded.size === 0) throw new Broken("no libjazz_ffi is loaded");
        for (const path of loaded) equal(await sha256(path), r.driver.loads, `another libjazz_ffi is loaded: ${path}`);
      } else if (loaded.size) {
        throw new Broken("a libjazz_ffi from outside the checked package is loaded", [...loaded].join(", "));
      }
      value(await process.call("close", { h: "a" }), "close");
    },
  },
];
