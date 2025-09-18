import { cpSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";

mkdirSync("./public", { recursive: true });

const wasm = readFileSync("./pkg/cojson_core_wasm_bg.wasm");

writeFileSync(
  "./public/cojson_core_wasm.wasm.js",
  `export const data = "data:application/wasm;base64,${wasm.toString("base64")}";`,
);
writeFileSync(
  "./public/cojson_core_wasm.wasm.d.ts",
  "export const data: string;",
);

const glueJs = readFileSync("./pkg/cojson_core_wasm.js", "utf8").replace(
  "module_or_path = new URL('cojson_core_wasm_bg.wasm', import.meta.url);",
  "throw new Error();",
);

cpSync("./pkg/cojson_core_wasm_bg.wasm", "./public/cojson_core_wasm.wasm");

writeFileSync("./public/cojson_core_wasm.js", glueJs);

writeFileSync(
  "./public/cojson_core_wasm.d.ts",
  readFileSync("./pkg/cojson_core_wasm.d.ts", "utf8"),
);
