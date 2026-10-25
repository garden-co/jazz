#!/bin/bash
# usage: dump.sh name dir
name=$1; dir=$2
schema=$(ls $dir/schema.ts $dir/schema/index.ts 2>/dev/null | head -1)
cat > e-$name.ts <<EOT
import * as P from "$dir/permissions";
import * as S from "${schema%.ts}";
const perms = (P as any).default ?? (P as any).permissions ?? Object.values(P).find((v: any) => v && typeof v === "object");
const app: any = (S as any).app ?? (S as any).default;
const ws = app?.wasmSchema ?? null;
console.log(JSON.stringify({ perms, schema: ws }, null, 1));
EOT
node build.mjs e-$name.ts o-$name.mjs && node o-$name.mjs > $name.json 2> $name.err; echo "$name exit $? $(wc -c < $name.json)"
