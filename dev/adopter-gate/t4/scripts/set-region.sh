#!/usr/bin/env bash
# Add `blobs` and `region` to the app's vercel({ ... }) call. usage: set-region.sh <app-dir> [region]
APP=$1; R=${2:-iad1}
if ! grep -q 'blobs:' "$APP/jazz.server.ts"; then
  sed -i 's/perBranch, vercel }/perBranch, vercel, vercelBlob }/;s|  store: vercel({|  store: vercel({\n    blobs: process.env.BLOB_STORE_ID ? vercelBlob() : "sqlite",|' "$APP/jazz.server.ts"
fi
grep -q 'region:' "$APP/jazz.server.ts" && { echo "region already set"; exit 0; }
sed -i "s|  store: vercel({|  store: vercel({\n    region: \"$R\",|" "$APP/jazz.server.ts" && grep -n -A2 "vercel({" "$APP/jazz.server.ts"
