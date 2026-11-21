#!/usr/bin/env bash
# Add `region` to the app's vercel({ ... }) call. usage: set-region.sh <app-dir> [region]
APP=$1; R=${2:-iad1}
grep -q 'region:' "$APP/jazz.server.ts" && { echo "region already set"; exit 0; }
sed -i "s|  store: vercel({|  store: vercel({\n    region: \"$R\",|" "$APP/jazz.server.ts" && grep -n -A2 "vercel({" "$APP/jazz.server.ts"
