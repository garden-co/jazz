#!/usr/bin/env node
// Bytes per package in a built chunk, from its sourcemap: node bundle-size.mjs <dist>/assets/index-*.js
// (build with: VITE_JAZZ_THIN=1 vite build --sourcemap --outDir /tmp/thin-sm)
import fs from "node:fs";
const f=process.argv[2]; const code=fs.readFileSync(f,"utf8"); const map=JSON.parse(fs.readFileSync(f+".map","utf8"));
// decode VLQ mappings, attribute bytes per source
const B64="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const lines=code.split("\n"); const sizes=new Map(); let src=0;
map.mappings.split(";").forEach((ln,li)=>{let col=0;const segs=ln?ln.split(","):[];const L=lines[li]??"";let prev=null;
 for(const s of segs){let vals=[],v=0,sh=0;for(const ch of s){let d=B64.indexOf(ch);v+=(d&31)<<sh;if(d&32){sh+=5}else{vals.push(v&1?-(v>>>1):v>>>1);v=0;sh=0}}
  col+=vals[0]; if(prev) sizes.set(prev.s,(sizes.get(prev.s)||0)+col-prev.c); if(vals.length>1){src+=vals[1];prev={s:src,c:col}} else prev=null;}
 if(prev) sizes.set(prev.s,(sizes.get(prev.s)||0)+L.length-prev.c);});
const agg=new Map(); for(const [s,n] of sizes){let p=map.sources[s].replace(/.*node_modules\//,"").replace(/\.pnpm\/[^/]+\/node_modules\//,""); let key=p.split("/").slice(0,p.startsWith("@")?2:1).join("/"); if(key==="jazz-tools") key=p.split("/").slice(0,3).join("/"); agg.set(key,(agg.get(key)||0)+n);}
[...agg].sort((a,b)=>b[1]-a[1]).slice(0,40).forEach(([k,n])=>console.log(String(Math.round(n/1024)).padStart(6),"KB",k));
