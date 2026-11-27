// Sum source-map-explorer results for a dir of chunks into packages, scaled to real gzip.
const fs=require("fs"),z=require("zlib"); const [dir, prefix, only] = process.argv.slice(2); const tot={}; const per={};
for (const f of fs.readdirSync("/tmp").filter(f=>f.startsWith(prefix))) {
  let j; try{j=JSON.parse(fs.readFileSync("/tmp/"+f))}catch{continue}; const r=j.results?.[0]; if(!r) continue;
  const js=f.slice(prefix.length,-5); if (only && !only.split(",").includes(js)) continue;
  const gz=z.gzipSync(fs.readFileSync(dir+"/"+js),{level:9}).length; const raw=r.totalBytes;
  for (const [file,v] of Object.entries(r.files)) { const m=file.match(/node_modules\/(?:\.pnpm\/[^/]+\/node_modules\/)?((?:@[^/]+\/)?[^/]+)\/(.*)/); let k=m?m[1]:"app/other"; if(k==="jazz-tools"){k="jazz-tools/"+m[2].replace(/^dist\//,"").split("/")[0]} tot[k]=(tot[k]||0)+gz*v.size/raw; if(/jazz|noble|opentelemetry/.test(k)) per[js]=(per[js]||0)+gz*v.size/raw; }
}
const e=Object.entries(tot).sort((a,b)=>b[1]-a[1]); for(const [k,v] of e.slice(0,25)) console.log((v/1024).toFixed(1).padStart(7),k);
console.log("jazz-ish per chunk:", Object.entries(per).sort((a,b)=>b[1]-a[1]).map(([k,v])=>k+" "+(v/1024).toFixed(1)).join(", "));
