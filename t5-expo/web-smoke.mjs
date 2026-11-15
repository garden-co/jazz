import { chromium } from 'playwright';
const b=await chromium.launch(); const p=await b.newPage(); const logs=[];
p.on('console',m=>logs.push(m.type()+': '+m.text().slice(0,300))); p.on('pageerror',e=>logs.push('pageerror: '+e.message.slice(0,300)));
await p.goto('http://localhost:7802/'); await p.waitForTimeout(8000);
console.log('UI:', await p.innerText('body')); console.log(logs.filter(l=>/jazz|Jazz|error/i.test(l)).slice(0,6).join('\n')); await b.close();
