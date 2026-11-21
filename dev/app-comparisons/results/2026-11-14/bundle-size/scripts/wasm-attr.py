# Attribute WASM code-section bytes to Rust crates via the name section.
import sys,re,collections
b=open(sys.argv[1],'rb').read()
def leb(i):
  r=s=0
  while True:
    x=b[i]; i+=1; r|=(x&0x7f)<<s; s+=7
    if x<0x80: return r,i
i=8; secs={}; nimports=0
while i<len(b):
  sid=b[i]; sz,j=leb(i+1)
  if sid==0:
    l,k=leb(j); secs['c:'+b[k:k+l].decode()]=(k+l,j+sz)
  else: secs[sid]=(j,j+sz)
  i=j+sz
# count function imports
p,e=secs[2]; n,p=leb(p)
for _ in range(n):
  l,p=leb(p); p+=l; l,p=leb(p); p+=l; kind=b[p]; p+=1
  if kind==0: _,p=leb(p); nimports+=1
  elif kind==1: p+=1; f=b[p]; p+=1; _,p=leb(p); 
  if kind==1 and f&1: _,p=leb(p)
  elif kind==2:
    f=b[p]; p+=1; _,p=leb(p)
    if f&1: _,p=leb(p)
  elif kind==3: p+=2
names={}
p,e=secs['c:name']
while p<e:
  sub=b[p]; sz,q=leb(p+1)
  if sub==1:
    n,r=leb(q)
    for _ in range(n):
      idx,r=leb(r); l,r=leb(r); names[idx]=b[r:r+l].decode('utf8','replace'); r+=l
  p=q+sz
p,e=secs[10]; n,p=leb(p); agg=collections.Counter(); fns=[]
for k in range(n):
  sz,q=leb(p); nm=names.get(nimports+k,'?'); p=q+sz
  s=nm
  s=re.sub(r'^<','',s)
  m=re.match(r'([A-Za-z0-9_]+)::',s)
  crate=m.group(1) if m else ('sqlite(C)' if re.match(r'^sqlite3|^sqlite',nm) else nm.split('::')[0][:30] if '::' in nm else 'other/C')
  agg[crate]+=sz; fns.append((sz,nm))
tot=sum(agg.values()); print('code bytes',tot)
for c,v in agg.most_common(int(sys.argv[2]) if len(sys.argv)>2 else 30): print(f'{v/1024:9.1f} KB {100*v/tot:5.1f}%  {c}')
print('--- largest functions')
for sz,nm in sorted(fns,reverse=True)[:15]: print(f'{sz/1024:8.1f} KB  {nm[:140]}')
if len(sys.argv)>3:
  for pat in sys.argv[3].split(','):
    t=sum(sz for sz,nm in fns if re.search(pat,nm)); c=sum(1 for sz,nm in fns if re.search(pat,nm))
    print(f'pattern {pat!r}: {t/1024:.1f} KB in {c} fns')
