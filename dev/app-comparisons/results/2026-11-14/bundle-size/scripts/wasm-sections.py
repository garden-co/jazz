import sys
b=open(sys.argv[1],'rb').read(); i=8
def leb(i):
  r=s=0
  while True:
    x=b[i]; i+=1; r|=(x&0x7f)<<s; s+=7
    if x<0x80: return r,i
names={0:'custom',1:'type',2:'import',3:'func',4:'table',5:'memory',6:'global',7:'export',8:'start',9:'elem',10:'code',11:'data',12:'datacount'}
while i<len(b):
  sid=b[i]; sz,j=leb(i+1); nm=names.get(sid)
  if sid==0:
    l,k=leb(j); nm='custom:'+b[k:k+l].decode()
  print(f'{sz/1024:10.1f} KB  {nm}'); i=j+sz
