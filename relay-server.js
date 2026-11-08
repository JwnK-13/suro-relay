/*
 * SURO CUP — server relay (tanpa dependency, cukup Node.js 18+)
 *
 * Fungsi: perantara pesan (publish/subscribe) lewat WebSocket. Game memakainya untuk
 * pencarian room + cadangan jika koneksi langsung antar pemain (P2P) gagal.
 *
 * Cara pakai:
 *   1) Deploy file ini ke layanan yang mendukung WebSocket dan punya server di dekat pemain
 *      (mis. Render.com, Railway, Fly.io -> pilih region SINGAPURA). Perintah start: node relay-server.js
 *      Platform akan memberi alamat https://namamu.onrender.com -> pakai versi wss:  wss://namamu.onrender.com
 *   2) Di SURO_CUP_Simulator_2026.html, cari baris:   const RELAY_URL='';
 *      lalu isi:                                     const RELAY_URL='wss://namamu.onrender.com';
 *   3) Deploy ulang website. Selesai.
 *
 * Catatan: paket gratis kadang "tidur" saat sepi (butuh beberapa detik untuk bangun).
 */
const http=require('http'),crypto=require('crypto');
const PORT=process.env.PORT||8080,MAXMSG=1<<20;
const topics=new Map(); // topic -> Set(socket)
const server=http.createServer((q,r)=>{r.writeHead(200,{'Content-Type':'text/plain'});r.end('SURO CUP relay OK\n')});

function frame(op,buf){const n=buf.length;let h;if(n<126)h=Buffer.from([0x80|op,n]);else if(n<65536){h=Buffer.alloc(4);h[0]=0x80|op;h[1]=126;h.writeUInt16BE(n,2)}else{h=Buffer.alloc(10);h[0]=0x80|op;h[1]=127;h.writeBigUInt64BE(BigInt(n),2)}return Buffer.concat([h,buf])}
function sendText(s,str){if(!s.destroyed&&s.writable)s.write(frame(1,Buffer.from(str)))}
function publish(topic,payload,from){const set=topics.get(topic);if(!set)return;const msg=JSON.stringify({op:'msg',topic,payload});set.forEach(c=>{if(c!==from)sendText(c,msg)})}
function sub(s,t){if(!topics.has(t))topics.set(t,new Set());topics.get(t).add(s);s.subs.add(t)}
function cleanup(s){if(s.closed)return;s.closed=true;clearInterval(s.pt);s.subs.forEach(t=>{const set=topics.get(t);if(set){set.delete(s);if(!set.size)topics.delete(t)}});if(s.will)publish(s.will.topic,s.will.payload,s);s.destroy()}
function handle(s,text){let m;try{m=JSON.parse(text)}catch(e){return}
 if(m.op==='sub'&&Array.isArray(m.topics))m.topics.slice(0,20).forEach(t=>typeof t==='string'&&t.length<200&&sub(s,t));
 else if(m.op==='pub'&&typeof m.topic==='string'&&typeof m.payload==='string')publish(m.topic,m.payload,s);
 else if(m.op==='will'&&typeof m.topic==='string')s.will={topic:m.topic,payload:String(m.payload||'')}}

server.on('upgrade',(req,s)=>{
 const key=req.headers['sec-websocket-key'];if(!key){s.destroy();return}
 const acc=crypto.createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
 s.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+acc+'\r\n\r\n');
 s.subs=new Set();s.will=null;s.closed=false;s.setNoDelay(true);let buf=Buffer.alloc(0),frag=null;
 s.pt=setInterval(()=>{if(s.writable)s.write(frame(9,Buffer.alloc(0)))},25000);
 s.on('data',d=>{buf=Buffer.concat([buf,d]);
  for(;;){if(buf.length<2)break;const fin=buf[0]&0x80,op=buf[0]&15,masked=buf[1]&0x80;let len=buf[1]&0x7f,off=2;
   if(len===126){if(buf.length<4)break;len=buf.readUInt16BE(2);off=4}else if(len===127){if(buf.length<10)break;len=Number(buf.readBigUInt64BE(2));off=10}
   if(len>MAXMSG){cleanup(s);return}const need=off+(masked?4:0)+len;if(buf.length<need)break;
   let p=buf.slice(off+(masked?4:0),need);if(masked){const k=buf.slice(off,off+4);p=Buffer.from(p);for(let i=0;i<p.length;i++)p[i]^=k[i&3]}buf=buf.slice(need);
   if(op===8){s.write(frame(8,Buffer.alloc(0)));cleanup(s);return}
   if(op===9){s.write(frame(10,p));continue}if(op===10)continue;
   if(op===1||op===0){frag=op===1?[p]:(frag||[]).concat([p]);if(fin){handle(s,Buffer.concat(frag).toString());frag=null}}}});
 s.on('close',()=>cleanup(s));s.on('error',()=>cleanup(s));
});
server.listen(PORT,()=>console.log('SURO CUP relay jalan di port',PORT));
