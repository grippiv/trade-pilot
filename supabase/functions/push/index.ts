// Web Push (RFC 8291 aes128gcm + RFC 8292 VAPID) nur mit WebCrypto – läuft in Deno (Supabase) und Node 20+
const enc = new TextEncoder();
const b64u = {
  enc(buf){ let s = ''; const a = new Uint8Array(buf); for (let i = 0; i < a.length; i++) s += String.fromCharCode(a[i]); return btoa(s).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,''); },
  dec(str){ str = str.replace(/-/g,'+').replace(/_/g,'/'); while (str.length % 4) str += '='; const b = atob(str), a = new Uint8Array(b.length); for (let i = 0; i < b.length; i++) a[i] = b.charCodeAt(i); return a; }
};
const cat = (...arrs) => { const n = arrs.reduce((s, a) => s + a.length, 0), out = new Uint8Array(n); let o = 0; for (const a of arrs){ out.set(a, o); o += a.length; } return out; };
async function hkdf(salt, ikm, info, len){
  const k = await crypto.subtle.importKey('raw', ikm, 'HKDF', false, ['deriveBits']);
  return new Uint8Array(await crypto.subtle.deriveBits({name:'HKDF', hash:'SHA-256', salt, info}, k, len*8));
}
async function generateVapid(){
  const kp = await crypto.subtle.generateKey({name:'ECDSA', namedCurve:'P-256'}, true, ['sign', 'verify']);
  const pub = new Uint8Array(await crypto.subtle.exportKey('raw', kp.publicKey));
  const jwk = await crypto.subtle.exportKey('jwk', kp.privateKey);
  return {pub: b64u.enc(pub), jwk};
}
async function vapidHeader(endpoint, vapid, subject){
  const key = await crypto.subtle.importKey('jwk', vapid.jwk, {name:'ECDSA', namedCurve:'P-256'}, false, ['sign']);
  const head = b64u.enc(enc.encode(JSON.stringify({typ:'JWT', alg:'ES256'})));
  const body = b64u.enc(enc.encode(JSON.stringify({aud: new URL(endpoint).origin, exp: Math.floor(Date.now()/1000) + 12*3600, sub: subject})));
  const sig = await crypto.subtle.sign({name:'ECDSA', hash:'SHA-256'}, key, enc.encode(head + '.' + body));
  return `vapid t=${head}.${body}.${b64u.enc(sig)}, k=${vapid.pub}`;
}
async function encryptPayload(sub, payload){
  const uaPub = b64u.dec(sub.p256dh), authSecret = b64u.dec(sub.auth);
  const as = await crypto.subtle.generateKey({name:'ECDH', namedCurve:'P-256'}, true, ['deriveBits']);
  const asPub = new Uint8Array(await crypto.subtle.exportKey('raw', as.publicKey));
  const uaKey = await crypto.subtle.importKey('raw', uaPub, {name:'ECDH', namedCurve:'P-256'}, false, []);
  const ecdh = new Uint8Array(await crypto.subtle.deriveBits({name:'ECDH', public: uaKey}, as.privateKey, 256));
  const ikm = await hkdf(authSecret, ecdh, cat(enc.encode('WebPush: info\0'), uaPub, asPub), 32);
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const cek = await hkdf(salt, ikm, enc.encode('Content-Encoding: aes128gcm\0'), 16);
  const nonce = await hkdf(salt, ikm, enc.encode('Content-Encoding: nonce\0'), 12);
  const key = await crypto.subtle.importKey('raw', cek, 'AES-GCM', false, ['encrypt']);
  const plain = cat(typeof payload === 'string' ? enc.encode(payload) : payload, new Uint8Array([2]));
  const ct = new Uint8Array(await crypto.subtle.encrypt({name:'AES-GCM', iv: nonce}, key, plain));
  const rs = new Uint8Array([0, 0, 16, 0]);   // Record-Größe 4096
  return cat(salt, rs, new Uint8Array([asPub.length]), asPub, ct);
}
async function sendPush(sub, payload, vapid, subject){
  const body = await encryptPayload(sub, JSON.stringify(payload));
  const res = await fetch(sub.endpoint, {method:'POST', headers:{
    'Authorization': await vapidHeader(sub.endpoint, vapid, subject), 'Content-Encoding':'aes128gcm',
    'Content-Type':'application/octet-stream', 'TTL':'21600', 'Urgency':'normal'}, body});
  return res.status;
}

// ---------------------------------------------------------------------------
// NoTilt – Erinnerungen per Push
// ---------------------------------------------------------------------------
import { createClient } from 'npm:@supabase/supabase-js@2';
const sb = createClient(Deno.env.get('SUPABASE_URL')!, Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!, {auth:{persistSession:false}});
const SUBJECT = 'https://grippiv.github.io/trade-pilot/';
const APP = 'https://grippiv.github.io/trade-pilot/';
const CORS = {'Access-Control-Allow-Origin':'*', 'Access-Control-Allow-Headers':'authorization, x-client-info, apikey, content-type', 'Access-Control-Allow-Methods':'GET, POST, OPTIONS'};
const json = (d: unknown, s = 200) => new Response(JSON.stringify(d), {status: s, headers: {...CORS, 'Content-Type':'application/json'}});

let VAPID: {pub: string, jwk: JsonWebKey} | null = null;
async function vapid(){
  if (VAPID) return VAPID;
  const { data } = await sb.from('push_keys').select('pub, priv_jwk').eq('id', 1).maybeSingle();
  if (data){ VAPID = {pub: data.pub, jwk: data.priv_jwk}; return VAPID; }
  const k = await generateVapid();                       // einmalig erzeugt, bleibt nur in der Datenbank
  await sb.from('push_keys').upsert({id: 1, pub: k.pub, priv_jwk: k.jwk});
  const again = await sb.from('push_keys').select('pub, priv_jwk').eq('id', 1).single();
  VAPID = {pub: again.data!.pub, jwk: again.data!.priv_jwk}; return VAPID;
}
const TXT = {
  de: { daily: (t: string) => ({title:'Zeit für deinen Tages-Check', body:`${t} – Wenn du gleich an die Charts gehst, dann zuerst NoTilt öffnen.`}),
        review: {title:'Dein Wochenrückblick', body:'2 Minuten: Was hat diese Woche funktioniert? Wähl ein Ziel für die nächste.'},
        test: {title:'NoTilt', body:'Benachrichtigungen funktionieren ✓'} },
  en: { daily: (t: string) => ({title:'Time for your daily check', body:`${t} – When you open the charts, open NoTilt first.`}),
        review: {title:'Your weekly review', body:'2 minutes: what worked this week? Pick one goal for the next.'},
        test: {title:'NoTilt', body:'Notifications are working ✓'} }
};
function localNow(tz: string){
  const p = Object.fromEntries(new Intl.DateTimeFormat('en-CA', {timeZone: tz, year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit', weekday:'short', hour12:false})
    .formatToParts(new Date()).map(x => [x.type, x.value]));
  const wd = {Mon:1, Tue:2, Wed:3, Thu:4, Fri:5, Sat:6, Sun:7}[p.weekday as string] || 1;
  return {date: `${p.year}-${p.month}-${p.day}`, min: (+p.hour % 24)*60 + +p.minute, wd};
}
const toMin = (t: string) => { const [h, m] = String(t||'').split(':').map(Number); return isFinite(h) ? h*60 + (m||0) : -1; };
async function deliver(row: any, payload: unknown){
  try {
    const st = await sendPush({endpoint: row.endpoint, p256dh: row.p256dh, auth: row.auth}, payload, await vapid(), SUBJECT);
    if (st === 404 || st === 410) await sb.from('push_subs').delete().eq('id', row.id);   // Abo abgelaufen
    return st;
  } catch (e) { return 0; }
}
async function userFrom(req: Request){
  const tok = (req.headers.get('Authorization') || '').replace(/^Bearer\s+/i, '');
  if (!tok) return null;
  const { data } = await sb.auth.getUser(tok);
  return data && data.user ? data.user : null;
}
Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response('ok', {headers: CORS});
  if (req.method === 'GET') return json({pub: (await vapid()).pub});
  let body: any = {}; try { body = await req.json(); } catch (_) {}
  const action = body.action;

  if (action === 'tick'){
    const { data: rows } = await sb.from('push_subs').select('*').limit(5000);
    let sent = 0;
    for (const r of rows || []){
      const L = localNow(r.tz || 'Europe/Berlin'), lang = r.lang === 'en' ? 'en' : 'de';
      const tm = toMin(r.remind_time);
      if (tm >= 0 && (r.remind_days || []).includes(L.wd) && L.min >= tm && L.min < tm + 15 && r.last_daily !== L.date){
        await sb.from('push_subs').update({last_daily: L.date}).eq('id', r.id);
        if (await deliver(r, {...TXT[lang].daily(r.remind_time), url: APP + '?from=push', tag:'daily'}) === 201) sent++;
      }
      if (r.review && L.wd === 7 && L.min >= 18*60 && L.min < 18*60 + 15 && r.last_review !== L.date){
        await sb.from('push_subs').update({last_review: L.date}).eq('id', r.id);
        if (await deliver(r, {...TXT[lang].review, url: APP + '?view=review', tag:'review'}) === 201) sent++;
      }
    }
    return json({ok: true, sent});
  }

  const user = await userFrom(req);
  if (!user) return json({error: 'not authenticated'}, 401);

  if (action === 'subscribe'){
    const s = body.sub || {}, k = s.keys || {};
    if (!s.endpoint || !k.p256dh || !k.auth || !/^https:\/\//.test(s.endpoint)) return json({error: 'bad subscription'}, 400);
    const days = Array.isArray(body.days) ? body.days.map(Number).filter((d: number) => d >= 1 && d <= 7) : [1,2,3,4,5];
    const row = {user_id: user.id, endpoint: s.endpoint, p256dh: k.p256dh, auth: k.auth,
      remind_time: /^\d{2}:\d{2}$/.test(body.time || '') ? body.time : null, remind_days: days, review: body.review !== false,
      tz: String(body.tz || 'Europe/Berlin').slice(0, 64), lang: body.lang === 'en' ? 'en' : 'de'};
    const { error } = await sb.from('push_subs').upsert(row, {onConflict: 'endpoint'});
    return error ? json({error: error.message}, 500) : json({ok: true});
  }
  if (action === 'unsubscribe'){
    await sb.from('push_subs').delete().eq('user_id', user.id).eq('endpoint', String(body.endpoint || ''));
    return json({ok: true});
  }
  if (action === 'test'){
    const { data: rows } = await sb.from('push_subs').select('*').eq('user_id', user.id);
    const res = [];
    for (const r of rows || []) res.push(await deliver(r, {...TXT[r.lang === 'en' ? 'en' : 'de'].test, url: APP, tag: 'test'}));
    return json({ok: true, results: res});
  }
  return json({error: 'unknown action'}, 400);
});
