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
// Texte in allen App-Sprachen (unbekannte Sprache → Englisch)
const T10: Record<string, {daily: [string, string], review: [string, string], test: string, news: [string, string]}> = {
  de: {daily: ['Zeit für deinen Tages-Check', '{t} – Wenn du gleich an die Charts gehst, dann zuerst NoTilt öffnen.'], review: ['Dein Wochenrückblick', '2 Minuten: Was hat diese Woche funktioniert? Wähl ein Ziel für die nächste.'], test: 'Benachrichtigungen funktionieren ✓', news: ['⚠ News in {m} Min.', '{t} – Trade vorher fertig? Stop steht?']},
  en: {daily: ['Time for your daily check', '{t} – When you open the charts, open NoTilt first.'], review: ['Your weekly review', '2 minutes: what worked this week? Pick one goal for the next.'], test: 'Notifications are working ✓', news: ['⚠ News in {m} min', '{t} – Trade done before? Stop set?']},
  es: {daily: ['Hora de tu check diario', '{t} – Antes de abrir los gráficos, abre primero NoTilt.'], review: ['Tu repaso semanal', '2 minutos: ¿qué funcionó esta semana? Elige un objetivo para la próxima.'], test: 'Las notificaciones funcionan ✓', news: ['⚠ Noticias en {m} min', '{t} – ¿Trade cerrado antes? ¿Stop puesto?']},
  pt: {daily: ['Hora do seu check diário', '{t} – Antes de abrir os gráficos, abra o NoTilt primeiro.'], review: ['Sua revisão semanal', '2 minutos: o que funcionou nesta semana? Escolha uma meta para a próxima.'], test: 'As notificações estão funcionando ✓', news: ['⚠ Notícia em {m} min', '{t} – Trade fechado antes? Stop definido?']},
  fr: {daily: ["C'est l'heure de ton check du jour", "{t} – Avant d'ouvrir les graphiques, ouvre d'abord NoTilt."], review: ['Ton bilan de la semaine', "2 minutes : qu'est-ce qui a marché cette semaine ? Choisis un objectif pour la suivante."], test: 'Les notifications fonctionnent ✓', news: ['⚠ News dans {m} min', '{t} – Trade fini avant ? Stop en place ?']},
  tr: {daily: ['Günlük kontrol zamanı', "{t} – Grafikleri açmadan önce NoTilt'i aç."], review: ['Haftalık değerlendirmen', '2 dakika: Bu hafta ne işe yaradı? Gelecek hafta için bir hedef seç.'], test: 'Bildirimler çalışıyor ✓', news: ['⚠ Haber {m} dk sonra', '{t} – Trade öncesinde bitti mi? Stop yerinde mi?']},
  ru: {daily: ['Время для дневной проверки', '{t} – Прежде чем открыть графики, сначала открой NoTilt.'], review: ['Твой обзор недели', '2 минуты: что сработало на этой неделе? Выбери цель на следующую.'], test: 'Уведомления работают ✓', news: ['⚠ Новости через {m} мин', '{t} – Сделка закрыта до этого? Стоп стоит?']},
  zh: {daily: ['该做每日检查了', '{t} —— 打开图表之前,先打开 NoTilt。'], review: ['你的每周回顾', '2 分钟:这周什么有效?为下周定一个目标。'], test: '通知已正常工作 ✓', news: ['⚠ 新闻 {m} 分钟后', '{t} —— 交易会在之前结束吗?止损设好了吗?']},
  ja: {daily: ['デイリーチェックの時間です', '{t} – チャートを開く前に、まずNoTiltを開こう。'], review: ['週間レビュー', '2分で:今週うまくいったことは?来週の目標をひとつ決めよう。'], test: '通知は正常に動作しています ✓', news: ['⚠ ニュースまで {m} 分', '{t} – その前にトレードを終える?ストップは置いた?']},
  ko: {daily: ['데일리 체크 시간이에요', '{t} – 차트를 열기 전에 먼저 NoTilt를 여세요.'], review: ['주간 리뷰', '2분: 이번 주에 무엇이 효과적이었나요? 다음 주 목표를 하나 정하세요.'], test: '알림이 정상 작동합니다 ✓', news: ['⚠ 뉴스까지 {m} 분', '{t} – 그 전에 트레이드 끝나? 손절 설정했어?']}
};
const langOf = (l: unknown) => (typeof l === 'string' && T10[l]) ? l : 'en';
const fill = (s: string, v: Record<string, string | number>) => s.replace(/\{(\w)\}/g, (_, k) => String(v[k] ?? ''));
const TXT = {
  daily: (l: string, t: string) => ({title: T10[l].daily[0], body: fill(T10[l].daily[1], {t})}),
  review: (l: string) => ({title: T10[l].review[0], body: T10[l].review[1]}),
  test: (l: string) => ({title: 'NoTilt', body: T10[l].test}),
  news: (l: string, m: number, t: string) => ({title: fill(T10[l].news[0], {m}), body: fill(T10[l].news[1], {t})})
};
// Wirtschaftskalender (gleiche Datei wie in der App) – 10 Min. zwischengespeichert
const CAL_URL = APP + 'data/calendar.json';
let CAL: {at: number, groups: {ts: number, titles: string}[]} = {at: 0, groups: []};
async function calGroups(){
  if (Date.now() - CAL.at < 10 * 60000) return CAL.groups;
  try {
    const j = await fetch(CAL_URL + '?t=' + Math.floor(Date.now() / 600000)).then(r => r.json());
    const ev = (j.events || []).filter((e: any) => e.cur === 'USD' && e.imp === 'High').map((e: any) => ({ts: Date.parse(e.t), title: String(e.title || '')}))
      .filter((e: any) => isFinite(e.ts)).sort((a: any, b: any) => a.ts - b.ts);
    const g: {ts: number, titles: string}[] = [];
    for (const e of ev){ const l = g[g.length - 1]; if (l && l.ts === e.ts) l.titles += ' · ' + e.title; else g.push({ts: e.ts, titles: e.title}); }
    CAL = {at: Date.now(), groups: g};
  } catch (_) { CAL.at = Date.now() - 8 * 60000; }   // Fehler: in 2 Min. erneut versuchen
  return CAL.groups;
}

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
    /* Nächster wichtiger Termin in 20–31 Min.? (läuft jede Minute, einmal pro Termin und Gerät) */
    const soon = (rows || []).some((r: any) => r.news === true) ? (await calGroups()).find(g => g.ts - Date.now() > 20 * 60000 && g.ts - Date.now() <= 31 * 60000) : null;
    for (const r of rows || []){
      const L = localNow(r.tz || 'Europe/Berlin'), lang = langOf(r.lang);
      const tm = toMin(r.remind_time);
      if (tm >= 0 && (r.remind_days || []).includes(L.wd) && L.min >= tm && L.min < tm + 15 && r.last_daily !== L.date){
        await sb.from('push_subs').update({last_daily: L.date}).eq('id', r.id);
        if (await deliver(r, {...TXT.daily(lang, r.remind_time), url: APP + '?from=push', tag:'daily'}) === 201) sent++;
      }
      if (soon && r.news === true && r.last_news !== String(soon.ts)){
        await sb.from('push_subs').update({last_news: String(soon.ts)}).eq('id', r.id);
        const m = Math.round((soon.ts - Date.now()) / 60000), t = soon.titles.length > 110 ? soon.titles.slice(0, 107) + '…' : soon.titles;
        if (await deliver(r, {...TXT.news(lang, m, t), url: APP + '?from=news', tag: 'news'}) === 201) sent++;
      }
      if (r.review && L.wd === 7 && L.min >= 18*60 && L.min < 18*60 + 15 && r.last_review !== L.date){
        await sb.from('push_subs').update({last_review: L.date}).eq('id', r.id);
        if (await deliver(r, {...TXT.review(lang), url: APP + '?view=review', tag:'review'}) === 201) sent++;
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
      tz: String(body.tz || 'Europe/Berlin').slice(0, 64), lang: langOf(body.lang)};
    let { error } = await sb.from('push_subs').upsert({...row, news: body.news !== false}, {onConflict: 'endpoint'});
    if (error && /news/.test(error.message || '')) ({ error } = await sb.from('push_subs').upsert(row, {onConflict: 'endpoint'}));   // Spalte noch nicht angelegt
    return error ? json({error: error.message}, 500) : json({ok: true});
  }
  if (action === 'unsubscribe'){
    await sb.from('push_subs').delete().eq('user_id', user.id).eq('endpoint', String(body.endpoint || ''));
    return json({ok: true});
  }
  if (action === 'test'){
    const { data: rows } = await sb.from('push_subs').select('*').eq('user_id', user.id);
    const res = [];
    for (const r of rows || []) res.push(await deliver(r, {...TXT.test(langOf(r.lang)), url: APP, tag: 'test'}));
    return json({ok: true, results: res});
  }
  return json({error: 'unknown action'}, 400);
});
