/* ------------------------------------------------------------------
   Speicher: lokal (sofort, offline) + Supabase (Konto & Sync)
------------------------------------------------------------------ */
const LS = {
  get(k, d){ try{ const v = localStorage.getItem('tp_'+k); return v===null ? d : JSON.parse(v); }catch(e){ return d; } },
  set(k, v){ try{ localStorage.setItem('tp_'+k, JSON.stringify(v)); }catch(e){} }
};
const STATE_KEYS = ['cfg','active','cooldownUntil','lastBias','stops','resetAt','lastStrat'];
const cache = {};
STATE_KEYS.forEach(k => cache[k] = LS.get(k, null));
cache.journal = (LS.get('journal', []) || []).map(j => Object.assign({id: 'j' + Date.parse(j.date)}, j));
const S = {
  get(k, d){ const v = cache[k]; return (v===null || v===undefined) ? d : v; },
  set(k, v){ cache[k] = v; LS.set(k, v); if (STATE_KEYS.includes(k)) Sync.pushState(); }
};
const DEFAULT_CHECKS = [
  'Das Setup entspricht genau meiner getesteten Strategie',
  'Der Kurs ist jetzt in meiner Zone – nicht schon weit weg',
  'Bestätigungssignal ist da – nach Kerzenschluss, nicht davor',
  'Der höhere Zeitrahmen spricht nicht gegen den Trade'
];
const DEFAULTS = {maxTrades:2, lossLimit:2, cooldownLoss:15, cooldownWin:5, risk:1, minCRV:2, konto:1000, hebel:5, newsWin:30, usOpenBlock:10,
  strats:['Fair Value Gap','Support & Resistance','Trendfolge'], checks: DEFAULT_CHECKS};
const freshDefaults = () => Object.assign({}, DEFAULTS, {strats: DEFAULTS.strats.slice(), checks: DEFAULTS.checks.slice()});
const cfg = Object.assign(freshDefaults(), S.get('cfg', {}));
const saveCfg = () => S.set('cfg', JSON.parse(JSON.stringify(cfg)));
let curView = 'home';

const TPC = window.TP_CONFIG || {};
const HAS_BACKEND = !!(TPC.SUPABASE_URL && TPC.SUPABASE_KEY && window.supabase && window.supabase.createClient);
const sb = HAS_BACKEND ? window.supabase.createClient(TPC.SUPABASE_URL, TPC.SUPABASE_KEY, {auth:{persistSession:true, autoRefreshToken:true, detectSessionInUrl:true}}) : null;

/* Warteschlange: Änderungen werden lokal gemerkt und hochgeladen, sobald Internet + Konto da sind */
const Sync = {
  uid:null, email:'', ok:false, t:null, writing:false, pending:false, lastWrite:0, draining:false, pullT:null, chan:null,
  dirty: LS.get('dirty', false), queue: LS.get('queue', []),
  saveQ(){ LS.set('queue', this.queue); },
  enqueue(op, id){ this.queue = this.queue.filter(q => q.id !== id); this.queue.push({op, id}); this.saveQ(); },
  pushState(){ this.dirty = true; LS.set('dirty', true); if (!this.ok) return; clearTimeout(this.t); this.t = setTimeout(() => this.flush(), 400); },
  async flush(){
    if (!this.ok) return;
    if (this.writing){ this.pending = true; return; }
    this.writing = true;
    const data = {}; STATE_KEYS.forEach(k => data[k] = cache[k] ?? null);
    const ts = Date.now(); this.lastWrite = ts;
    const { error } = await sb.from('tp_state').upsert({user_id:this.uid, data, updated_at:ts});
    if (error) badge('err'); else { this.dirty = false; LS.set('dirty', false); badge('ok'); }
    this.writing = false;
    if (this.pending){ this.pending = false; this.flush(); }
  },
  putTrade(t){ this.enqueue('put', t.id); this.drain(); },
  delTrades(ids){ ids.forEach(id => this.enqueue('del', id)); this.drain(); },
  async drain(){
    if (!this.ok || this.draining || !this.queue.length) return;
    this.draining = true; let did = false;
    while (this.queue.length){
      const q = this.queue[0]; let error = null;
      if (q.op === 'put'){
        const t = cache.journal.find(j => j.id === q.id);
        if (t) ({ error } = await sb.from('tp_trades').upsert({user_id:this.uid, id:t.id, data:t, updated_at:Date.now()}));
      } else {
        ({ error } = await sb.from('tp_trades').delete().eq('user_id', this.uid).eq('id', q.id));
      }
      if (error){ badge('err'); break; }
      this.queue.shift(); this.saveQ(); did = true;
    }
    this.draining = false;
    if (did) this.pushState();   /* stupst die anderen Geräte an */
  },
  async pull(){
    if (!this.ok) return;
    await this.drain();
    if (this.queue.length) return;          /* offline: lokale Version behalten */
    const st = await sb.from('tp_state').select('data,updated_at').eq('user_id', this.uid).maybeSingle();
    if (st.error){ badge('err'); return; }
    const rows = []; let from = 0;
    for (;;){
      const r = await sb.from('tp_trades').select('data').eq('user_id', this.uid).range(from, from+999);
      if (r.error){ badge('err'); return; }
      rows.push(...r.data.map(x => x.data)); if (r.data.length < 1000) break; from += 1000;
    }
    if (st.data && !this.dirty) applyState(st.data.data || {});
    else if (!st.data || this.dirty) await this.flush();
    cache.journal = rows; LS.set('journal', rows);
    badge('ok'); refreshView();
  },
  schedulePull(){ clearTimeout(this.pullT); this.pullT = setTimeout(() => this.pull(), 300); },
  listen(){
    if (this.chan) sb.removeChannel(this.chan);
    this.chan = sb.channel('tp-' + this.uid)
      .on('postgres_changes', {event:'*', schema:'public', table:'tp_state', filter:'user_id=eq.' + this.uid}, p => {
        if (p.new && p.new.updated_at === this.lastWrite) return;
        this.schedulePull();
      }).subscribe();
  },
  async connect(session){
    if (!session || !session.user){ this.ok = false; this.uid = null; badge(HAS_BACKEND ? 'out' : 'local'); return; }
    if (this.uid === session.user.id && this.ok) return;
    this.uid = session.user.id; this.email = session.user.email || ''; this.ok = true;
    badge('sync'); this.listen(); await this.pull();
  }
};
function badge(m){
  const el = document.getElementById('sync'); if (!el) return;
  const txt = {ok:'Synchron', err:'Offline', local:'Nur dieses Gerät', out:'Nicht angemeldet', sync:'Verbinde…'}[m] || 'Verbinde…';
  el.className = 'sync ' + (m==='ok' ? 'ok' : m==='err' ? 'err' : '');
  el.title = m==='ok' ? 'Synchron mit deinen anderen Geräten' : m==='err' ? 'Gerade keine Verbindung – Änderungen werden später hochgeladen' : txt;
  el.lastElementChild.textContent = txt;
}
function journalList(){ return cache.journal.slice().sort((a,b) => Date.parse(b.date) - Date.parse(a.date)); }
function addTrade(t){
  t.id = 'j' + Date.now() + Math.floor(Math.random()*1000);
  cache.journal.push(t); LS.set('journal', cache.journal); Sync.putTrade(t);
}
function updateTrade(t){
  const i = cache.journal.findIndex(j => j.id===t.id); if (i<0) return;
  cache.journal[i] = t; LS.set('journal', cache.journal); Sync.putTrade(t);
}
function removeTrade(id){
  cache.journal = cache.journal.filter(j => j.id!==id); LS.set('journal', cache.journal); Sync.delTrades([id]);
}
function applyState(d){
  STATE_KEYS.forEach(k => { cache[k] = d[k] ?? null; LS.set(k, cache[k]); });
  Object.keys(cfg).forEach(k => delete cfg[k]);
  Object.assign(cfg, freshDefaults(), cache.cfg || {});
}
function refreshView(){
  const m = document.getElementById('modal'); if (m && m.classList.contains('on')) return;
  if (curView==='home'){ stopTimers(); home(); }
  else if (curView==='journal'){ stopTimers(); journal(); }
  else if (curView==='intrade' && !cache.active){ stopTimers(); home(); }
  else if (curView==='onboard' && !needsOnboarding()){ stopTimers(); ob = null; home(); }
}
async function startSync(){
  if (!HAS_BACKEND){ badge('local'); return; }
  sb.auth.onAuthStateChange((ev, session) => {
    if (ev === 'PASSWORD_RECOVERY') return setTimeout(() => accountView('newpw'), 50);
    if (ev === 'SIGNED_OUT'){ Sync.connect(null); return; }
    if (session) setTimeout(() => Sync.connect(session), 0);
  });
  const { data } = await sb.auth.getSession();
  await Sync.connect(data.session);
  addEventListener('online', () => Sync.schedulePull());
  document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'visible') Sync.schedulePull(); });
}
const needsLogin = () => HAS_BACKEND && !LS.get('localMode', false);
/* ------------------------------------------------------------------
   Inhalte
------------------------------------------------------------------ */
const PHASES = ['Atmen','Kopf','Setup','Plan','Los'];
const FLOW = ['sigh','kopf','setup','plan','commit'];
const QUOTES = [
  '„Die Checkliste denkt für dich, wenn die Emotion übernehmen will.“',
  '„Ein einzelner Trade ist Zufall. Deine Edge zeigt sich erst über hunderte.“',
  '„Warte, bis das Setup zu dir kommt – nicht umgekehrt.“',
  '„Ein Verlust ist kein Urteil über dich, sondern ein Datenpunkt.“',
  '„Kein Trade ist auch eine Entscheidung.“',
  '„Wer Trading wie ein Business führt, misst seine Zahlen.“'
];
const STOPS = {
  overtrading:{t:'Langeweile ist kein Setup', x:'Du willst traden, weil du etwas tun willst – nicht, weil dein System ein Setup zeigt.',
    h:'Ein erzwungenes, mittelmäßiges Setup ist meistens der teuerste Trade des Tages.'},
  revenge:{t:'Revenge-Trading-Gefahr', x:'Nach Verlusten gehen Trader nachweislich mehr Risiko ein. Genau jetzt ist der schlechteste Moment für den nächsten Trade.',
    h:'Erst Abstand gewinnen: Laptop zu, kurz raus, dann mit klarem Kopf neu bewerten.', cool:true},
  fomo:{t:'FOMO erkannt', x:'Die Angst, etwas zu verpassen, macht dich fast immer zum Käufer kurz vor der Umkehr.',
    h:'FOMO ist ein alter Herden-Instinkt. Am Finanzmarkt macht er dich zum Käufer am Hoch.'},
  gier:{t:'Gier am Steuer', x:'„Der wird sicher groß“ führt zu zu großen Positionen und Setups, die dein System nie traden würde.',
    h:'Gier fühlt sich im Moment wie ein guter Instinkt an. Deshalb entscheidet die Checkliste, nicht das Gefühl.'},
  neutral:{t:'Bias heute: neutral', x:'Dein Tages-Check sagt: keine klare Richtung. Heute kein Trade – und das ist völlig in Ordnung.',
    h:'Keine klare Richtung ist ein vollkommen gültiges Ergebnis. Abwarten ist auch eine Position.'},
  gegenbias:{t:'Gegen deinen Bias', x:'Du willst gegen die Richtung deines Tages-Checks traden.',
    h:'Der Bias entscheidet, in welche Richtung du suchst. Hat sich der Markt klar gedreht, mach den Tages-Check neu – aber nicht mitten im Impuls.'},
  setup:{t:'Setup unvollständig', x:'Mindestens eine Bedingung deines Systems ist nicht erfüllt. Kein Trade.',
    h:'Nicht alles abgehakt heißt: kein Trade. Alarm setzen und warten.'},
  crv:{t:'Chance-Risiko zu schlecht', x:'Dein Ziel ist im Verhältnis zum Stop zu nah.', h:'Nur Trades mit klarem Mindest-CRV.'},
  liq:{t:'Liquidation vor dem Stop', x:'Mit diesem Hebel wirst du liquidiert, bevor dein Stop überhaupt greift.', h:'Weniger Hebel oder engerer Stop.'},
  limit:{t:'Trade-Limit für heute erreicht', x:'Du hast heute so viele Trades gemacht, wie du dir vorgenommen hast.',
    h:'Mehr Trades, als dein System hergibt, heißt: Du tradest Gefühle, nicht Setups.'},
  verlustlimit:{t:'Verlust-Limit für heute erreicht', x:'Du hast heute dein maximales Minus erreicht. Für heute ist Schluss.',
    h:'Nach Verlusten steigt die Risikobereitschaft – genau dann schützt dich eine harte Grenze.'},
  news:{t:'News-Fenster', x:'', h:'Bei News: erst 30–60 Minuten abwarten und beobachten.'},
  usopen:{t:'US-Börsenstart läuft', x:'', h:'Die ersten Minuten nach dem Börsenstart sind oft hektisch und täuschen. Erst beobachten, dann handeln.'}
};
const TEMPT = {
  stop:{t:'Stop weiter weg schieben?', x:'Das ist Verlustaversion: Du hältst Verlierer zu lange, in der Hoffnung „der dreht schon“.', h:'Du hast einen Plan. Jag deine Verluste nicht weiter ins Minus.'},
  frueh:{t:'Gewinne früher mitnehmen?', x:'Die Angst, den Buchgewinn zu verlieren, lässt dich Gewinner zu früh verkaufen. Über hunderte Trades kostet genau das deine Edge.', h:'Du hast ein Ziel – halte es.'},
  mehr:{t:'Position vergrößern?', x:'Das ist Gier. Dein Risiko war mit dem Plan festgelegt.', h:'Der Plan entscheidet, nicht das Gefühl.'},
  nervoes:{t:'Nervös am Chart?', x:'Handy weg oder Laptop zu. Preisalarme bei Ziel und Stop setzen. Kurzer Spaziergang.', h:'Deine Orders sind gesetzt – das reicht.'}
};
const REMIND = [
  'Du musst nichts tun. Stop und Ziel arbeiten für dich.',
  'Ein Verlust schmerzt doppelt so stark wie ein gleich großer Gewinn – deshalb will dein Kopf gleich Dummes tun. Lass ihn reden.',
  'Unrealisierter Gewinn ist nicht dein Geld. Halte dich an dein Ziel.',
  'Starrst du seit Minuten auf den Chart? Alarm setzen, Handy weg.',
  'Der Trade ist im Minus? Normal. Der Stop bleibt, wo er ist.'
];
const FEEL = [['ruhig','Ruhig & klar'],['fomo','Ungeduldig – will nichts verpassen'],['frust','Frust nach einem Verlust'],
  ['gier','Euphorisch – der wird groß'],['lw','Langeweile – will einfach traden'],['angst','Nervös / ängstlich']];
const FEEL_TXT = {ruhig:'Ruhig', angst:'Nervös'};
const checks = () => (Array.isArray(cfg.checks) && cfg.checks.length) ? cfg.checks : DEFAULT_CHECKS;
const RULES = [['bed','Bedingungen'],['ein','Einstieg'],['sl','Stop-Loss'],['tp','Ziel']];
const GRADES = [['S','Lehrbuch – alles glasklar'],['A','Gut – kleine Abstriche'],['B','Grenzwertig']];

let st = {}, fi = 0, timers = [];
let jTab = 'stats', jRange = '30', jStrat = 'alle';

/* ------------------------------------------------------------------
   Hilfen
------------------------------------------------------------------ */
const $ = s => document.querySelector(s);
const $$ = s => [...document.querySelectorAll(s)];
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const today = () => { const d = new Date(); return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0'); };
const isToday = iso => new Date(iso).toDateString() === new Date().toDateString();
const fmt = (n, d=2) => isFinite(n) ? Number(n).toLocaleString('de-DE',{minimumFractionDigits:d,maximumFractionDigits:d}) : '–';
const sgn = (n, d=2) => isFinite(n) ? (n>0?'+':'') + fmt(n,d) : '–';
const num = v => { let x = String(v ?? '').trim(); if (x.includes(',') && x.includes('.')) x = x.replace(/\./g,''); return parseFloat(x.replace(',', '.')); };
const mmss = ms => { const s = Math.ceil(ms/1000); return `${String(Math.floor(s/60)).padStart(2,'0')}:${String(s%60).padStart(2,'0')}`; };
const coinFmt = s => String(s||'').toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,12);
function berlin(){
  const p = new Intl.DateTimeFormat('de-DE',{timeZone:'Europe/Berlin',hour:'2-digit',minute:'2-digit',hour12:false}).formatToParts(new Date());
  return +p.find(x=>x.type==='hour').value*60 + +p.find(x=>x.type==='minute').value;
}
function session(){
  const t = berlin(), s = [];
  if (t>=60 && t<600) s.push('Asia'); if (t>=540 && t<1050) s.push('London'); if (t>=870 && t<1320) s.push('New York');
  return s.length ? s.join(' + ') : 'ruhig';
}
const nearUSOpen = () => { const t = berlin(); return t>=900 && t<=990; };
const todayTrades = () => cache.journal.filter(j => isToday(j.date));
const tradesToday = () => todayTrades().length;
const todayR = () => todayTrades().reduce((a,x) => a + (+x.R||0), 0);
const cooldownLeft = () => Math.max(0, S.get('cooldownUntil', 0) - Date.now());
const biasToday = () => { const b = S.get('lastBias', null); return b && b.date === today() ? b : null; };
const strats = () => strategies().map(s => s.name);
function newsBlock(){
  const b = biasToday(); if (!b || !b.newsTime) return null;
  const [h, m] = b.newsTime.split(':').map(Number); if (!isFinite(h)) return null;
  const diff = berlin() - (h*60+m);
  return (diff >= -cfg.newsWin && diff <= cfg.newsWin) ? b.newsTime : null;
}
function usOpenBlock(){ const t = berlin(); return cfg.usOpenBlock>0 && t>=930 && t<930+cfg.usOpenBlock; }
function recentCoins(){
  const out = [], b = biasToday();
  (b && b.fokus || []).forEach(c => out.includes(c) || out.push(c));
  journalList().slice(0,20).forEach(j => j.coin && !out.includes(j.coin) && out.push(j.coin));
  return out.slice(0,6);
}
function stopTimers(){ timers.forEach(clearInterval); timers.forEach(clearTimeout); timers = []; }
function render(html, cls=''){
  const stage = $('#stage'), old = stage.firstElementChild;
  const show = () => { stage.innerHTML = `<div class="view ${cls}">${html}</div>`; window.scrollTo({top:0,behavior:'smooth'}); };
  if (old){ old.classList.add('out'); setTimeout(show, 200); } else show();
}
const later = fn => setTimeout(fn, 230);
function progress(i){
  const p = $('#progress');
  if (i===null){ p.classList.remove('on'); return; }
  p.classList.add('on');
  $('#phases').innerHTML = PHASES.map((n,k)=>`<div class="phase ${k<i?'done':k===i?'act':''}"><div class="bar"><div class="fill" style="width:${k===i?55:0}%"></div></div><div class="lbl">${n}</div></div>`).join('');
}
function chipRow(id, items, selected, cls=''){
  return `<div class="chips ${cls}" id="${id}">${items.map(it => { const [v, l] = Array.isArray(it) ? it : [it, it];
    return `<button type="button" data-v="${esc(v)}" class="${v===selected?'on':''}">${esc(l)}</button>`; }).join('')}</div>`;
}
function bindChips(id, cb, multi=false){
  $$(`#${id} button`).forEach(b => b.onclick = () => {
    if (multi) b.classList.toggle('on'); else $$(`#${id} button`).forEach(x => x.classList.toggle('on', x===b));
    cb && cb(b.dataset.v, b);
  });
}
function go(v){ stopTimers(); closeModal(); ({home, journal, settings})[v](); }

/* ------------------------------------------------------------------
   Start
------------------------------------------------------------------ */
function home(){
  curView = 'home'; progress(null);
  if (S.get('active', null)) return inTrade();
  if (needsOnboarding()) return onboarding();
  const n = tradesToday(), cd = cooldownLeft(), r = todayR(), b = biasToday();
  const locked = cd>0 || n>=cfg.maxTrades || r <= -cfg.lossLimit;
  const biasChip = !b ? `<button class="chip warn" onclick="dayCheck()">Tages-Check <b>offen</b></button>`
    : `<button class="chip" onclick="dayCheck()">Bias <b style="color:${b.b==='long'?'var(--green)':b.b==='short'?'var(--red)':'var(--mut)'}">${b.b==='long'?'Long':b.b==='short'?'Short':'Neutral'}</b> ✎</button>`;
  render(`<div class="home">
    <h1>Bereit für einen Trade?</h1>
    <p class="sub">Unter einer Minute. Erst die Checkliste, dann der Trade.</p>
    <div class="bigwrap">
      ${locked?'':'<div class="ring"></div><div class="ring r2"></div><div class="ring r3"></div>'}
      <button class="bigbtn ${locked?'locked':''}" onclick="start()">TRADEN<small>${cd>0?'PAUSE':locked?'LIMIT':!b?'TAGES-CHECK':'CHECKLISTE'}</small></button>
    </div>
    <div class="stats">
      ${biasChip}
      <span class="chip ${n>=cfg.maxTrades?'warn':''}">Trades <b>${n}/${cfg.maxTrades}</b></span>
      <span class="chip ${r<0?'warn':''}">Heute <b>${sgn(r,1)} R</b></span>
      <span class="chip">Session <b>${session()}</b></span>
      ${b && b.newsTime ? `<span class="chip">News <b>${esc(b.newsTime)}</b></span>` : ''}
      ${cd>0?`<span class="chip warn">Pause <b id="cdh">${mmss(cd)}</b></span>`:''}
    </div>
    ${b && b.fokus && b.fokus.length ? `<p class="fokus">Fokus heute: ${b.fokus.map(c=>`<b>${esc(c)}</b>`).join(' · ')}</p>` : ''}
    <p class="quote" id="quote">${QUOTES[Math.floor(Math.random()*QUOTES.length)]}</p>
    <button class="linkbtn" onclick="addManual()">Trade ohne Checkliste nachtragen</button>
  </div>`);
  let qi = 0;
  timers.push(setInterval(()=>{ const q = $('#quote'); if(!q) return; q.style.opacity = 0;
    setTimeout(()=>{ qi=(qi+1)%QUOTES.length; q.textContent = QUOTES[qi]; q.style.opacity=1; }, 500); }, 7000));
  if (cd>0) timers.push(setInterval(()=>{ const c=$('#cdh'); if(!c) return; const l=cooldownLeft(); if(l<=0){ stopTimers(); home(); } else c.textContent=mmss(l); },1000));
}

function start(){
  stopTimers();
  if (needsOnboarding()) return onboarding();
  if (cooldownLeft()>0) return cooldownView();
  if (tradesToday()>=cfg.maxTrades) return stopView('limit');
  if (todayR() <= -cfg.lossLimit) return stopView('verlustlimit');
  const b = biasToday();
  if (!b) return dayCheck(true);
  if (b.b==='neutral') return stopView('neutral');
  const nb = newsBlock(); if (nb) return stopView('news', `News um ${nb} Uhr. Kein neuer Trade ${cfg.newsWin} Minuten davor und danach.`);
  if (usOpenBlock()) return stopView('usopen', `Die US-Börse hat um 15:30 Uhr geöffnet. In den ersten ${cfg.usOpenBlock} Minuten keine neuen Trades.`);
  const ls = S.get('lastStrat', ''); st = {dir: b.b, risk: cfg.risk, strat: strats().includes(ls) ? ls : strats()[0]}; fi = 0; step();
}
function step(){ stopTimers(); curView = 'flow'; progress(fi); ({sigh, kopf, setup, plan, commit})[FLOW[fi]](); }
function next(){ fi++; step(); }

/* ------------------------------------------------------------------
   Tages-Check (einmal pro Tag) – Routine vor dem Handelstag
------------------------------------------------------------------ */
function dayCheck(thenStart){
  stopTimers(); curView = 'day'; progress(null);
  const b = biasToday() || {};
  st.day = {struktur: b.struktur||'', news: b.news||'', newsTime: b.newsTime||'', b: b.b||'', thenStart: !!thenStart};
  render(`<div class="card">
    <div class="tag">◷ Tages-Check · einmal pro Tag</div>
    <div class="q">Wie ist die Lage heute?</div>
    <p class="hint">Routine: Was ist die letzten Tage passiert? Daily → 4h → 30m, Liquidität markieren, News prüfen.</p>
    <div class="f"><label>Struktur auf Daily / 4h (primäre Swing Levels)</label>
      <div class="seg" id="d_str"><button data-v="bull">Bullisch</button><button data-v="bear">Bärisch</button><button data-v="unklar">Unklar</button></div></div>
    <div class="f"><label>Wichtige News heute (3 Sterne)?</label>
      <div class="seg" id="d_news"><button data-v="nein">Keine</button><button data-v="ja">Ja, um …</button></div>
      <input id="d_time" type="time" value="${esc(st.day.newsTime)}" ${st.day.news==='ja'?'':'hidden'} aria-label="Uhrzeit der News"></div>
    <div class="f"><label>Dein Bias heute</label>
      <div class="seg" id="d_bias"><button data-v="long" class="lg">Long</button><button data-v="short" class="sh">Short</button><button data-v="neutral">Neutral</button></div></div>
    <div class="f"><label for="d_fokus">Fokus-Coins mit Alert (optional, mit Komma)</label>
      <input id="d_fokus" placeholder="z. B. XRP, SOL, ENA" value="${esc((b.fokus||[]).join(', '))}" autocomplete="off"></div>
    <div class="f"><label for="d_note">Gedanken für heute (optional)</label>
      <input id="d_note" placeholder="z. B. Montag, erster Börsenstart der Woche – erst beobachten" value="${esc(b.note||'')}" autocomplete="off"></div>
    <div class="row"><button class="btn ghost" onclick="home()">Abbrechen</button><button class="btn pri" id="d_go" onclick="saveDay()">Bias speichern</button></div>
  </div>`);
  later(()=>{
    const bind = (id, key, after) => $$(`#${id} button`).forEach(btn => {
      if (st.day[key] === btn.dataset.v) btn.classList.add('on');
      btn.onclick = () => { st.day[key] = btn.dataset.v; $$(`#${id} button`).forEach(x => x.classList.toggle('on', x===btn)); after && after(); dayUpd(); };
    });
    bind('d_str', 'struktur', () => { st.day.b = {bull:'long', bear:'short', unklar:'neutral'}[st.day.struktur]; $$('#d_bias button').forEach(x => x.classList.toggle('on', x.dataset.v===st.day.b)); });
    bind('d_news', 'news', () => { $('#d_time').hidden = st.day.news!=='ja'; if (st.day.news==='ja') $('#d_time').focus(); });
    bind('d_bias', 'b');
    $('#d_time').oninput = e => { st.day.newsTime = e.target.value; dayUpd(); };
    dayUpd();
  });
}
function dayUpd(){ const d = st.day, ok = d.struktur && d.b && (d.news==='nein' || (d.news==='ja' && d.newsTime)); const g = $('#d_go'); if (g) g.disabled = !ok; }
function saveDay(){
  const d = st.day;
  const fokus = $('#d_fokus').value.split(/[,\s]+/).map(coinFmt).filter(Boolean).slice(0,8);
  S.set('lastBias', {b:d.b, date:today(), struktur:d.struktur, news:d.news, newsTime: d.news==='ja' ? d.newsTime : '', fokus, note: $('#d_note').value.trim().slice(0,300)});
  if (d.b === 'neutral') return stopView('neutral');
  if (d.thenStart) return start();
  home();
}

/* ------------------------------------------------------------------
   1 · Atmen
------------------------------------------------------------------ */
function sigh(){
  render(`<div class="card breath">
    <div class="tag">◎ Atmen · 9 Sekunden</div>
    <div class="q">Ein tiefer Seufzer</div>
    <p class="hint">Durch die Nase einatmen, kurz nachatmen, dann lange durch den Mund aus.</p>
    <div class="circle"><div class="core" id="core"></div>
      <svg viewBox="0 0 216 216" aria-hidden="true"><circle id="arc" cx="108" cy="108" r="104" stroke-dasharray="653" stroke-dashoffset="653"/></svg>
      <div class="lab" id="blab">Einatmen</div></div>
    <div class="row"><button class="btn ghost" onclick="home()">Abbrechen</button><button class="btn ghost" style="flex:1" onclick="next()">Überspringen</button></div>
  </div>`);
  const P = [[0,2600,'Tief einatmen',.82],[2600,3600,'Nochmal kurz nach',1],[3600,9000,'Langsam ausatmen …',.42]];
  const t0 = Date.now() + 250;
  timers.push(setInterval(() => {
    const core = $('#core'); if (!core) return;
    const el = Date.now()-t0; if (el < 0) return;
    const ph = P.find(p => el>=p[0] && el<p[1]);
    if (ph){ $('#blab').textContent = ph[2]; core.style.transitionDuration = ((ph[1]-ph[0])/1000)+'s'; core.style.transform = `scale(${ph[3]})`; }
    $('#arc').style.strokeDashoffset = 653*(1-Math.min(1, el/9000));
    if (el >= 9000){ stopTimers(); next(); }
  }, 100));
}

/* ------------------------------------------------------------------
   2 · Kopf
------------------------------------------------------------------ */
function kopf(){
  render(`<div class="card">
    <div class="tag">● Kopf</div>
    <div class="q">Was fühlst du gerade?</div>
    <p class="hint">Benenne es ehrlich. Schon das Benennen dämpft die Emotion.</p>
    <div class="opts two">${FEEL.map((f,i)=>`<button class="opt" onclick="feel('${f[0]}',${i})"><span class="k" ${i===0?'style="color:var(--green)"':''}>${i===0?'✓':'!'}</span>${esc(f[1])}</button>`).join('')}</div>
  </div>`);
}
function feel(k, i){
  $$('.opt')[i]?.classList.add('sel'); st.feel = k;
  setTimeout(()=>{
    if (k==='ruhig') return next();
    if (k==='angst'){ st.risk = cfg.risk/2; return warnAngst(); }
    stopView({fomo:'fomo', frust:'revenge', gier:'gier', lw:'overtrading'}[k]);
  }, 160);
}
function warnAngst(){
  render(`<div class="card">
    <div class="tag" style="color:var(--gold)">▲ Achtung</div>
    <div class="q">Angst im Spiel</div>
    <p class="hint">Angst lässt dich zu spät rein, zu früh raus oder gar nicht handeln. Meist ist das Risiko zu groß, damit ein Verlust dich kaltlässt.</p>
    <div class="henry">Für diesen Trade halbiere ich dein Risiko auf <b>${fmt(st.risk,2)} %</b>.</div>
    <div class="row"><button class="btn ghost" onclick="home()">Lieber nicht</button><button class="btn pri" onclick="next()">Mit halbem Risiko weiter</button></div>
  </div>`, 'warnv');
}

/* ------------------------------------------------------------------
   3 · Setup
------------------------------------------------------------------ */
function setup(){
  render(`<div class="card">
    <div class="tag">● Setup</div>
    <div class="q">Alles erfüllt?</div>
    <div class="seg" id="dir"><button data-v="long" class="lg ${st.dir==='long'?'on':''}">▲ Long</button><button data-v="short" class="sh ${st.dir==='short'?'on':''}">▼ Short</button></div>
    <div class="f"><label>Strategie</label>${chipRow('strat', strats(), st.strat)}</div>
    ${nearUSOpen()?'<p class="note">⚠ US-Börsenstart (15:30) in Reichweite: oft ein kurzer Liquidity Grab.</p>':''}
    <div class="checks" id="cks">${ckHtml(st.strat)}</div>
    <div class="row"><button class="btn ghost" onclick="stopView('setup')">Etwas fehlt</button><button class="btn pri" id="cgo" disabled onclick="next()">Setup bestätigt</button></div>
  </div>`);
  later(()=>{
    $$('#dir button').forEach(b => b.onclick = () => { if (b.dataset.v !== biasToday().b) stopView('gegenbias'); });
    bindChips('strat', v => { st.strat = v; S.set('lastStrat', v); $('#cks').innerHTML = ckHtml(v); ckUpd(); });
  });
}
function ckUpd(){ $('#cgo').disabled = $$('.ck.on').length < $$('.ck').length; }

/* ------------------------------------------------------------------
   4 · Plan
------------------------------------------------------------------ */
function plan(){
  const d = st.dir, coins = recentCoins();
  if (!st.coin && coins.length) st.coin = coins[0];
  render(`<div class="card">
    <div class="tag">● Plan</div>
    <div class="q">${d==='long'?'<span style="color:var(--green)">Long</span>':'<span style="color:var(--red)">Short</span>'} – Einstieg, Stop, Ziel</div>
    <div class="f"><label for="coin">Coin</label>
      <div class="coinrow"><input id="coin" value="${esc(st.coin||'')}" placeholder="z. B. XRP" autocomplete="off" autocapitalize="characters">
      ${coins.length ? chipRow('coins', coins, st.coin, 'small') : ''}</div></div>
    <div class="grid3">
      <div class="f"><label for="entry">Einstieg</label><input id="entry" inputmode="decimal" autocomplete="off"></div>
      <div class="f"><label for="sl">Stop-Loss</label><input id="sl" inputmode="decimal" autocomplete="off"></div>
      <div class="f"><label for="tp">Ziel</label><input id="tp" inputmode="decimal" autocomplete="off"></div>
    </div>
    ${(() => { const s = stratBy(st.strat); return s && (s.stop || s.target) ? `<p class="hint small rulehint">${s.stop ? `Stop: <b>${esc(s.stop)}</b>` : ''}${s.stop && s.target ? ' · ' : ''}${s.target ? `Ziel: <b>${esc(s.target)}</b>` : ''}</p>` : ''; })()}
    <button class="mini" onclick="$('#accf').hidden=!$('#accf').hidden">Konto ${fmt(cfg.konto,0)} USDT · Risiko ${fmt(st.risk,2)} % · Hebel ${fmt(cfg.hebel,0)}x · ändern</button>
    <div class="grid3" id="accf" hidden>
      <div class="f"><label for="konto">Konto (USDT)</label><input id="konto" inputmode="decimal" value="${cfg.konto}"></div>
      <div class="f"><label for="risk">Risiko (%)</label><input id="risk" inputmode="decimal" value="${st.risk}"></div>
      <div class="f"><label for="hebel">Hebel</label><input id="hebel" inputmode="decimal" value="${cfg.hebel}"></div>
    </div>
    <div class="calc" id="calc"></div>
    <div class="msg" id="rmsg"></div>
    <div class="row"><button class="btn ghost" onclick="home()">Abbrechen</button><button class="btn pri" id="rgo" disabled onclick="planGo()">Plan steht</button></div>
  </div>`);
  later(()=>{
    $$('.f input').forEach(i=>i.addEventListener('input', calc));
    $('#coin').addEventListener('input', e => { st.coin = coinFmt(e.target.value); $$('#coins button').forEach(x => x.classList.toggle('on', x.dataset.v===st.coin)); });
    bindChips('coins', v => { st.coin = v; $('#coin').value = v; });
    calc(); $('#entry').focus();
  });
}
function calc(){
  const g = id => num($('#'+id).value);
  const konto=g('konto'), r=g('risk'), e=g('entry'), sl=g('sl'), tp=g('tp'), lev=g('hebel'), dir = st.dir==='long'?1:-1;
  const b = $('#rgo'), msg = $('#rmsg');
  let ok = false, fail = '', m = '';
  const dist = Math.abs(e-sl), crv = Math.abs(tp-e)/dist, riskAmt = konto*r/100, vol = riskAmt/dist*e, margin = vol/lev;
  const liq = e*(1 - dir*(1/lev - 0.005));
  const filled = [konto,r,e,sl,tp,lev].every(x=>isFinite(x) && x>0);
  if (!filled) m = 'Einstieg, Stop und Ziel eintragen.';
  else if ((dir===1 && !(sl<e && tp>e)) || (dir===-1 && !(sl>e && tp<e))) m = dir===1 ? 'Long: Stop unter, Ziel über dem Einstieg.' : 'Short: Stop über, Ziel unter dem Einstieg.';
  else if (crv < cfg.minCRV){ fail='crv'; m = `CRV ${fmt(crv)} – dein Minimum ist ${fmt(cfg.minCRV,1)}.`; }
  else if ((dir===1 && liq>=sl) || (dir===-1 && liq<=sl)){ fail='liq'; m = `Liquidation bei ca. ${fmt(liq,4)} – vor deinem Stop.`; }
  else if (margin > konto) m = 'Zu wenig Kapital für diese Position. Hebel oder Risiko anpassen.';
  else { ok = true; m = r>2 ? 'Mehr als 2 % Risiko – bewusst so?' : 'Plan ist stimmig.'; }
  const show = filled && isFinite(crv);
  $('#calc').innerHTML = `
    <div class="kpi ${show?(crv>=cfg.minCRV?'good':'bad'):''}"><div class="v">${show?fmt(crv):'–'}</div><div class="l">CRV</div></div>
    <div class="kpi"><div class="v">${isFinite(riskAmt)?fmt(riskAmt):'–'}</div><div class="l">Risiko USDT</div></div>
    <div class="kpi"><div class="v">${show?fmt(vol,0):'–'}</div><div class="l">Position USDT</div></div>
    <div class="kpi ${show&&fail==='liq'?'bad':''}"><div class="v">${show?fmt(liq,4):'–'}</div><div class="l">Liquidation</div></div>`;
  msg.textContent = m; msg.className = 'msg ' + (ok ? 'good' : (fail ? 'bad' : ''));
  b.disabled = !ok && !fail; b.dataset.fail = fail;
  b.textContent = fail ? 'So nicht traden' : 'Plan steht';
  Object.assign(st, {konto, risk:r, entry:e, sl, tp, hebel:lev, crv, riskAmt});
}
function planGo(){
  const f = $('#rgo').dataset.fail; if (f) return stopView(f);
  st.coin = coinFmt($('#coin').value);
  cfg.konto = st.konto; cfg.hebel = st.hebel; saveCfg(); next();
}

/* ------------------------------------------------------------------
   5 · Los: Setup-Note VOR dem Ergebnis + Wenn-dann-Plan
------------------------------------------------------------------ */
function commit(){
  render(`<div class="card">
    <div class="tag">● Los</div>
    <div class="f"><label>Wie gut ist das Setup? Jetzt bewerten, nicht erst nach dem Ergebnis.</label>
      <div class="grades" id="grade">${GRADES.map(g=>`<button data-v="${g[0]}"><b>${g[0]}</b><span>${esc(g[1])}</span></button>`).join('')}</div></div>
    <div class="ifthen">
      <div><b>Wenn</b> der Kurs gegen mich läuft, <b>dann</b> bleibt der Stop, wo er ist.</div>
      <div><b>Wenn</b> ich früher raus oder nachlegen will, <b>dann</b> tippe ich den Impuls an, statt zu handeln.</div>
      <div><b>Wenn</b> der Trade vorbei ist, <b>dann</b> mache ich ${cfg.cooldownLoss} Minuten Pause.</div>
    </div>
    <p class="hint" style="margin-top:14px">${esc(st.coin||'')} ${st.dir==='long'?'LONG':'SHORT'} · ${esc(st.strat)} · Einstieg ${fmt(st.entry,4)} · Stop ${fmt(st.sl,4)} · Ziel ${fmt(st.tp,4)} · ${fmt(st.crv)}R</p>
    <div class="row"><button class="btn green" id="go" disabled onclick="enterTrade()">Stop & Ziel gesetzt – ich bin drin</button></div>
    <div class="row" style="margin-top:8px"><button class="btn ghost" style="flex:1" onclick="home()">Doch nicht eingestiegen</button></div>
  </div>`);
  later(()=> $$('#grade button').forEach(b => b.onclick = () => { st.grade = b.dataset.v; $$('#grade button').forEach(x => x.classList.toggle('on', x===b)); $('#go').disabled = false; }));
}
function enterTrade(){
  curView = 'intrade';
  progress(4); $$('.phase').forEach(p=>p.classList.add('done'));
  confetti();
  S.set('active', {dir:st.dir, coin:st.coin||'', strat:st.strat, grade:st.grade, entry:st.entry, sl:st.sl, tp:st.tp, crv:st.crv,
    riskAmt:st.riskAmt, risk:st.risk, feel:st.feel||'', start:Date.now()});
  setTimeout(inTrade, 500);
}

/* ------------------------------------------------------------------
   Im Trade
------------------------------------------------------------------ */
function inTrade(){
  curView = 'intrade'; progress(null);
  const a = S.get('active', null); if (!a) return home();
  render(`<div class="card">
    <div class="live"><span class="livedot"></span><span class="tag" style="margin:0;color:var(--green)">Im Trade · ${esc(a.coin||'')} ${a.dir==='long'?'LONG':'SHORT'} · ${esc(a.strat||'')}</span></div>
    <div class="clock" id="clock">00:00:00</div>
    <div class="plan">
      <div class="kpi"><div class="v">${fmt(a.entry,4)}</div><div class="l">Einstieg</div></div>
      <div class="kpi bad"><div class="v">${fmt(a.sl,4)}</div><div class="l">Stop</div></div>
      <div class="kpi good"><div class="v">${fmt(a.tp,4)}</div><div class="l">Ziel</div></div>
      <div class="kpi"><div class="v">${fmt(a.riskAmt)}</div><div class="l">Risiko USDT</div></div>
    </div>
    <div class="reminder" id="rem">${REMIND[0]}</div>
    <p class="hint" style="margin:16px 0 0">Impuls? Erst hier tippen:</p>
    <div class="temps">
      <button class="temp" onclick="tempt('stop')">Stop verschieben</button>
      <button class="temp" onclick="tempt('frueh')">Früher raus</button>
      <button class="temp" onclick="tempt('mehr')">Position vergrößern</button>
      <button class="temp" onclick="tempt('nervoes')">Nervös am Chart</button>
    </div>
    <p class="hint" style="margin:20px 0 0">Trade beendet?</p>
    <div class="ends">
      <button class="btn green" onclick="endTrade('tp')">Ziel</button>
      <button class="btn red" onclick="endTrade('sl')">Stop</button>
      <button class="btn ghost" onclick="endTrade('manuell')">Manuell</button>
    </div>
  </div>`);
  let ri = 0;
  const tick = () => { const c = $('#clock'); if (!c) return; const s = Math.max(0, Math.floor((Date.now()-a.start)/1000));
    c.textContent = [Math.floor(s/3600), Math.floor(s/60)%60, s%60].map(x=>String(x).padStart(2,'0')).join(':'); };
  setTimeout(tick, 250); timers.push(setInterval(tick, 1000));
  timers.push(setInterval(()=>{ const r = $('#rem'); if(!r) return; r.style.opacity=0;
    setTimeout(()=>{ ri=(ri+1)%REMIND.length; r.textContent=REMIND[ri]; r.style.opacity=1; },500); }, 9000));
}
function tempt(k){
  const t = TEMPT[k];
  const a = Object.assign({}, S.get('active', {})); a.impulse = (a.impulse||0)+1; S.set('active', a);
  modal(`<div class="tag" style="color:var(--gold)">⚠ Impuls erkannt</div>
    <div class="q">${esc(t.t)}</div><p class="hint">${esc(t.x)}</p>
    <div class="henry"><b>Merke:</b> ${esc(t.h)}</div>
    <div class="row"><button class="btn pri" onclick="closeModal()">Ich bleibe bei meinem Plan</button></div>`);
}
function modal(h){ $('#modalc').innerHTML = h; $('#modal').classList.add('on'); }
function closeModal(){ const m = $('#modal'); if (m) m.classList.remove('on'); }

/* ------------------------------------------------------------------
   Nach dem Trade: Regeltreue pro Regel pro Regel
------------------------------------------------------------------ */
function rulesRow(id, rules){
  return `<div class="chips rules" id="${id}">${RULES.map(([k,l])=>`<button type="button" data-v="${k}" class="${rules[k]!==false?'on':''}">${l}</button>`).join('')}</div>`;
}
function readRules(id){ const r = {}; $$(`#${id} button`).forEach(b => r[b.dataset.v] = b.classList.contains('on')); return r; }
function endTrade(art){
  stopTimers(); curView = 'end';
  const a = S.get('active', null); if (!a) return home();
  const R = art==='tp' ? a.crv : art==='sl' ? -1 : null;
  render(`<div class="card">
    <div class="tag">● Nach dem Trade</div>
    <div class="q">${art==='tp'?'Ziel erreicht':art==='sl'?'Stop getroffen – ein Datenpunkt':'Manuell geschlossen'}</div>
    <div class="f"><label for="rr">Ergebnis in R</label><input id="rr" inputmode="decimal" value="${R!==null?fmt(R):''}" placeholder="z. B. 0,5"></div>
    <div class="f"><label>An welche Regeln hast du dich gehalten? Antippen, was du gebrochen hast.</label>${rulesRow('rules', art==='manuell'?{tp:false}:{})}</div>
    <div class="f"><label for="note">1 % besser beim nächsten Mal (optional)</label><input id="note" placeholder="z. B. auf Kerzenschluss warten" autocomplete="off"></div>
    <div class="f"><label for="link">Chart-Screenshot-Link (optional, z. B. TradingView)</label><input id="link" type="url" placeholder="https://www.tradingview.com/x/…" autocomplete="off"></div>
    <div class="row"><button class="btn pri" onclick="saveTrade('${art}')">Speichern</button></div>
  </div>`);
  later(()=>{ bindChips('rules', null, true); if (R===null) $('#rr')?.focus(); });
}
function cleanLink(v){ v = String(v||'').trim(); return /^https:\/\/[^\s]+$/.test(v) ? v.slice(0,300) : ''; }
function saveTrade(art){
  const a = S.get('active', null); if (!a) return home();
  const R = num($('#rr').value);
  if (!isFinite(R)){ $('#rr').focus(); $('#rr').style.borderColor='var(--red)'; return; }
  const rules = readRules('rules');
  addTrade({date:new Date().toISOString(), coin:a.coin||'', strat:a.strat||'', grade:a.grade||'', dir:a.dir, entry:a.entry, sl:a.sl, tp:a.tp, R,
    art, rules, plan: Object.values(rules).every(Boolean), feel:a.feel||'', impulse:a.impulse||0, risk:a.risk||null,
    note:$('#note').value.trim().slice(0,500), link: cleanLink($('#link').value), checklist:true});
  S.set('active', null);
  const mins = R<0 ? cfg.cooldownLoss : cfg.cooldownWin;
  if (mins>0) S.set('cooldownUntil', Date.now()+mins*60000);
  if (R>0) confetti();
  cooldownView(R);
}
function cooldownView(R){
  stopTimers(); curView = 'cool'; progress(null);
  const loss = R!==undefined ? R<0 : null;
  render(`<div class="card" style="text-align:center">
    <div class="tag" style="color:var(--blue)">◎ Pause</div>
    <div class="q">${loss===true?'Handy weg. Kurz raus.':loss===false?'Gespeichert. Kein Übermut-Trade.':'Du bist noch in der Pause.'}</div>
    <p class="hint">${loss===true?'Nach Verlusten steigt die Risikobereitschaft messbar. Die Pause schützt dich davor.':'Auch nach Gewinnen: Der nächste Trade kommt nur über die Checkliste.'}</p>
    <div class="timer" id="cdt" style="color:var(--blue)">${mmss(cooldownLeft())}</div>
    <div class="row"><button class="btn ghost" style="flex:1" onclick="home()">Start</button><button class="btn ghost" style="flex:1" onclick="journal()">Journal</button></div>
  </div>`);
  timers.push(setInterval(()=>{ const c=$('#cdt'); if(!c) return; const l=cooldownLeft(); c.textContent = l>0?mmss(l):'Fertig'; }, 1000));
}

/* ------------------------------------------------------------------
   Stopp
------------------------------------------------------------------ */
function stopView(k, text){
  stopTimers(); curView = 'stop'; progress(null);
  const s = STOPS[k];
  if (s.cool) S.set('cooldownUntil', Math.max(S.get('cooldownUntil',0), Date.now()+cfg.cooldownLoss*60000));
  const log = S.get('stops', []).slice(); log.unshift({k, date:new Date().toISOString()}); S.set('stops', log.slice(0,150));
  render(`<div class="card">
    <svg class="xicon" viewBox="0 0 64 64" aria-hidden="true"><path d="M18 18 L46 46"/><path d="M46 18 L18 46" style="animation-delay:.35s"/></svg>
    <div class="tag">■ Kein Trade</div>
    <div class="q">${esc(s.t)}</div>
    <p class="hint">${esc(text || s.x)}</p>
    <div class="henry"><b>Merke:</b> ${esc(s.h)}</div>
    ${s.cool?`<p class="hint" style="margin-top:14px">${cfg.cooldownLoss} Minuten Pause gestartet.</p>`:''}
    <div class="row"><button class="btn pri" onclick="home()">Verstanden</button></div>
    ${k==='neutral'||k==='gegenbias' ? '<div class="row" style="margin-top:8px"><button class="btn ghost" style="flex:1" onclick="dayCheck()">Tages-Check neu machen</button></div>' : ''}
  </div>`, 'stop');
}

/* ------------------------------------------------------------------
   Journal: Kennzahlen, Erkenntnisse, Trades
------------------------------------------------------------------ */
function filtered(){
  const days = jRange==='all' ? Infinity : +jRange;
  const since = Date.now() - days*86400000;
  return journalList().filter(j => Date.parse(j.date) >= since && (jStrat==='alle' || j.strat===jStrat));
}
function kpis(arr){
  const n = arr.length, wins = arr.filter(x=>x.R>0), losses = arr.filter(x=>x.R<0);
  const sum = arr.reduce((a,x)=>a+x.R,0), gp = wins.reduce((a,x)=>a+x.R,0), gl = -losses.reduce((a,x)=>a+x.R,0);
  return {n, win: n? wins.length/n*100 : NaN, exp: n? sum/n : NaN, sum, pf: gl>0 ? gp/gl : (gp>0 ? Infinity : NaN),
    rule: n? arr.filter(x=>x.plan).length/n*100 : NaN};
}
function insights(arr){
  const out = [], avg = a => a.length ? a.reduce((s,x)=>s+x.R,0)/a.length : NaN, MIN = 3;
  const kept = arr.filter(x=>x.plan), broke = arr.filter(x=>!x.plan);
  if (kept.length>=MIN && broke.length>=MIN) out.push(`Plan gehalten: Ø <b>${sgn(avg(kept))} R</b> · Plan gebrochen: Ø <b>${sgn(avg(broke))} R</b>`);
  const g = {}; arr.forEach(x => { if (x.grade) (g[x.grade] = g[x.grade] || []).push(x); });
  const gs = ['S','A','B'].filter(k => (g[k]||[]).length>=MIN);
  if (gs.length>=2) out.push('Nach Setup-Note: ' + gs.map(k=>`${k} <b>${sgn(avg(g[k]))} R</b> (${g[k].length})`).join(' · '));
  const calm = arr.filter(x=>x.feel==='ruhig'), nerv = arr.filter(x=>x.feel==='angst');
  if (calm.length>=MIN && nerv.length>=MIN) out.push(`Ruhig getradet: Ø <b>${sgn(avg(calm))} R</b> · Nervös: Ø <b>${sgn(avg(nerv))} R</b>`);
  const withC = arr.filter(x=>x.checklist), noC = arr.filter(x=>!x.checklist);
  if (withC.length>=MIN && noC.length>=MIN) out.push(`Mit Checkliste: Ø <b>${sgn(avg(withC))} R</b> · Ohne: Ø <b>${sgn(avg(noC))} R</b>`);
  const imp = arr.filter(x=>x.impulse>0), noImp = arr.filter(x=>!x.impulse);
  if (imp.length>=MIN && noImp.length>=MIN) out.push(`Trades mit Impulsen: Ø <b>${sgn(avg(imp))} R</b> · ohne: Ø <b>${sgn(avg(noImp))} R</b>`);
  const bs = {}; arr.forEach(x => { if (x.strat) (bs[x.strat] = bs[x.strat] || []).push(x); });
  const sk = Object.keys(bs).filter(k => bs[k].length >= MIN).sort((a,b) => avg(bs[b]) - avg(bs[a]));
  if (sk.length >= 2) out.push('Nach Strategie: ' + sk.map(k => `${esc(k)} <b>${sgn(avg(bs[k]))} R</b> (${bs[k].length})`).join(' · '));
  const brokenRule = RULES.map(([k,l]) => [l, arr.filter(x => x.rules && x.rules[k]===false).length]).sort((a,b)=>b[1]-a[1])[0];
  if (brokenRule && brokenRule[1]>=2) out.push(`Am häufigsten gebrochen: <b>${brokenRule[0]}</b> (${brokenRule[1]}×)`);
  return out;
}
function journal(){
  curView = 'journal'; progress(null); closeModal();
  const arr = filtered(), k = kpis(arr), stops = S.get('stops', []);
  const RANGES = [['7','7 Tage'],['30','30 Tage'],['all','Alles']];
  const head = `<div class="tag">● Journal</div>
    <div class="tabs"><button class="${jTab==='stats'?'on':''}" onclick="jTab='stats';journal()">Übersicht</button><button class="${jTab==='list'?'on':''}" onclick="jTab='list';journal()">Trades (${arr.length})</button></div>
    ${chipRow('jr', RANGES, jRange, 'small')}${chipRow('js', [['alle','Alle Strategien'], ...strats()], jStrat, 'small')}`;
  let body;
  if (jTab==='stats'){
    const ins = insights(arr);
    body = `<div class="jstats">
        <div class="kpi"><div class="v">${k.n}</div><div class="l">Trades</div></div>
        <div class="kpi"><div class="v">${k.n?fmt(k.win,0)+' %':'–'}</div><div class="l">Treffer</div></div>
        <div class="kpi ${k.exp>0?'good':k.exp<0?'bad':''}"><div class="v">${k.n?sgn(k.exp):'–'}</div><div class="l">Ø R / Trade</div></div>
        <div class="kpi"><div class="v">${isFinite(k.pf)?fmt(k.pf):k.pf===Infinity?'∞':'–'}</div><div class="l">Profit-Faktor</div></div>
        <div class="kpi ${k.sum>0?'good':k.sum<0?'bad':''}"><div class="v">${k.n?sgn(k.sum,1):'–'}</div><div class="l">Netto R</div></div>
        <div class="kpi"><div class="v">${k.n?fmt(k.rule,0)+' %':'–'}</div><div class="l">Regeltreue</div></div>
      </div>
      <div class="eqwrap"><canvas id="eq" height="150" aria-label="Kapitalkurve in R"></canvas></div>
      ${k.n && k.n<30 ? `<p class="hint small">Noch ${30-k.n} Trades bis 30 – erst ab da werden die Zahlen aussagekräftig.</p>` : ''}
      <div class="ins"><div class="lbl2">Erkenntnisse</div>${ins.length ? ins.map(i=>`<div>${i}</div>`).join('') : '<div class="mutedtxt">Ab ein paar Trades pro Gruppe zeigt dir die App hier, was bei dir funktioniert – z. B. Plan gehalten vs. gebrochen, S- vs. B-Setups, ruhig vs. nervös.</div>'}</div>
      <p class="hint small">Von der Checkliste verhinderte Trades: <b>${stops.length}</b></p>`;
  } else {
    body = `<div class="jlist">${arr.length ? arr.map(x=>`<button class="jitem" onclick="editTrade('${x.id}')">
      <span class="d" style="color:${x.dir==='long'?'var(--green)':'var(--red)'}">${x.dir==='long'?'▲':'▼'}</span>
      <div class="m"><div><b>${esc(x.coin||'–')}</b> · ${esc(x.strat||'–')}${x.grade?` · ${esc(x.grade)}`:''}${x.checklist?'':' · <span class="warnc">ohne Checkliste</span>'}${x.plan?'':' · <span class="warnc">Regel gebrochen</span>'}</div>
      <div class="n">${new Date(x.date).toLocaleString('de-DE',{dateStyle:'short',timeStyle:'short'})}${x.note?' · '+esc(x.note):''}</div></div>
      <span class="r" style="color:${x.R>0?'var(--green)':x.R<0?'var(--red)':'var(--mut)'}">${sgn(x.R)} R</span></button>`).join('')
      : '<div class="empty">Keine Trades in diesem Zeitraum. Trades aus der Checkliste landen hier automatisch – andere kannst du nachtragen.</div>'}</div>`;
  }
  render(`<div class="card">${head}${body}
    <div class="row jrow"><button class="btn ghost" onclick="addManual()">+ Trade nachtragen</button><button class="btn ghost" onclick="exportCSV()">CSV exportieren</button></div>
  </div>`);
  later(()=>{
    bindChips('jr', v => { jRange = v; journal(); });
    bindChips('js', v => { jStrat = v; journal(); });
    if (jTab==='stats') drawEquity(arr.slice().reverse());
  });
}
function drawEquity(arr){
  const c = $('#eq'); if (!c) return;
  const css = getComputedStyle(document.documentElement);
  const W = c.clientWidth, H = 150, dpr = devicePixelRatio || 1;
  c.width = W*dpr; c.height = H*dpr; const x = c.getContext('2d'); x.scale(dpr, dpr);
  const pts = [0]; arr.forEach(t => pts.push(pts[pts.length-1] + (+t.R||0)));
  const lo = Math.min(0, ...pts), hi = Math.max(0, ...pts), pad = 18, span = (hi-lo) || 1;
  const X = i => pad + (W-2*pad) * (pts.length>1 ? i/(pts.length-1) : 0.5), Y = v => H-pad - (H-2*pad) * (v-lo)/span;
  x.strokeStyle = css.getPropertyValue('--line'); x.lineWidth = 1; x.setLineDash([3,4]);
  x.beginPath(); x.moveTo(pad, Y(0)); x.lineTo(W-pad, Y(0)); x.stroke(); x.setLineDash([]);
  x.fillStyle = css.getPropertyValue('--mut'); x.font = '11px -apple-system, sans-serif'; x.fillText('0 R', 2, Y(0)-4);
  if (pts.length < 2){ x.fillText('Kapitalkurve erscheint nach dem ersten Trade', pad, H/2); return; }
  const last = pts[pts.length-1], col = css.getPropertyValue(last>=0?'--green':'--red').trim();
  const grad = x.createLinearGradient(0,0,0,H); grad.addColorStop(0, col+'55'); grad.addColorStop(1, col+'00');
  x.beginPath(); pts.forEach((v,i)=> i? x.lineTo(X(i),Y(v)) : x.moveTo(X(i),Y(v))); x.lineTo(X(pts.length-1), Y(0)); x.lineTo(X(0), Y(0)); x.closePath();
  x.fillStyle = grad; x.fill();
  x.beginPath(); pts.forEach((v,i)=> i? x.lineTo(X(i),Y(v)) : x.moveTo(X(i),Y(v))); x.strokeStyle = col; x.lineWidth = 2; x.stroke();
  x.beginPath(); x.arc(X(pts.length-1), Y(last), 4, 0, 7); x.fillStyle = col; x.fill();
  x.fillStyle = css.getPropertyValue('--txt'); x.font = 'bold 12px -apple-system, sans-serif';
  const lbl = sgn(last,1)+' R'; x.fillText(lbl, Math.min(W - x.measureText(lbl).width - 4, X(pts.length-1)+6), Math.max(14, Y(last)-8));
}

/* Trade bearbeiten / löschen / nachtragen */
function tradeForm(t, isNew){
  return `<div class="tag">${isNew?'+ Trade nachtragen':'✎ Trade bearbeiten'}</div>
    ${isNew?'<p class="hint">Für Trades ohne Checkliste. Sie zählen in deine Statistik, damit sie ehrlich bleibt.</p>':''}
    <div class="grid2">
      <div class="f"><label for="e_coin">Coin</label><input id="e_coin" value="${esc(t.coin||'')}" autocomplete="off"></div>
      <div class="f"><label for="e_r">Ergebnis in R</label><input id="e_r" inputmode="decimal" value="${isFinite(t.R)?fmt(t.R):''}"></div>
    </div>
    ${isNew?`<div class="f"><label for="e_date">Datum</label><input id="e_date" type="datetime-local" value="${new Date(Date.now()-new Date().getTimezoneOffset()*60000).toISOString().slice(0,16)}"></div>
    <div class="f"><label>Richtung</label>${chipRow('e_dir', [['long','Long'],['short','Short']], t.dir||'long')}</div>`:''}
    <div class="f"><label>Strategie</label>${chipRow('e_strat', strats(), t.strat||strats()[0], 'small')}</div>
    <div class="f"><label>Regeln eingehalten</label>${rulesRow('e_rules', t.rules||{})}</div>
    <div class="f"><label for="e_note">Notiz</label><input id="e_note" value="${esc(t.note||'')}" autocomplete="off"></div>
    <div class="f"><label for="e_link">Chart-Link</label><input id="e_link" type="url" value="${esc(t.link||'')}" placeholder="https://…" autocomplete="off"></div>
    ${t.link?`<p class="hint small"><a href="${esc(t.link)}" target="_blank" rel="noopener">Chart öffnen ↗</a></p>`:''}
    <div class="row"><button class="btn ghost" onclick="closeModal()">Abbrechen</button><button class="btn pri" onclick="${isNew?'saveManual()':`saveEdit('${t.id}')`}">Speichern</button></div>
    ${isNew?'':`<div class="row" style="margin-top:8px"><button class="btn ghost danger" style="flex:1" onclick="delTrade('${t.id}', this)">Trade löschen</button></div>`}`;
}
function bindForm(){ bindChips('e_dir'); bindChips('e_strat'); bindChips('e_rules', null, true); }
const chipVal = id => $(`#${id} button.on`)?.dataset.v;
function editTrade(id){
  const t = cache.journal.find(j => j.id===id); if (!t) return;
  modal(tradeForm(t, false)); bindForm();
}
function saveEdit(id){
  const t = cache.journal.find(j => j.id===id); if (!t) return;
  const R = num($('#e_r').value); if (!isFinite(R)){ $('#e_r').style.borderColor='var(--red)'; return; }
  const rules = readRules('e_rules');
  updateTrade(Object.assign({}, t, {coin: coinFmt($('#e_coin').value), R, strat: chipVal('e_strat')||t.strat, rules, plan: Object.values(rules).every(Boolean),
    note: $('#e_note').value.trim().slice(0,500), link: cleanLink($('#e_link').value)}));
  closeModal(); journal();
}
let delArmed = 0;
function delTrade(id, btn){
  if (Date.now()-delArmed > 4000){ delArmed = Date.now(); btn.textContent = 'Nochmal tippen zum Löschen'; return; }
  removeTrade(id); closeModal(); journal();
}
function addManual(){ modal(tradeForm({}, true)); bindForm(); }
function saveManual(){
  const R = num($('#e_r').value); if (!isFinite(R)){ $('#e_r').style.borderColor='var(--red)'; return; }
  const d = new Date($('#e_date').value); const rules = readRules('e_rules');
  addTrade({date: (isNaN(d) ? new Date() : d).toISOString(), coin: coinFmt($('#e_coin').value), dir: chipVal('e_dir')||'long', strat: chipVal('e_strat')||'',
    R, rules, plan: Object.values(rules).every(Boolean), note: $('#e_note').value.trim().slice(0,500), link: cleanLink($('#e_link').value), checklist:false, art:'nachgetragen'});
  closeModal(); if (curView==='journal') journal(); else home();
}
function exportCSV(){
  const j = journalList();
  const rows = [['datum','coin','strategie','note','richtung','einstieg','stop','ziel','R','ausgang','regeln_eingehalten','bedingungen','einstieg_ok','stop_ok','ziel_ok','gefuehl','impulse','checkliste','notiz','chart']]
    .concat(j.map(x=>[x.date,x.coin||'',x.strat||'',x.grade||'',x.dir,x.entry??'',x.sl??'',x.tp??'',x.R,x.art||'',x.plan?'ja':'nein',
      ...RULES.map(([k])=> x.rules ? (x.rules[k]===false?'nein':'ja') : ''), x.feel||'', x.impulse||0, x.checklist?'ja':'nein',
      (x.note||'').replace(/[\n;]/g,' '), x.link||'']));
  const csv = '\ufeff'+rows.map(r=>r.join(';')).join('\n');
  saveFile('trade_journal.csv', csv, 'text/csv');
}

/* ------------------------------------------------------------------
   Einstellungen
------------------------------------------------------------------ */
function settings(){
  curView = 'settings'; progress(null);
  const F = [['s_max','Max. Trades pro Tag',cfg.maxTrades],['s_loss','Max. Verlust pro Tag (R)',cfg.lossLimit],['s_crv','Min. Chance-Risiko (CRV)',cfg.minCRV],
    ['s_risk','Risiko pro Trade (%)',cfg.risk],['s_lev','Standard-Hebel',cfg.hebel],['s_konto','Kontostand (USDT)',cfg.konto],
    ['s_cl','Pause nach Verlust (Min.)',cfg.cooldownLoss],['s_cw','Pause nach Gewinn (Min.)',cfg.cooldownWin],
    ['s_nw','Abstand zu News (Min.)',cfg.newsWin],['s_us','Sperre nach US-Börsenstart (Min.)',cfg.usOpenBlock]];
  render(`<div class="card">
    <div class="tag">⚙︎ Einstellungen</div>
    <div class="q">Deine Regeln</div>
    <p class="hint">Einmal in Ruhe festlegen, nicht mitten im Trade.</p>
    ${stratListHtml(true)}
    <div class="lbl3">Deine Grenzen</div>
    <div class="grid2">${F.map(f=>`<div class="f"><label for="${f[0]}">${f[1]}</label><input id="${f[0]}" inputmode="decimal" value="${f[2]}"></div>`).join('')}</div>
    <div class="row"><button class="btn pri" onclick="saveSettings()">Speichern</button></div>
    <div class="row" style="margin-top:8px"><button class="btn ghost danger" style="flex:1" onclick="if(confirmReset(this))home()">Journal & Daten löschen</button></div>
    <div class="acct">${accountBlock()}</div>
    <div class="lbl3">Backup</div>
    <div class="row" style="margin-top:8px"><button class="btn ghost" style="flex:1" onclick="exportBackup()">Backup speichern</button><button class="btn ghost" style="flex:1" onclick="$('#imp').click()">Backup laden</button></div>
    <input type="file" id="imp" accept=".json,application/json" hidden onchange="importBackup(this)">
    <p class="msg" id="impmsg"></p>
    <p class="disc">Trade Pilot ist ein Werkzeug für Disziplin und Selbstbeobachtung – keine Anlageberatung. Gehebelter Handel mit Kryptowährungen kann zum Totalverlust des eingesetzten Kapitals führen.<br><a href="impressum.html">Impressum</a> · <a href="datenschutz.html">Datenschutz</a></p>
  </div>`);
}
function saveSettings(){
  const g = id => num($('#'+id).value);
  const pos = (v, d) => isFinite(v) && v>0 ? v : d, nn = (v, d) => isFinite(v) && v>=0 ? Math.round(v) : d;
  Object.assign(cfg, {maxTrades: Math.round(pos(g('s_max'),2)), lossLimit: pos(g('s_loss'),2), minCRV: pos(g('s_crv'),2), risk: pos(g('s_risk'),1),
    hebel: pos(g('s_lev'),5), konto: pos(g('s_konto'),1000), cooldownLoss: nn(g('s_cl'),15), cooldownWin: nn(g('s_cw'),5),
    newsWin: nn(g('s_nw'),30), usOpenBlock: nn(g('s_us'),10)});
  saveCfg(); home();
}
let resetArmed = 0;
function confirmReset(btn){
  if (Date.now()-resetArmed < 4000){
    const ids = cache.journal.map(j => j.id);
    cache.journal = []; LS.set('journal', []);
    ['stops','active','cooldownUntil','lastBias'].forEach(k => { cache[k] = null; LS.set(k, null); });
    S.set('resetAt', Date.now());
    Sync.delTrades(ids);
    return true;
  }
  resetArmed = Date.now(); btn.textContent = 'Nochmal tippen zum Löschen'; return false;
}

/* ------------------------------------------------------------------
   Effekte
------------------------------------------------------------------ */
(function bgfx(){
  const c = $('#bgfx'), x = c.getContext('2d'); let w, h, P;
  const size = () => { w = c.width = innerWidth*devicePixelRatio; h = c.height = innerHeight*devicePixelRatio;
    P = Array.from({length: Math.min(50, Math.floor(innerWidth/22))}, () => ({x:Math.random()*w, y:Math.random()*h, v:(.15+Math.random()*.35)*devicePixelRatio, r:(Math.random()*1.6+.4)*devicePixelRatio, a:Math.random()*.5+.1})); };
  size(); addEventListener('resize', size);
  if (matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  (function loop(){ x.clearRect(0,0,w,h);
    for (const p of P){ p.y -= p.v; if (p.y < -5){ p.y = h+5; p.x = Math.random()*w; }
      x.beginPath(); x.arc(p.x,p.y,p.r,0,7); x.fillStyle = `rgba(245,197,66,${p.a*.5})`; x.fill(); }
    requestAnimationFrame(loop); })();
})();
function confetti(){
  if (matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  const c = $('#confetti'), x = c.getContext('2d'); c.width = innerWidth; c.height = innerHeight;
  const cols = ['#f5c542','#34d399','#60a5fa','#ffffff','#ffdf7a'];
  const P = Array.from({length:130}, () => ({x:innerWidth/2, y:innerHeight/2.2, vx:(Math.random()-.5)*14, vy:-Math.random()*14-4,
    s:Math.random()*7+4, r:Math.random()*6, vr:(Math.random()-.5)*.3, c:cols[Math.floor(Math.random()*cols.length)]}));
  let t = 0;
  (function loop(){ x.clearRect(0,0,c.width,c.height); t++;
    for (const p of P){ p.vy += .35; p.x += p.vx; p.y += p.vy; p.vx *= .99; p.r += p.vr;
      x.save(); x.translate(p.x,p.y); x.rotate(p.r); x.fillStyle = p.c; x.globalAlpha = Math.max(0, 1 - t/110); x.fillRect(-p.s/2,-p.s/4,p.s,p.s/2); x.restore(); }
    if (t < 110) requestAnimationFrame(loop); else x.clearRect(0,0,c.width,c.height); })();
}
addEventListener('keydown', e => { if (e.key==='Escape') closeModal(); });
$('#modal').addEventListener('click', e => { if (e.target.id==='modal') closeModal(); });
addEventListener('resize', () => { if (curView==='journal' && jTab==='stats') drawEquity(filtered().reverse()); });

/* ------------------------------------------------------------------
   Strategien: Vorlagen, eigene Checklisten, Einrichtung
------------------------------------------------------------------ */
const TEMPLATES = [
  {name:'Fair Value Gap', icon:'▤', desc:'Rücksetzer in eine Preislücke',
   checks:['Die Lücke liegt in Richtung des Trends auf dem höheren Zeitrahmen','Der Kurs ist jetzt zurück in der Lücke','Reaktion mit Bestätigungskerze – nach Kerzenschluss','Bis zum Ziel liegt kein starkes Hindernis'],
   stop:'Hinter der Kerze, die die Lücke erzeugt hat', target:'Nächstes Hoch bzw. Tief mit Liquidität'},
  {name:'Support & Resistance', icon:'☰', desc:'Reaktion an einer wichtigen Zone',
   checks:['Die Zone hat in der Vergangenheit klar reagiert','Der Kurs ist jetzt in der Zone – nicht schon weit weg','Bestätigungskerze: Hammer, Engulfing oder Schluss über/unter der Zone','Der höhere Zeitrahmen spricht nicht dagegen'],
   stop:'Hinter der Zone plus etwas Puffer', target:'Nächste gegenüberliegende Zone'},
  {name:'Trendfolge / Breakout', icon:'↗', desc:'Mitfahren, wenn ein Level bricht',
   checks:['Klarer Trend: höhere Hochs und Tiefs (bzw. tiefere)','Ausbruch mit Kerzenschluss über bzw. unter dem Level','Direkt vor dem Kurs liegt kein großer Widerstand','Momentum oder Volumen bestätigt den Ausbruch'],
   stop:'Hinter dem letzten Swing', target:'Nachziehender Stop oder festes CRV'},
  {name:'Pullback', icon:'↩', desc:'Einstieg im Rücksetzer eines Trends',
   checks:['Der Trend auf dem höheren Zeitrahmen ist intakt','Der Kurs setzt an eine Zone oder gleitende Linie zurück','Umkehrsignal nach Kerzenschluss','Die Struktur ist nicht gebrochen'],
   stop:'Unter bzw. über dem Tief des Rücksetzers', target:'Letztes Hoch bzw. Tief des Trends'},
  {name:'Range', icon:'⇆', desc:'Seitwärtsmarkt von Rand zu Rand',
   checks:['Die Range ist klar: oben und unten mindestens zweimal berührt','Der Kurs ist am Rand der Range – nicht in der Mitte','Ablehnung am Rand nach Kerzenschluss','Kein Ausbruch auf dem höheren Zeitrahmen'],
   stop:'Knapp außerhalb der Range', target:'Mitte oder andere Seite der Range'},
  {name:'Liquidity Sweep', icon:'⤓', desc:'Fehlausbruch über ein offensichtliches Level',
   checks:['Offensichtliches Hoch bzw. Tief mit Stops dahinter','Docht holt die Liquidität und schließt wieder zurück','Strukturbruch auf dem kleineren Zeitrahmen','Einstieg im Rücksetzer, nicht im Impuls'],
   stop:'Hinter dem Docht des Sweeps', target:'Gegenüberliegende Liquidität'}
];
const sid = () => 's' + Date.now().toString(36) + Math.floor(Math.random()*1e4).toString(36);
const tplByName = n => { const q = String(n||'').toLowerCase().trim(); return q ? TEMPLATES.find(t => t.name.toLowerCase() === q) || TEMPLATES.find(t => t.name.toLowerCase().startsWith(q)) : null; };
function fromTemplate(t){ return {id: sid(), name: t.name, checks: t.checks.slice(), stop: t.stop||'', target: t.target||''}; }
function strategies(){
  if (Array.isArray(cfg.strategies) && cfg.strategies.length) return cfg.strategies;
  /* Umzug von der alten Version: eine Liste für alle Strategien */
  const names = (Array.isArray(cfg.strats) && cfg.strats.length) ? cfg.strats : DEFAULTS.strats;
  const custom = Array.isArray(cfg.checks) && cfg.checks.length && cfg.checks.join('|') !== DEFAULT_CHECKS.join('|') ? cfg.checks : null;
  cfg.strategies = names.map(n => { const t = tplByName(n) || {name:n, checks:DEFAULT_CHECKS};
    return {id: sid(), name: n, checks: (custom || t.checks).slice(), stop: t.stop||'', target: t.target||''}; });
  return cfg.strategies;
}
const stratBy = name => strategies().find(s => s.name === name) || strategies()[0];
const checksFor = name => { const s = stratBy(name); return s && s.checks.length ? s.checks : DEFAULT_CHECKS; };
function ckHtml(name){ return checksFor(name).map(c=>`<button class="ck" onclick="this.classList.toggle('on');ckUpd()"><span class="box"></span><span>${esc(c)}</span></button>`).join(''); }
const needsOnboarding = () => !cfg.onboarded && !cache.journal.length && !(Array.isArray(cfg.strategies) && cfg.strategies.length);

/* Einrichtung beim ersten Start: 1) Strategien wählen 2) Fragen prüfen 3) Grundregeln */
let ob = null;
function onboarding(stage=1){
  stopTimers(); curView = 'onboard'; progress(null); closeModal();
  if (!ob) ob = {pick: new Set(), list: [], i: 0};
  if (stage===1){
    render(`<div class="card ob">
      <div class="tag">● Einrichtung 1/3</div>
      <div class="q">Welche Strategien tradest du?</div>
      <p class="hint">Tipp alle an, die du nutzt. Jede bekommt ihre eigene Checkliste – die kannst du gleich anpassen.</p>
      <div class="tpls">${TEMPLATES.map((t,i)=>`<button class="tpl ${ob.pick.has(i)?'on':''}" data-i="${i}"><span class="ti">${t.icon}</span><b>${esc(t.name)}</b><small>${esc(t.desc)}</small></button>`).join('')}
        <button class="tpl ${ob.pick.has('own')?'on':''}" data-i="own"><span class="ti">✎</span><b>Eigene Strategie</b><small>Von null selbst beschreiben</small></button></div>
      <div class="row"><button class="btn ghost" onclick="obSkip()">Später</button><button class="btn pri" id="ob1" ${ob.pick.size?'':'disabled'} onclick="obToEdit()">Weiter</button></div>
    </div>`);
    later(()=> $$('.tpl').forEach(b => b.onclick = () => { const k = b.dataset.i==='own' ? 'own' : +b.dataset.i;
      ob.pick.has(k) ? ob.pick.delete(k) : ob.pick.add(k); b.classList.toggle('on'); $('#ob1').disabled = !ob.pick.size; }));
  }
  if (stage===2){
    const s = ob.list[ob.i];
    render(`<div class="card ob">
      <div class="tag">● Einrichtung 2/3${ob.list.length>1 ? ` · Strategie ${ob.i+1}/${ob.list.length}` : ''}</div>
      ${stratEditor(s, true)}
      <div class="row"><button class="btn ghost" onclick="obBack()">Zurück</button><button class="btn pri" onclick="obNextStrat()">${ob.i < ob.list.length-1 ? 'Nächste Strategie' : 'Weiter'}</button></div>
    </div>`);
    later(growAll);
  }
  if (stage===3){
    render(`<div class="card ob">
      <div class="tag">● Einrichtung 3/3</div>
      <div class="q">Deine Grundregeln</div>
      <p class="hint">Die App hält dich an diese Grenzen – vor allem dann, wenn du es selbst gerade nicht willst.</p>
      <div class="grid2">
        <div class="f"><label for="o_konto">Kontostand (USDT)</label><input id="o_konto" inputmode="decimal" value="${cfg.konto}"></div>
        <div class="f"><label for="o_risk">Risiko pro Trade (%)</label><input id="o_risk" inputmode="decimal" value="${cfg.risk}"></div>
        <div class="f"><label for="o_max">Max. Trades pro Tag</label><input id="o_max" inputmode="decimal" value="${cfg.maxTrades}"></div>
        <div class="f"><label for="o_crv">Mindest-CRV</label><input id="o_crv" inputmode="decimal" value="${cfg.minCRV}"></div>
      </div>
      <p class="hint small">Faustregel: 0,5–1 % Risiko pro Trade. So überstehst du auch eine Pechsträhne von 10 Verlusten.</p>
      <div class="row"><button class="btn ghost" onclick="onboarding(2)">Zurück</button><button class="btn pri" onclick="obFinish()">Los geht's</button></div>
    </div>`);
  }
}
function obToEdit(){
  const prev = new Map(ob.list.map(s => [s._k, s]));
  ob.list = [...ob.pick].sort((a,b)=> (a==='own') - (b==='own') || a-b).map(k => prev.get(k) ||
    Object.assign(k==='own' ? {id:sid(), name:'', checks:['','',''], stop:'', target:''} : fromTemplate(TEMPLATES[k]), {_k:k}));
  ob.i = 0; onboarding(2);
}
function obBack(){ readEditor(ob.list[ob.i], true); if (ob.i>0){ ob.i--; onboarding(2); } else onboarding(1); }
function obNextStrat(){
  if (!readEditor(ob.list[ob.i])) return;
  if (ob.i < ob.list.length-1){ ob.i++; onboarding(2); } else onboarding(3);
}
function obFinish(){
  const g = id => num($('#'+id).value), pos = (v,d) => isFinite(v) && v>0 ? v : d;
  Object.assign(cfg, {konto: pos(g('o_konto'),1000), risk: pos(g('o_risk'),1), maxTrades: Math.round(pos(g('o_max'),2)), minCRV: pos(g('o_crv'),2),
    strategies: ob.list.map(({_k, ...s}) => s), onboarded: true});
  S.set('lastStrat', cfg.strategies[0].name);
  saveCfg(); ob = null; confetti(); home();
}
function obSkip(){ strategies(); cfg.onboarded = true; saveCfg(); ob = null; home(); }

/* Editor für eine Strategie (Einrichtung und Einstellungen) */
function stratEditor(s, wizard){
  return `<div class="q" style="margin-bottom:14px">${wizard ? (s.name ? esc(s.name) : 'Deine eigene Strategie') : 'Strategie bearbeiten'}</div>
    <div class="f"><label for="se_name">Name</label><input id="se_name" value="${esc(s.name)}" maxlength="30" placeholder="z. B. Orderblock-Retest" autocomplete="off"></div>
    <div class="f"><label>Was muss erfüllt sein, bevor du einsteigst?</label>
      <p class="hint small" style="margin-bottom:8px">Das werden deine Fragen vor jedem Trade. Kurz und mit Ja/Nein zu beantworten – am besten 3 bis 6.</p>
      <div class="cklist" id="se_checks">${s.checks.map(c => ckRow(c)).join('')}</div>
      <button class="mini" type="button" onclick="addCk()">+ Bedingung hinzufügen</button></div>
    <div class="grid2">
      <div class="f"><label for="se_stop">Wohin kommt dein Stop?</label><input id="se_stop" value="${esc(s.stop)}" maxlength="80" placeholder="z. B. hinter das letzte Tief" autocomplete="off"></div>
      <div class="f"><label for="se_tp">Wo ist dein Ziel?</label><input id="se_tp" value="${esc(s.target)}" maxlength="80" placeholder="z. B. nächstes Hoch" autocomplete="off"></div>
    </div>
    <p class="msg" id="se_msg"></p>`;
}
const ckRow = c => `<div class="ckrow"><textarea rows="1" maxlength="140" placeholder="z. B. Kurs ist an meiner Zone" oninput="grow(this)">${esc(c)}</textarea><button type="button" aria-label="Bedingung entfernen" onclick="this.parentElement.remove()">✕</button></div>`;
function addCk(){ const l = $('#se_checks'); if (l.children.length >= 9) return; l.insertAdjacentHTML('beforeend', ckRow('')); l.lastElementChild.querySelector('textarea').focus(); }
function grow(t){ t.style.height = 'auto'; t.style.height = t.scrollHeight + 2 + 'px'; const m = $('#se_msg'); if (m) m.textContent = ''; }
function growAll(){ $$('#se_checks textarea').forEach(grow); const n = $('#se_name'); if (n) n.oninput = () => { const m = $('#se_msg'); if (m) m.textContent = ''; }; }
function readEditor(s, silent){
  const name = $('#se_name').value.trim().slice(0,30);
  const checks = $$('#se_checks textarea').map(i => i.value.replace(/\s+/g,' ').trim()).filter(Boolean).slice(0,9);
  const msg = t => { if (!silent){ const m = $('#se_msg'); m.textContent = t; m.className = 'msg bad'; } return false; };
  if (!silent){
    if (!name) return msg('Gib der Strategie einen Namen.');
    if (checks.length < 2) return msg('Trag mindestens 2 Bedingungen ein – sonst ist es keine Checkliste.');
  }
  Object.assign(s, {name: name || s.name, checks: checks.length ? checks : s.checks, stop: $('#se_stop').value.trim().slice(0,80), target: $('#se_tp').value.trim().slice(0,80)});
  return true;
}

/* Strategien in den Einstellungen */
function stratListHtml(first){
  return `<div class="lbl3 ${first?'first':''}">Deine Strategien</div>
    <div class="slist">${strategies().map(s => `<button class="sitem" onclick="editStrat('${s.id}')"><b>${esc(s.name)}</b><span>${s.checks.length} Fragen ✎</span></button>`).join('')}</div>
    <div class="row" style="margin-top:8px"><button class="btn ghost" style="flex:1" onclick="pickTemplate()">+ Strategie hinzufügen</button></div>`;
}
function editStrat(id){
  const s = strategies().find(x => x.id===id); if (!s) return;
  modal(`<div class="tag">● Strategie</div>${stratEditor(s, false)}
    <div class="row"><button class="btn ghost" onclick="closeModal()">Abbrechen</button><button class="btn pri" onclick="saveStrat('${id}')">Speichern</button></div>
    ${strategies().length > 1 ? `<div class="row" style="margin-top:8px"><button class="btn ghost danger" style="flex:1" onclick="delStrat('${id}', this)">Strategie löschen</button></div>` : ''}`);
  growAll();
}
function saveStrat(id){
  const list = strategies(), s = list.find(x => x.id===id); if (!s) return;
  const old = s.name, copy = Object.assign({}, s);
  if (!readEditor(copy)) return;
  if (list.some(x => x.id!==id && x.name===copy.name)) { const m = $('#se_msg'); m.textContent = 'Diesen Namen gibt es schon.'; m.className='msg bad'; return; }
  Object.assign(s, copy);
  if (old !== s.name){
    cache.journal.filter(t => t.strat===old).forEach(t => updateTrade(Object.assign({}, t, {strat: s.name})));
    if (S.get('lastStrat')===old) S.set('lastStrat', s.name);
  }
  saveCfg(); closeModal(); settings();
}
let delStratArmed = 0;
function delStrat(id, btn){
  if (Date.now()-delStratArmed > 4000){ delStratArmed = Date.now(); btn.textContent = 'Nochmal tippen – Trades bleiben im Journal'; return; }
  cfg.strategies = strategies().filter(x => x.id!==id); saveCfg(); closeModal(); settings();
}
function pickTemplate(){
  const have = new Set(strategies().map(s => s.name));
  modal(`<div class="tag">● Neue Strategie</div><div class="q">Womit möchtest du starten?</div>
    <div class="tpls">${TEMPLATES.filter(t => !have.has(t.name)).map(t=>`<button class="tpl" onclick="newStrat('${esc(t.name)}')"><span class="ti">${t.icon}</span><b>${esc(t.name)}</b><small>${esc(t.desc)}</small></button>`).join('')}
      <button class="tpl" onclick="newStrat('')"><span class="ti">✎</span><b>Eigene Strategie</b><small>Von null selbst beschreiben</small></button></div>
    <div class="row"><button class="btn ghost" style="flex:1" onclick="closeModal()">Abbrechen</button></div>`);
}
function newStrat(name){
  const t = tplByName(name), s = t ? fromTemplate(t) : {id:sid(), name:'', checks:['','',''], stop:'', target:''};
  modal(`<div class="tag">● Neue Strategie</div>${stratEditor(s, false)}
    <div class="row"><button class="btn ghost" onclick="closeModal()">Abbrechen</button><button class="btn pri" id="ns_go">Hinzufügen</button></div>`);
  growAll();
  $('#ns_go').onclick = () => {
    if (!readEditor(s)) return;
    if (strategies().some(x => x.name===s.name)){ const m = $('#se_msg'); m.textContent = 'Diesen Namen gibt es schon.'; m.className='msg bad'; return; }
    strategies().push(s); saveCfg(); closeModal(); settings();
  };
}
/* ------------------------------------------------------------------
   Konto: Anmelden, Registrieren, Passwort
------------------------------------------------------------------ */
function accountView(mode='login'){
  stopTimers(); curView = 'account'; progress(null); closeModal();
  const T = {
    login:  ['Willkommen bei Trade Pilot', 'Melde dich an, damit dein Journal auf allen Geräten gleich ist.', 'Anmelden'],
    signup: ['Konto erstellen', 'Mit Konto sind Journal und Einstellungen auf Mac und iPhone synchron.', 'Konto erstellen'],
    reset:  ['Passwort vergessen', 'Wir schicken dir einen Link, mit dem du ein neues Passwort setzt.', 'Link senden'],
    newpw:  ['Neues Passwort', 'Wähle ein neues Passwort mit mindestens 8 Zeichen.', 'Passwort speichern']
  }[mode];
  render(`<div class="card auth">
    <div class="tag">● Konto</div>
    <div class="q">${T[0]}</div>
    <p class="hint">${T[1]}</p>
    <form id="authf" onsubmit="event.preventDefault();authGo('${mode}')">
      ${mode!=='newpw' ? `<div class="f"><label for="a_mail">E-Mail</label><input id="a_mail" type="email" autocomplete="email" required></div>` : ''}
      ${mode!=='reset' ? `<div class="f"><label for="a_pw">Passwort</label><input id="a_pw" type="password" minlength="8" required
         autocomplete="${mode==='login'?'current-password':'new-password'}"></div>` : ''}
      <p class="msg" id="a_msg"></p>
      <div class="row"><button class="btn pri" id="a_go" type="submit">${T[2]}</button></div>
    </form>
    <div class="authlinks">
      ${mode==='login' ? `<button class="linkbtn" onclick="accountView('signup')">Neu hier? Konto erstellen</button><button class="linkbtn" onclick="accountView('reset')">Passwort vergessen?</button>`
        : mode!=='newpw' ? `<button class="linkbtn" onclick="accountView('login')">Ich habe schon ein Konto</button>` : ''}
      ${mode!=='newpw' ? `<button class="linkbtn" onclick="useLocal()">Ohne Konto nutzen (nur dieses Gerät)</button>` : ''}
    </div>
  </div>`);
  later(() => { const i = $('#a_mail') || $('#a_pw'); i && i.focus(); });
}
function authMsg(t, ok){ const m = $('#a_msg'); if (m){ m.textContent = t; m.className = 'msg ' + (ok ? 'good' : 'bad'); } }
const AUTH_ERR = e => {
  const m = (e && e.message || '').toLowerCase();
  if (m.includes('invalid login')) return 'E-Mail oder Passwort stimmt nicht.';
  if (m.includes('not confirmed')) return 'Bitte bestätige zuerst deine E-Mail (Link im Postfach).';
  if (m.includes('already registered')) return 'Diese E-Mail hat schon ein Konto. Melde dich an.';
  if (m.includes('password')) return 'Das Passwort braucht mindestens 8 Zeichen.';
  if (m.includes('rate') || m.includes('too many')) return 'Zu viele Versuche. Warte kurz und probier es dann nochmal.';
  if (m.includes('fetch') || m.includes('network')) return 'Keine Verbindung. Prüf dein Internet.';
  return 'Das hat nicht geklappt: ' + (e && e.message || 'unbekannter Fehler');
};
async function authGo(mode){
  if (!sb) return useLocal();
  const btn = $('#a_go'); btn.disabled = true;
  const mail = $('#a_mail') ? $('#a_mail').value.trim() : '', pw = $('#a_pw') ? $('#a_pw').value : '';
  const back = location.origin + location.pathname;
  try {
    if (mode==='login'){
      const { error } = await sb.auth.signInWithPassword({email:mail, password:pw});
      if (error) throw error;
      LS.set('localMode', false); home(); return;
    }
    if (mode==='signup'){
      const { data, error } = await sb.auth.signUp({email:mail, password:pw, options:{emailRedirectTo: back}});
      if (error) throw error;
      if (data.session){ LS.set('localMode', false); home(); return; }
      authMsg('Fast geschafft: Wir haben dir eine E-Mail geschickt. Bestätige den Link und melde dich dann hier an.', true);
    }
    if (mode==='reset'){
      const { error } = await sb.auth.resetPasswordForEmail(mail, {redirectTo: back});
      if (error) throw error;
      authMsg('Wenn es ein Konto mit dieser E-Mail gibt, ist der Link jetzt unterwegs.', true);
    }
    if (mode==='newpw'){
      const { error } = await sb.auth.updateUser({password: pw});
      if (error) throw error;
      home(); return;
    }
  } catch(e){ authMsg(AUTH_ERR(e)); }
  btn.disabled = false;
}
function useLocal(){ LS.set('localMode', true); home(); }
function accountBlock(){
  if (!HAS_BACKEND) return '';
  if (Sync.ok) return `<div class="lbl3">Konto</div>
    <p class="hint small">Angemeldet als <b>${esc(Sync.email)}</b>${Sync.queue.length ? ` · ${Sync.queue.length} Änderungen warten auf Internet` : ''}</p>
    <div class="row" style="margin-top:0"><button class="btn ghost" style="flex:1" onclick="logout(this)">Abmelden</button>
    <button class="btn ghost danger" style="flex:1" onclick="deleteAccount(this)">Konto löschen</button></div>`;
  return `<div class="lbl3">Konto</div><p class="hint small">Nicht angemeldet – deine Daten liegen nur auf diesem Gerät.</p>
    <div class="row" style="margin-top:0"><button class="btn ghost" style="flex:1" onclick="accountView('login')">Anmelden oder Konto erstellen</button></div>`;
}
function wipeLocal(){
  cache.journal = []; LS.set('journal', []);
  STATE_KEYS.forEach(k => { cache[k] = null; LS.set(k, null); });
  Sync.queue = []; Sync.saveQ(); Sync.dirty = false; LS.set('dirty', false);
  Object.keys(cfg).forEach(k => delete cfg[k]); Object.assign(cfg, freshDefaults());
}
async function logout(btn){
  await Sync.drain();
  if (Sync.queue.length){ btn.textContent = 'Erst online gehen – Änderungen nicht hochgeladen'; return; }
  await sb.auth.signOut();
  Sync.ok = false; Sync.uid = null; wipeLocal(); LS.set('localMode', false);
  accountView('login');
}
let delAccArmed = 0;
async function deleteAccount(btn){
  if (Date.now() - delAccArmed > 4000){ delAccArmed = Date.now(); btn.textContent = 'Wirklich? Alles wird gelöscht – nochmal tippen'; return; }
  btn.disabled = true;
  const { error } = await sb.rpc('delete_my_account');
  if (error){ btn.disabled = false; btn.textContent = 'Löschen fehlgeschlagen – später nochmal'; return; }
  try { await sb.auth.signOut(); } catch(e){}
  Sync.ok = false; Sync.uid = null; wipeLocal(); LS.set('localMode', false);
  accountView('signup');
}

/* ------------------------------------------------------------------
   Backup: speichern und laden
------------------------------------------------------------------ */
async function saveFile(name, data, type){
  const blob = new Blob([data], {type});
  try {
    const file = new File([blob], name, {type});
    if (/iPhone|iPad|iPod/.test(navigator.userAgent) && navigator.canShare && navigator.canShare({files:[file]})){
      await navigator.share({files:[file], title:name}); return;
    }
  } catch(e){ if (e && e.name === 'AbortError') return; }
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = name;
  document.body.appendChild(a); a.click(); setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 1000);
}
function exportBackup(){
  const data = {app:'trade-pilot', v:1, exported:new Date().toISOString(), cfg:JSON.parse(JSON.stringify(cfg)),
    journal: journalList(), stops: S.get('stops', []), lastStrat: S.get('lastStrat', null)};
  saveFile(`trade_pilot_backup_${today()}.json`, JSON.stringify(data, null, 1), 'application/json');
}
function importBackup(inp){
  const f = inp.files && inp.files[0]; if (!f) return;
  const msg = $('#impmsg');
  const r = new FileReader();
  r.onload = () => {
    try {
      const d = JSON.parse(r.result);
      if (!d || d.app !== 'trade-pilot' || !Array.isArray(d.journal)) throw new Error('format');
      const have = new Set(cache.journal.map(j => j.id));
      let added = 0;
      d.journal.forEach(t => {
        if (!t || !t.date || !isFinite(+t.R)) return;
        const id = String(t.id || ('j' + Date.parse(t.date))).slice(0, 40);
        if (have.has(id)) return;
        const clean = Object.assign({}, t, {id, R:+t.R}); have.add(id);
        cache.journal.push(clean); Sync.enqueue('put', id); added++;
      });
      LS.set('journal', cache.journal);
      if (d.cfg && typeof d.cfg === 'object'){
        const keep = {konto: cfg.konto};
        Object.assign(cfg, d.cfg, d.cfg.konto ? {} : keep);
        if (!Array.isArray(d.cfg.strategies)){ delete cfg.strategies; strategies(); }   /* altes Backup: Strategien daraus übernehmen */
        cfg.onboarded = true; saveCfg();
      }
      if (Array.isArray(d.stops)) S.set('stops', d.stops.concat(S.get('stops', [])).slice(0, 150));
      if (d.lastStrat) S.set('lastStrat', d.lastStrat);
      Sync.drain();
      msg.textContent = `Fertig: ${added} Trades übernommen, Einstellungen geladen.`; msg.className = 'msg good';
      setTimeout(settings, 1600);
    } catch(e){ msg.textContent = 'Das ist keine gültige Trade-Pilot-Backup-Datei.'; msg.className = 'msg bad'; }
    inp.value = '';
  };
  r.readAsText(f);
}

/* ------------------------------------------------------------------
   Start der App
------------------------------------------------------------------ */
async function boot(){
  if (!HAS_BACKEND){ home(); startSync(); return; }
  let session = null;
  try { session = (await sb.auth.getSession()).data.session; } catch(e){}
  if (!session && needsLogin()) accountView('login'); else home();
  startSync();
}
boot();
if ('serviceWorker' in navigator && location.protocol === 'https:'){
  navigator.serviceWorker.register('sw.js').catch(()=>{});
  let reloaded = !navigator.serviceWorker.controller;   /* erster Besuch: nicht neu laden */
  navigator.serviceWorker.addEventListener('controllerchange', () => {
    if (reloaded || curView === 'flow' || curView === 'intrade' || curView === 'end') return;
    reloaded = true; location.reload();
  });
}
