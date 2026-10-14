/* ------------------------------------------------------------------
   Speicher: lokal (sofort, offline) + Supabase (Konto & Sync)
------------------------------------------------------------------ */
const LS = {
  get(k, d){ try{ const v = localStorage.getItem('tp_'+k); return v===null ? d : JSON.parse(v); }catch(e){ return d; } },
  set(k, v){ try{ localStorage.setItem('tp_'+k, JSON.stringify(v)); }catch(e){} }
};
const STATE_KEYS = ['cfg','active','cooldownUntil','lastBias','stops','resetAt','lastStrat','days','weekGoal','reviews'];
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
const DEFAULT_CHECKS_EN = [
  'The setup matches my tested strategy exactly',
  'Price is in my zone now – not already far away',
  'Confirmation signal is there – after the candle closes, not before',
  'The higher timeframe does not argue against the trade'
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
    const snap = () => JSON.stringify([cache.journal, STATE_KEYS.map(k => cache[k])]), before = snap();
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
    badge('ok'); if (snap() !== before) refreshView();   /* nur neu zeichnen, wenn sich wirklich etwas geändert hat */
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
/* ==================================================================
   Sprache: Deutsch / Englisch
   Übersetzt alles, was angezeigt wird – Zahlen bleiben erhalten, eigene Eingaben (Coins, Notizen, Strategienamen) bleiben unverändert.
   ================================================================== */
const I18N_READY = true;
/* Unterstützte Sprachen: [Code, Eigenname, Flagge, Locale]. Wörterbücher außer DE/EN liegen in lang/xx.js und werden nur bei Bedarf geladen. */
const LANGS = [['de','Deutsch','🇩🇪','de-DE'],['en','English','🇬🇧','en-US'],['es','Español','🇪🇸','es-ES'],['pt','Português','🇧🇷','pt-BR'],['fr','Français','🇫🇷','fr-FR'],
  ['tr','Türkçe','🇹🇷','tr-TR'],['ru','Русский','🇷🇺','ru-RU'],['zh','中文','🇨🇳','zh-CN'],['ja','日本語','🇯🇵','ja-JP'],['ko','한국어','🇰🇷','ko-KR']];
const LANG_CODES = LANGS.map(l => l[0]);
const langOk = l => LANG_CODES.includes(l) && (l === 'de' || l === 'en' || (window.I18N_X && window.I18N_LANG === l));
let LANG = (() => { try { const q = new URLSearchParams(location.search).get('lang'); if (LANG_CODES.includes(q)) LS.set('lang', q); } catch(e){}
  const s = LS.get('lang', null); if (LANG_CODES.includes(s)) return langOk(s) ? s : 'en';
  const nav = String(navigator.language || 'en').slice(0, 2).toLowerCase();
  return nav === 'de' ? 'de' : langOk(nav) ? nav : 'en'; })();
const LOC = () => (LANGS.find(l => l[0] === LANG) || LANGS[1])[3];
const DEC = (1.5).toLocaleString(LOC()).replace(/[^.,]/g, '') || '.';      // Dezimaltrenner der Sprache
const I18N_EN = {};            // wird unten befüllt
const NUM_RE = /[+\-−]?\d(?:[\d.,:]|[\u00a0\u202f](?=\d{3}(?!\d)))*%?/g;   // auch 1 000,50 (FR/RU)
const i18nKey = s => s.replace(NUM_RE, '#').replace(/\s+/g, ' ').trim();
function tr(s){
  if (LANG === 'de' || s == null) return s;
  const raw = String(s), lead = raw.match(/^\s*/)[0], tail = raw.match(/\s*$/)[0], core = raw.trim();
  if (!core) return raw;
  const k = i18nKey(core), X = LANG === 'en' ? null : window.I18N_X;
  const en = (X && X[k] !== undefined) ? X[k] : I18N_EN[k];
  if (en === undefined) return trParts(raw);
  const nums = core.match(NUM_RE) || []; let i = 0;
  return lead + en.replace(/#/g, () => nums[i++] ?? '') + tail;
}
/* Für zusammengesetzte Texte mit eigenen Inhalten (z. B. „SOL LONG · Meine Strategie · Einstieg 1,4“) */
const I18N_PARTS = [];
function trParts(s){ let o = s; const X = LANG === 'en' ? null : window.I18N_PARTS_X; I18N_PARTS.forEach(([re, en], i) => { o = o.replace(re, (X && X[i]) || en); }); return o; }
const I18N_ATTRS = ['placeholder', 'aria-label', 'title', 'alt'];
function localize(root){
  if (LANG === 'de' || !root) return;
  const w = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {acceptNode: n => n.parentElement && /^(SCRIPT|STYLE|TEXTAREA)$/.test(n.parentElement.tagName) ? NodeFilter.FILTER_REJECT : NodeFilter.FILTER_ACCEPT});
  const nodes = []; while (w.nextNode()) nodes.push(w.currentNode);
  nodes.forEach(n => { const t = tr(n.data); if (t !== n.data) n.data = t; });
  const els = root.nodeType === 1 ? [root, ...root.querySelectorAll('*')] : [];
  els.forEach(el => I18N_ATTRS.forEach(a => { if (el.hasAttribute && el.hasAttribute(a)){ const v = el.getAttribute(a), t = tr(v); if (t !== v) el.setAttribute(a, t); } }));
}
let i18nObs = null;
function startI18n(){
  document.documentElement.lang = LANG;
  if (LANG === 'de') return;
  document.title = 'NoTilt – ' + tr('Trading-Checkliste & Journal');
  localize(document.body);
  i18nObs = new MutationObserver(muts => {
    for (const m of muts){
      if (m.type === 'characterData'){ const t = tr(m.target.data); if (t !== m.target.data) m.target.data = t; }
      else if (m.type === 'attributes'){ const v = m.target.getAttribute(m.attributeName), t = tr(v); if (v != null && t !== v) m.target.setAttribute(m.attributeName, t); }
      else m.addedNodes.forEach(n => n.nodeType === 3 ? (() => { const t = tr(n.data); if (t !== n.data) n.data = t; })() : localize(n));
    }
  });
  i18nObs.observe(document.body, {subtree: true, childList: true, characterData: true, attributes: true, attributeFilter: I18N_ATTRS});
}
function setLang(l){ LS.set('lang', l); location.href = location.pathname; }
function langPicker(){
  const pick = l => `if('${l}'!==LANG){ if (curView==='settings' && typeof saveSettings==='function') saveSettings(true); setLang('${l}'); } else closeModal();`;
  modal(`<div class="tag">● Sprache</div><div class="q">Sprache wählen</div>
    <div class="langlist" translate="no">${LANGS.map(([c, n, f]) => `<button type="button" class="${c === LANG ? 'on' : ''}" data-l="${c}" onclick="${pick(c)}"><span class="lf">${f}</span><span class="ln">${n}</span>${c === LANG ? '<span class="lc">✓</span>' : ''}</button>`).join('')}</div>
    <div class="row"><button class="btn ghost" style="flex:1" onclick="closeModal()">Abbrechen</button></div>`);
}
const langName = () => { const l = LANGS.find(x => x[0] === LANG) || LANGS[0]; return l[2] + ' ' + l[1]; };
/* ==================================================================
   Englisches Wörterbuch. Schlüssel = deutscher Text, Zahlen als #.
   ================================================================== */
Object.assign(I18N_EN, {
  /* Start */
  'Bereit für einen Trade?': 'Ready for a trade?',
  'Unter einer Minute. Erst die Checkliste, dann der Trade.': 'Under a minute. Checklist first, then the trade.',
  'TRADEN': 'TRADE',
  'CHECKLISTE': 'CHECKLIST',
  'TAGES-CHECK': 'DAILY CHECK',
  'PAUSE': 'BREAK',
  'LIMIT': 'LIMIT',
  'Tages-Check': 'Daily check',
  'offen': 'open',
  'Bias': 'Bias',
  'Trades': 'Trades',
  'Heute': 'Today',
  'Session': 'Session',
  'ruhig': 'quiet',
  'News': 'News',
  'Pause': 'Break',
  'Serie': 'Streak',
  'Wochenrückblick': 'Weekly review',
  '# Minuten – was hat funktioniert, was nicht? Ein Ziel für nächste Woche.': '# minutes – what worked, what didn\'t? One goal for next week.',
  'Fokus heute:': 'Focus today:',
  'Trade ohne Checkliste nachtragen': 'Log a trade without checklist',
  '„Die Checkliste denkt für dich, wenn die Emotion übernehmen will.“': '“The checklist thinks for you when emotions want to take over.”',
  '„Ein einzelner Trade ist Zufall. Deine Edge zeigt sich erst über hunderte.“': '“A single trade is random. Your edge only shows over hundreds.”',
  '„Warte, bis das Setup zu dir kommt – nicht umgekehrt.“': '“Wait for the setup to come to you – not the other way around.”',
  '„Ein Verlust ist kein Urteil über dich, sondern ein Datenpunkt.“': '“A loss is not a verdict on you – it\'s a data point.”',
  '„Kein Trade ist auch eine Entscheidung.“': '“No trade is a decision too.”',
  '„Wer Trading wie ein Business führt, misst seine Zahlen.“': '“If you run trading like a business, you measure your numbers.”',

  /* Navigation */
  'Start': 'Home',
  'Journal': 'Journal',
  'Profil': 'Profile',
  '← Zurück': '← Back',
  'Zurück': 'Back',
  'Abbrechen': 'Cancel',
  'Weiter': 'Next',
  'Speichern': 'Save',
  '✓ Gespeichert': '✓ Saved',
  'Verstanden': 'Got it',
  'Später': 'Later',
  'Fertig': 'Done',
  'Ja': 'Yes',
  'Nein': 'No',
  'An': 'On',
  'Aus': 'Off',
  'und': 'and',
  'Verbinde…': 'Connecting…',
  'Synchron': 'Synced',
  'Offline': 'Offline',
  'Nur dieses Gerät': 'This device only',
  'Nicht angemeldet': 'Not signed in',

  /* Tages-Check */
  '◷ Tages-Check · einmal pro Tag': '◷ Daily check · once a day',
  'Wie ist die Lage heute?': 'What does the market look like today?',
  'Routine: Was ist die letzten Tage passiert? Daily → #h → #m, Liquidität markieren, News prüfen.': 'Routine: what happened over the last few days? Daily → #h → #m, mark liquidity, check the news.',
  'Struktur auf Daily / #h (primäre Swing Levels)': 'Structure on daily / #h (major swing levels)',
  'Bullisch': 'Bullish',
  'Bärisch': 'Bearish',
  'Unklar': 'Unclear',
  'Wichtige News heute (# Sterne)?': 'High-impact news today (# stars)?',
  'Keine': 'None',
  'Ja, um …': 'Yes, at …',
  'Uhrzeit der News': 'Time of the news',
  'Dein Bias heute': 'Your bias today',
  'Long': 'Long',
  'Short': 'Short',
  'Neutral': 'Neutral',
  'Fokus-Coins mit Alert (optional, mit Komma)': 'Focus coins with alerts (optional, comma-separated)',
  'z. B. XRP, SOL, ENA': 'e.g. XRP, SOL, ENA',
  'Gedanken für heute (optional)': 'Thoughts for today (optional)',
  'z. B. Montag, erster Börsenstart der Woche – erst beobachten': 'e.g. Monday, first market open of the week – watch first',
  'Bias speichern': 'Save bias',

  /* Ablauf */
  'Atmen': 'Breathe',
  'Kopf': 'Mind',
  'Setup': 'Setup',
  'Plan': 'Plan',
  'Los': 'Go',
  '◎ Atmen · # Sekunden': '◎ Breathe · # seconds',
  'Ein tiefer Seufzer': 'One deep sigh',
  'Durch die Nase einatmen, kurz nachatmen, dann lange durch den Mund aus. Mit Ton kannst du dabei die Augen schließen.': 'Breathe in through your nose, top up with a short second breath, then breathe out slowly through your mouth. With sound on, you can close your eyes.',
  'Durch die Nase einatmen, kurz nachatmen, dann lange durch den Mund aus.': 'Breathe in through your nose, top up with a short second breath, then breathe out slowly through your mouth.',
  'Einatmen': 'Breathe in',
  'Tief einatmen': 'Breathe in deeply',
  'Nochmal kurz nach': 'One more short breath',
  'Langsam ausatmen …': 'Breathe out slowly …',
  'Überspringen': 'Skip',
  '● Kopf': '● Mind',
  'Was fühlst du gerade?': 'How are you feeling right now?',
  'Benenne es ehrlich. Schon das Benennen dämpft die Emotion.': 'Name it honestly. Just naming it calms the emotion.',
  'Ruhig & klar': 'Calm & clear',
  'Ungeduldig – will nichts verpassen': 'Impatient – don\'t want to miss out',
  'Frust nach einem Verlust': 'Frustrated after a loss',
  'Euphorisch – der wird groß': 'Euphoric – this one\'s going to be big',
  'Langeweile – will einfach traden': 'Bored – just want to trade',
  'Nervös / ängstlich': 'Nervous / anxious',
  'Ruhig': 'Calm',
  'Nervös': 'Nervous',
  '▲ Achtung': '▲ Careful',
  'Angst im Spiel': 'Fear is in play',
  'Angst lässt dich zu spät rein, zu früh raus oder gar nicht handeln. Meist ist das Risiko zu groß, damit ein Verlust dich kaltlässt.': 'Fear gets you in too late, out too early, or not at all. Usually the risk is too big for a loss to leave you cold.',
  'Für diesen Trade halbiere ich dein Risiko auf': 'For this trade I\'m cutting your risk in half to',
  'Lieber nicht': 'Better not',
  'Mit halbem Risiko weiter': 'Continue with half the risk',
  '● Setup': '● Setup',
  'Alles erfüllt?': 'Everything in place?',
  '▲ Long': '▲ Long',
  '▼ Short': '▼ Short',
  'Strategie': 'Strategy',
  'Du hast das Setup schon bestätigt – Haken sind gesetzt.': 'You already confirmed the setup – the boxes are ticked.',
  'Etwas fehlt': 'Something\'s missing',
  'Setup bestätigt': 'Setup confirmed',
  '⚠ US-Börsenstart (# Uhr) in Reichweite: oft ein kurzer Liquidity Grab.': '⚠ US market open (#) is close: often a quick liquidity grab.',
  '● Plan': '● Plan',
  '– Einstieg, Stop, Ziel': '– entry, stop, target',
  'Coin': 'Coin',
  'z. B. XRP': 'e.g. XRP',
  'Einstieg': 'Entry',
  'Stop-Loss': 'Stop loss',
  'Ziel': 'Target',
  'Stop:': 'Stop:',
  '· Ziel:': '· Target:',
  'Konto # USDT · Risiko # % · Hebel #x · ändern': 'Account # USDT · Risk # % · Leverage #x · change',
  'Konto (USDT)': 'Account (USDT)',
  'Risiko (%)': 'Risk (%)',
  'Hebel': 'Leverage',
  'CRV': 'R:R',
  'Risiko USDT': 'Risk USDT',
  'Position USDT': 'Position USDT',
  'Liquidation': 'Liquidation',
  'Einstieg, Stop und Ziel eintragen.': 'Enter entry, stop and target.',
  'Long: Stop unter, Ziel über dem Einstieg.': 'Long: stop below, target above the entry.',
  'Short: Stop über, Ziel unter dem Einstieg.': 'Short: stop above, target below the entry.',
  'CRV # – dein Minimum ist #': 'R:R # – your minimum is #',
  'CRV # – dein Minimum ist #.': 'R:R # – your minimum is #.',
  'Liquidation bei ca. # – vor deinem Stop.': 'Liquidation at about # – before your stop.',
  'Zu wenig Kapital für diese Position. Hebel oder Risiko anpassen.': 'Not enough capital for this position. Adjust leverage or risk.',
  'Mehr als # % Risiko – bewusst so?': 'More than # % risk – on purpose?',
  'Plan ist stimmig.': 'The plan checks out.',
  'Plan steht': 'Plan is set',
  'So nicht traden': 'Don\'t trade it like this',
  '● Los': '● Go',
  'Wie gut ist das Setup? Jetzt bewerten, nicht erst nach dem Ergebnis.': 'How good is the setup? Rate it now, not after the result.',
  'Lehrbuch – alles glasklar': 'Textbook – crystal clear',
  'Gut – kleine Abstriche': 'Good – minor flaws',
  'Grenzwertig': 'Borderline',
  'Wenn': 'If',
  'dann': 'then',
  'der Kurs gegen mich läuft,': 'price moves against me,',
  'bleibt der Stop, wo er ist.': 'the stop stays where it is.',
  'ich früher raus oder nachlegen will,': 'I want to exit early or add to the position,',
  'tippe ich den Impuls an, statt zu handeln.': 'I tap the impulse instead of acting on it.',
  'der Trade vorbei ist,': 'the trade is over,',
  'mache ich # Minuten Pause.': 'I take a # minute break.',
  'Dein Wochenziel': 'Your weekly goal',
  'Stop & Ziel gesetzt – ich bin drin': 'Stop & target set – I\'m in',
  'Doch nicht eingestiegen': 'Didn\'t enter after all',

  /* Im Trade */
  'Im Trade': 'In trade',
  'Impuls? Erst hier tippen:': 'Got an urge? Tap here first:',
  'Stop verschieben': 'Move stop',
  'Früher raus': 'Exit early',
  'Position vergrößern': 'Add to position',
  'Nervös am Chart': 'Nervous at the chart',
  'Trade beendet?': 'Trade closed?',
  'Manuell': 'Manual',
  'Du musst nichts tun. Stop und Ziel arbeiten für dich.': 'You don\'t have to do anything. Stop and target are working for you.',
  'Ein Verlust schmerzt doppelt so stark wie ein gleich großer Gewinn – deshalb will dein Kopf gleich Dummes tun. Lass ihn reden.': 'A loss hurts twice as much as an equal gain – that\'s why your mind wants to do something dumb. Let it talk.',
  'Unrealisierter Gewinn ist nicht dein Geld. Halte dich an dein Ziel.': 'Unrealized profit isn\'t your money. Stick to your target.',
  'Starrst du seit Minuten auf den Chart? Alarm setzen, Handy weg.': 'Been staring at the chart for minutes? Set an alert, put the phone away.',
  'Der Trade ist im Minus? Normal. Der Stop bleibt, wo er ist.': 'Trade is in the red? Normal. The stop stays where it is.',
  '⚠ Impuls erkannt': '⚠ Impulse detected',
  'Merke:': 'Remember:',
  'Ich bleibe bei meinem Plan': 'I\'m sticking to my plan',
  'Stop weiter weg schieben?': 'Move the stop further away?',
  'Das ist Verlustaversion: Du hältst Verlierer zu lange, in der Hoffnung „der dreht schon“.': 'That\'s loss aversion: you hold losers too long, hoping “it\'ll turn around”.',
  'Du hast einen Plan. Jag deine Verluste nicht weiter ins Minus.': 'You have a plan. Don\'t chase your losses deeper into the red.',
  'Gewinne früher mitnehmen?': 'Take profits early?',
  'Die Angst, den Buchgewinn zu verlieren, lässt dich Gewinner zu früh verkaufen. Über hunderte Trades kostet genau das deine Edge.': 'The fear of losing paper profits makes you sell winners too early. Over hundreds of trades, that\'s exactly what costs you your edge.',
  'Du hast ein Ziel – halte es.': 'You have a target – hold it.',
  'Position vergrößern?': 'Add to the position?',
  'Das ist Gier. Dein Risiko war mit dem Plan festgelegt.': 'That\'s greed. Your risk was set with the plan.',
  'Nervös am Chart?': 'Nervous at the chart?',
  'Deine Orders sind gesetzt – das reicht.': 'Your orders are in – that\'s enough.',
  'Handy weg oder Laptop zu. Preisalarme bei Ziel und Stop setzen. Kurzer Spaziergang.': 'Phone away or laptop closed. Set price alerts at target and stop. Take a short walk.',
  'Der Plan entscheidet, nicht das Gefühl.': 'The plan decides, not the feeling.',

  /* Nach dem Trade */
  '● Nach dem Trade': '● After the trade',
  'Ziel erreicht': 'Target hit',
  'Stop getroffen – ein Datenpunkt': 'Stop hit – one data point',
  'Manuell geschlossen': 'Closed manually',
  'Ergebnis in R': 'Result in R',
  'z. B. #': 'e.g. #',
  'An welche Regeln hast du dich gehalten? Antippen, was du gebrochen hast.': 'Which rules did you follow? Tap the ones you broke.',
  'Bedingungen': 'Conditions',
  'War ein Fehler dabei? (optional, ehrlich hilft dir am meisten)': 'Any mistakes? (optional – honesty helps you most)',
  'FOMO-Einstieg': 'FOMO entry',
  'Zu früh raus': 'Exited too early',
  'Stop verschoben': 'Moved stop',
  'Position zu groß': 'Position too big',
  'Nachgelegt': 'Added to position',
  'Kein echtes Setup': 'No real setup',
  'Revenge-Trade': 'Revenge trade',
  '# % besser beim nächsten Mal (optional)': '# % better next time (optional)',
  'z. B. auf Kerzenschluss warten': 'e.g. wait for the candle close',
  'Chart-Screenshots (optional)': 'Chart screenshots (optional)',
  '＋ Bild auswählen': '＋ Choose image',
  '# ausgewählt': '# selected',
  'Screenshots speichern geht mit Konto.': 'Saving screenshots requires an account.',
  'Chart-Link (optional, z. B. TradingView)': 'Chart link (optional, e.g. TradingView)',
  'Chart-Link': 'Chart link',
  '◎ Pause': '◎ Break',
  'Handy weg. Kurz raus.': 'Phone away. Step outside.',
  'Gespeichert. Kein Übermut-Trade.': 'Saved. No overconfidence trade.',
  'Du bist noch in der Pause.': 'You\'re still on your break.',
  'Nach Verlusten steigt die Risikobereitschaft messbar. Die Pause schützt dich davor.': 'After losses, risk-taking measurably goes up. The break protects you from that.',
  'Auch nach Gewinnen: Der nächste Trade kommt nur über die Checkliste.': 'After wins too: the next trade only goes through the checklist.',
  '✓ Plan eingehalten – das zählt, nicht das Ergebnis.': '✓ Plan followed – that\'s what counts, not the result.',

  /* Stopps */
  '■ Kein Trade': '■ No trade',
  'Langeweile ist kein Setup': 'Boredom is not a setup',
  'Du willst traden, weil du etwas tun willst – nicht, weil dein System ein Setup zeigt.': 'You want to trade because you want to do something – not because your system shows a setup.',
  'Ein erzwungenes, mittelmäßiges Setup ist meistens der teuerste Trade des Tages.': 'A forced, mediocre setup is usually the most expensive trade of the day.',
  'Revenge-Trading-Gefahr': 'Revenge trading risk',
  'Nach Verlusten gehen Trader nachweislich mehr Risiko ein. Genau jetzt ist der schlechteste Moment für den nächsten Trade.': 'After losses, traders demonstrably take more risk. Right now is the worst moment for the next trade.',
  'Erst Abstand gewinnen: Laptop zu, kurz raus, dann mit klarem Kopf neu bewerten.': 'Get some distance first: laptop shut, step outside, then reassess with a clear head.',
  'FOMO erkannt': 'FOMO detected',
  'Die Angst, etwas zu verpassen, macht dich fast immer zum Käufer kurz vor der Umkehr.': 'The fear of missing out almost always makes you the buyer right before the reversal.',
  'FOMO ist ein alter Herden-Instinkt. Am Finanzmarkt macht er dich zum Käufer am Hoch.': 'FOMO is an old herd instinct. In financial markets it makes you the buyer at the top.',
  'Gier am Steuer': 'Greed at the wheel',
  '„Der wird sicher groß“ führt zu zu großen Positionen und Setups, die dein System nie traden würde.': '“This one\'s definitely going to be big” leads to oversized positions and setups your system would never trade.',
  'Gier fühlt sich im Moment wie ein guter Instinkt an. Deshalb entscheidet die Checkliste, nicht das Gefühl.': 'Greed feels like a good instinct in the moment. That\'s why the checklist decides, not the feeling.',
  'Bias heute: neutral': 'Bias today: neutral',
  'Dein Tages-Check sagt: keine klare Richtung. Heute kein Trade – und das ist völlig in Ordnung.': 'Your daily check says: no clear direction. No trade today – and that\'s completely fine.',
  'Keine klare Richtung ist ein vollkommen gültiges Ergebnis. Abwarten ist auch eine Position.': 'No clear direction is a perfectly valid result. Waiting is a position too.',
  'Gegen deinen Bias': 'Against your bias',
  'Du willst gegen die Richtung deines Tages-Checks traden.': 'You want to trade against the direction of your daily check.',
  'Der Bias entscheidet, in welche Richtung du suchst. Hat sich der Markt klar gedreht, mach den Tages-Check neu – aber nicht mitten im Impuls.': 'Your bias decides which direction you look for. If the market has clearly turned, redo the daily check – but not in the middle of an impulse.',
  'Setup unvollständig': 'Setup incomplete',
  'Mindestens eine Bedingung deines Systems ist nicht erfüllt. Kein Trade.': 'At least one condition of your system isn\'t met. No trade.',
  'Nicht alles abgehakt heißt: kein Trade. Alarm setzen und warten.': 'Not everything ticked means: no trade. Set an alert and wait.',
  'Chance-Risiko zu schlecht': 'Risk-reward too poor',
  'Dein Ziel ist im Verhältnis zum Stop zu nah.': 'Your target is too close relative to your stop.',
  'Nur Trades mit klarem Mindest-CRV.': 'Only trades with a clear minimum R:R.',
  'Liquidation vor dem Stop': 'Liquidation before the stop',
  'Mit diesem Hebel wirst du liquidiert, bevor dein Stop überhaupt greift.': 'With this leverage you get liquidated before your stop even triggers.',
  'Weniger Hebel oder engerer Stop.': 'Less leverage or a tighter stop.',
  'Trade-Limit für heute erreicht': 'Trade limit reached for today',
  'Du hast heute so viele Trades gemacht, wie du dir vorgenommen hast.': 'You\'ve made as many trades today as you planned.',
  'Mehr Trades, als dein System hergibt, heißt: Du tradest Gefühle, nicht Setups.': 'More trades than your system gives you means you\'re trading feelings, not setups.',
  'Verlust-Limit für heute erreicht': 'Daily loss limit reached',
  'Du hast heute dein maximales Minus erreicht. Für heute ist Schluss.': 'You\'ve hit your maximum loss for today. That\'s it for today.',
  'Nach Verlusten steigt die Risikobereitschaft – genau dann schützt dich eine harte Grenze.': 'After losses, risk-taking goes up – that\'s exactly when a hard limit protects you.',
  'News-Fenster': 'News window',
  'News um # Uhr. Kein neuer Trade # Minuten davor und danach.': 'News at #. No new trade # minutes before and after.',
  'Bei News: erst #–# Minuten abwarten und beobachten.': 'With news: wait and watch for #–# minutes first.',
  'US-Börsenstart läuft': 'US market open in progress',
  'Die US-Börse hat um # Uhr geöffnet. In den ersten # Minuten keine neuen Trades.': 'The US stock market opened at # (your time). No new trades in the first # minutes.',
  'Die ersten Minuten nach dem Börsenstart sind oft hektisch und täuschen. Erst beobachten, dann handeln.': 'The first minutes after the open are often hectic and misleading. Watch first, then act.',
  '# Minuten Pause gestartet.': '# minute break started.',
  'Tages-Check neu machen': 'Redo daily check',

  /* Journal */
  '● Journal': '● Journal',
  'Übersicht': 'Overview',
  'Trades (#)': 'Trades (#)',
  'Alles': 'All',
  'Alle Strategien': 'All strategies',
  '# Tage': '# days',
  '# Tag': '# day',
  'Treffer': 'Win rate',
  'Ø R / Trade': 'Avg R / trade',
  'Profit-Faktor': 'Profit factor',
  'Netto R': 'Net R',
  'Regeltreue': 'Rule adherence',
  'Kapitalkurve in R': 'Equity curve in R',
  'Erkenntnisse': 'Insights',
  'Edge oder Zufall?': 'Edge or luck?',
  'Dein Ø liegt mit # % Wahrscheinlichkeit zwischen': 'With # % probability, your average lies between',
  'pro Trade.': 'per trade.',
  'Edge belegt': 'Edge confirmed',
  '– selbst im ungünstigen Fall liegst du im Plus.': '– even in the worst case you\'re in profit.',
  'So verliert das System': 'This system is losing',
  '– auch im günstigen Fall liegt der Ø unter null.': '– even in the best case, the average is below zero.',
  'Noch offen': 'Still open',
  '– ca. # weitere Trades nötig, bis das Ergebnis kein Zufall mehr sein kann.': '– about # more trades needed before the result can\'t be luck anymore.',
  '– ca. sehr viele weitere Trades nötig, bis das Ergebnis kein Zufall mehr sein kann.': '– a lot more trades needed before the result can\'t be luck anymore.',
  '– mehr Trades nötig, bis das Ergebnis kein Zufall mehr sein kann.': '– more trades needed before the result can\'t be luck anymore.',
  'Ab # Trades zeigt dir die App, in welcher Spanne dein echter Ø R pro Trade wahrscheinlich liegt.': 'From # trades on, the app shows you the range your true average R per trade most likely falls in.',
  'Ab # Trades rechnet die App, ab etwa #–# wird es belastbar.': 'The app starts calculating at # trades; from about #–# it becomes reliable.',
  'Teuerster Fehler:': 'Costliest mistake:',
  'Teuerster Fehler': 'Costliest mistake',
  '– #× · kostet dich ca.': '– #× · costs you about',
  '– #× · kostet dich geschätzt': '– #× · costs you an estimated',
  'Plan gehalten: Ø': 'Plan followed: avg',
  '· Plan gebrochen: Ø': '· Plan broken: avg',
  'Ruhig getradet: Ø': 'Traded calm: avg',
  '· Nervös: Ø': '· Nervous: avg',
  'Mit Checkliste: Ø': 'With checklist: avg',
  '· Ohne: Ø': '· Without: avg',
  'Trades mit Impulsen: Ø': 'Trades with impulses: avg',
  '· ohne: Ø': '· without: avg',
  'Am häufigsten gebrochen:': 'Most often broken:',
  'Von der Checkliste verhinderte Trades:': 'Trades prevented by the checklist:',
  'Ab ein paar Trades pro Gruppe zeigt dir die App hier, was bei dir funktioniert – z. B. Plan gehalten vs. gebrochen, S- vs. B-Setups, ruhig vs. nervös.': 'Once there are a few trades per group, the app shows you what works for you – e.g. plan followed vs. broken, S vs. B setups, calm vs. nervous.',
  'Keine Trades in diesem Zeitraum. Trades aus der Checkliste landen hier automatisch – andere kannst du nachtragen.': 'No trades in this period. Trades from the checklist land here automatically – you can log others manually.',
  '+ Trade nachtragen': '+ Log trade',
  '⇩ Von Börse importieren': '⇩ Import from exchange',
  'CSV exportieren': 'Export CSV',
  'ohne Checkliste': 'without checklist',
  'nachgetragen': 'logged manually',
  '✎ Trade bearbeiten': '✎ Edit trade',
  'Für Trades ohne Checkliste. Sie zählen in deine Statistik, damit sie ehrlich bleibt.': 'For trades without the checklist. They count in your stats so they stay honest.',
  'Datum': 'Date',
  'Richtung': 'Direction',
  'Ergebnis': 'Result',
  'Regeln eingehalten': 'Rules followed',
  'Regel gebrochen': 'Rule broken',
  'Fehler': 'Mistakes',
  'Notiz': 'Note',
  'Trade löschen': 'Delete trade',
  'Nochmal tippen zum Löschen': 'Tap again to delete',
  'Hinzufügen': 'Add',

  /* Import */
  '⇩ Trades importieren': '⇩ Import trades',
  'Von der Börse übernehmen': 'Import from your exchange',
  'Exportiere deinen Verlauf als CSV und wähle die Datei hier aus. Funktioniert mit Bitget, Binance und den meisten anderen Börsen.': 'Export your history as CSV and select the file here. Works with Bitget, Binance and most other exchanges.',
  '＋ CSV-Datei auswählen': '＋ Choose CSV file',
  'Wo finde ich den Export?': 'Where do I find the export?',
  '(am Computer): Aufträge → Futures → Positions- oder Auftragsverlauf → Herunterladen. Alternativ im Profil unter „Datenexport“.': '(on desktop): Orders → Futures → Position or order history → Download. Alternatively in your profile under “Data export”.',
  ': Aufträge → USDⓈ-M-Futures → Handelsverlauf → Exportieren.': ': Orders → USDⓈ-M Futures → Trade history → Export.',
  'Die Menünamen können sich je nach Version leicht unterscheiden.': 'Menu names may differ slightly depending on the version.',
  'Trades gefunden · Teil-Ausführungen zusammengefasst': 'trades found · partial fills combined',
  '# Trades': '# trades',
  '# – # · zusammen': '# – # · total',
  'Zeitpunkt *': 'Time *',
  'Coin / Kontrakt': 'Coin / contract',
  'Gewinn/Verlust (USDT) *': 'Profit/loss (USDT) *',
  'Gebühr (optional)': 'Fee (optional)',
  'Gebühren abziehen': 'Deduct fees',
  '# R entspricht (USDT)': '# R equals (USDT)',
  'Dein Risiko pro Trade in USDT. Daraus wird jeder Gewinn oder Verlust in R umgerechnet – so sind alte und neue Trades vergleichbar.': 'Your risk per trade in USDT. Every profit or loss is converted into R from this – so old and new trades are comparable.',
  'Importierte Trades zählen in deine Statistik. Für Regeltreue und Serie zählen sie nicht – dafür fehlen die Angaben aus der Checkliste. Du kannst sie einzeln nachpflegen.': 'Imported trades count in your stats. They don\'t count toward rule adherence and streak – the checklist details are missing. You can fill them in one by one.',
  'Ordne mindestens Zeitpunkt und Gewinn/Verlust einer Spalte zu.': 'Map at least time and profit/loss to a column.',
  '# Trades importieren': 'Import # trades',
  'Keine neuen Trades gefunden – alles schon im Journal.': 'No new trades found – everything\'s already in the journal.',

  /* Profil */
  '● Profil': '● Profile',
  'Wie stehst du da?': 'Where do you stand?',
  'Ergebnis': 'Result',
  'vom Startkonto': 'of starting account',
  'Max. Drawdown': 'Max drawdown',
  'Größter Rückgang vom Hoch:': 'Largest drop from the peak:',
  '(≈ # USDT) · du stehst am Hoch. Längste Serie: # Gewinne, # Verluste in Folge.': '(≈ # USDT) · you\'re at your peak. Longest streak: # wins, # losses in a row.',
  '(≈ # USDT) · aktuell # R unter dem Hoch. Längste Serie: # Gewinne, # Verluste in Folge.': '(≈ # USDT) · currently # R below the peak. Longest streak: # wins, # losses in a row.',
  'Stärken & Schwächen': 'Strengths & weaknesses',
  'Ab ein paar Trades pro Strategie, Coin und Tageszeit siehst du hier, wo deine Edge liegt.': 'Once you have a few trades per strategy, coin and time of day, you\'ll see here where your edge is.',
  'Tageszeit': 'Time of day',
  'Nacht': 'Night',
  'Asien': 'Asia',
  'Kalender': 'Calendar',
  'Vorheriger Monat': 'Previous month',
  'Nächster Monat': 'Next month',
  'Monat:': 'Month:',
  'Keine Trades in diesem Monat.': 'No trades this month.',
  'Woche auswerten und ein Ziel festlegen': 'Review the week and set a goal',
  'Zum Journal': 'To journal',
  'Disziplin': 'Discipline',
  'Regeltreue # Tage': 'Rule adherence # days',
  'Noch zu wenige Trades für ein Urteil': 'Not enough trades for a verdict yet',
  'Noch nicht profitabel': 'Not profitable yet',
  'Du tradest nachweislich profitabel': 'You\'re provably profitable',
  'Profitabel und regelkonform – genau so soll es sein.': 'Profitable and by the rules – exactly how it should be.',
  'Plus trotz Regelbrüchen. Vorsicht: Glück wiederholt sich nicht zuverlässig.': 'In profit despite broken rules. Careful: luck doesn\'t repeat reliably.',
  'Minus trotz sauberer Umsetzung – das ist normale Schwankung, dein Prozess stimmt.': 'In the red despite clean execution – that\'s normal variance, your process is right.',
  'Minus mit Regelbrüchen – hier liegt dein größter Hebel.': 'In the red with broken rules – this is your biggest lever.',
  'Diese Woche keine Trades – auch das kann die richtige Entscheidung sein.': 'No trades this week – that can be the right decision too.',
  'Noch zu wenige Wochen für einen Vergleich mit deinem Durchschnitt. Eine Woche allein sagt wenig – entscheidend ist die Spanne über viele Trades.': 'Not enough weeks yet to compare with your average. One week alone says little – what matters is the range over many trades.',
  'Spanne # bis # R pro Trade.': 'Range # to # R per trade.',
  'Spanne # bis # R pro Trade · ca. sehr viele weitere Trades bis zur Gewissheit.': 'Range # to # R per trade · a lot more trades needed for certainty.',
  '≈ # R': '≈ # R',
  '≈ # USDT · # %': '≈ # USDT · # %',
  '≈ # USDT · # Trades': '≈ # USDT · # trades',
  '# R · # Trades': '# R · # trades',

  /* Serie */
  '◆ Disziplin-Serie': '◆ Discipline streak',
  '# Tage in Folge': '# days in a row',
  'Rekord #': 'Record #',
  '· Rekord #': '· Record #',
  'Ein Tag zählt, wenn du vor dem Traden den Tages-Check gemacht und dich bei jedem Trade an deine Regeln gehalten hast. Gewinne spielen keine Rolle – nur dein Verhalten.': 'A day counts if you did the daily check before trading and followed your rules on every trade. Profits don\'t matter – only your behavior.',
  'Tage ohne Trade zählen nicht gegen dich. Nicht zu traden ist nie ein Fehler.': 'Days without trades don\'t count against you. Not trading is never a mistake.',
  'Pro Woche fängt': 'Each week,',
  'ein Joker': 'one joker',
  'einen Ausrutscher ab – diese Woche ist er noch frei. Rekord:': 'covers one slip – this week it\'s still available. Record:',
  'einen Ausrutscher ab – diese Woche ist er schon verbraucht. Rekord:': 'covers one slip – this week it\'s already used. Record:',
  'Disziplin-Serie:': 'Discipline streak:',

  /* Wochenrückblick */
  '◷ Wochenrückblick · # Minuten': '◷ Weekly review · # minutes',
  'Deine Woche': 'Your week',
  'Dein teuerster Fehler (# Tage)': 'Your costliest mistake (# days)',
  'Markier nach jedem Trade, ob ein Fehler dabei war – dann zeigt dir der Rückblick, welcher dich am meisten kostet.': 'Mark after each trade whether there was a mistake – then the review shows you which one costs you the most.',
  'Ein Ziel für nächste Woche': 'One goal for next week',
  'Nur eins. Als Wenn-dann-Plan – so wird es im entscheidenden Moment automatisch abgerufen.': 'Just one. As an if-then plan – that way it kicks in automatically at the decisive moment.',
  'Wenn …': 'If …',
  '… dann …': '… then …',
  'Ziel festlegen': 'Set goal',
  'Teilweise': 'Partly',
  'Regel brechen': 'Break a rule',

  /* Einstellungen */
  '⚙︎ Einstellungen': '⚙︎ Settings',
  'Deine Regeln': 'Your rules',
  'Einmal in Ruhe festlegen, nicht mitten im Trade.': 'Set them once, calmly – not in the middle of a trade.',
  'Deine Strategien': 'Your strategies',
  '# Fragen ✎': '# questions ✎',
  '+ Strategie hinzufügen': '+ Add strategy',
  'Deine Grenzen': 'Your limits',
  'Max. Trades pro Tag': 'Max trades per day',
  'Max. Verlust pro Tag (R)': 'Max loss per day (R)',
  'Min. Chance-Risiko (CRV)': 'Min risk-reward (R:R)',
  'Risiko pro Trade (%)': 'Risk per trade (%)',
  'Standard-Hebel': 'Default leverage',
  'Kontostand (USDT)': 'Account balance (USDT)',
  'Pause nach Verlust (Min.)': 'Break after loss (min)',
  'Pause nach Gewinn (Min.)': 'Break after win (min)',
  'Abstand zu News (Min.)': 'Distance to news (min)',
  'Sperre nach US-Börsenstart (Min.)': 'Lock after US market open (min)',
  'Sprache / Language': 'Language / Sprache',
  'Design': 'Design',
  'Cyan (Standard)': 'Cyan (default)',
  'Gold': 'Gold',
  'Töne (Atmen, Pause vorbei, Stopp, Plan eingehalten)': 'Sounds (breathing, break over, stop, plan followed)',
  '▶ Ton testen': '▶ Test sound',
  'Am iPhone: Lautlos-Schalter aus und Lautstärke hoch.': 'On iPhone: silent switch off and volume up.',
  'Journal & Daten löschen': 'Delete journal & data',
  'Wirklich? Alles wird gelöscht – nochmal tippen': 'Really? Everything will be deleted – tap again',
  'Backup': 'Backup',
  'Backup speichern': 'Save backup',
  'Backup laden': 'Load backup',
  'Das ist keine gültige NoTilt-Backup-Datei.': 'That\'s not a valid NoTilt backup file.',
  'Backup geladen ✓': 'Backup loaded ✓',
  'NoTilt ist ein Werkzeug für Disziplin und Selbstbeobachtung – keine Anlageberatung. Gehebelter Handel mit Kryptowährungen kann zum Totalverlust des eingesetzten Kapitals führen.': 'NoTilt is a tool for discipline and self-observation – not investment advice. Leveraged crypto trading can lead to the total loss of your capital.',
  '● Bevor es losgeht': '● Before you start',
  'Wichtiger Hinweis': 'Important notice',
  'Strategie-Vorlagen, Beispiele und Auswertungen in der App dienen nur zum Lernen und zur Selbstkontrolle. Vergangene Ergebnisse – auch deine eigenen – garantieren nichts für die Zukunft. Bitte bestätige:': 'Strategy templates, examples and stats in this app are for learning and self-monitoring only. Past results – including your own – guarantee nothing about the future. Please confirm:',
  'Ich verstehe: NoTilt ist ein Werkzeug für Disziplin und Selbstbeobachtung. Die App gibt keine Anlage- oder Finanzberatung und keine Kauf- oder Verkaufsempfehlungen.': 'I understand: NoTilt is a tool for discipline and self-observation. It does not provide investment or financial advice and gives no buy or sell recommendations.',
  'Mir ist klar, dass gehebelter Handel mit Kryptowährungen zum Totalverlust des eingesetzten Geldes führen kann. Jede Entscheidung treffe ich selbst und auf eigenes Risiko.': 'I know that leveraged crypto trading can lead to the total loss of the money I put in. I make every decision myself and at my own risk.',
  'Ich bin mindestens # Jahre alt.': 'I am at least # years old.',
  'Verstanden – weiter': 'Got it – continue',
  'Noch ein Trade?': 'Another trade?',
  'offen · seit # Min.': 'open · for # min',
  'offen · seit # Std.': 'open · for # h',
  'Öffnen →': 'Open →',
  '⚠ Schon ein Trade offen': '⚠ One trade already open',
  '⚠ Schon # Trades offen': '⚠ # trades already open',
  'Noch einen dazu?': 'Add another one?',
  'Gerade riskierst du # %. Mit einem weiteren Trade sind es ca. # % gleichzeitig. Laufen die Trades in dieselbe Richtung, verlieren sie oft auch zusammen.': 'Right now you are risking # %. With another trade it is about # % at the same time. Trades in the same direction often lose together, too.',
  'Lieber nicht': 'Better not',
  'Zur Checkliste': 'To the checklist',
  '+ Weiteren Trade über die Checkliste': '+ Another trade via the checklist',
  'Profit – Take Profit erreicht': 'Profit – take profit hit',
  'Loss – Stop Loss getroffen. Ein Datenpunkt.': 'Loss – stop loss hit. Just one data point.',
  '● Beta': '● Beta',
  'Du nutzt eine frühe Version von NoTilt': "You're using an early version of NoTilt",
  'Die App wird gerade mit den ersten Tradern getestet. Manches kann sich noch ändern, und hier und da kann etwas haken. Deine Daten bleiben dabei erhalten – am sichersten mit einem Konto.': 'The app is being tested with its first traders. Some things may still change, and something may glitch here and there. Your data stays safe – safest with an account.',
  'Danke, dass du dabei bist!': 'Thanks for being part of it!',
  'Alles klar': 'Got it',
  'Aktiv ✓ Eine Testnachricht ist unterwegs – sie kommt in ein paar Sekunden.': 'Active ✓ A test notification is on its way – it arrives in a few seconds.',
  '● Sprache': '● Language',
  'Sprache wählen': 'Choose language',
  'Impressum': 'Legal notice',
  'Datenschutz': 'Privacy',

  /* Erinnerung */
  'Erinnerung': 'Reminder',
  'Feste Zeit, fester Auslöser:': 'Fixed time, fixed trigger:',
  'es # Uhr ist und ich an die Charts gehe,': 'it\'s # and I open the charts,',
  'mache ich zuerst den Tages-Check.': 'I do the daily check first.',
  'Uhrzeit': 'Time',
  'Tage': 'Days',
  'Mo': 'Mon', 'Di': 'Tue', 'Mi': 'Wed', 'Do': 'Thu', 'Fr': 'Fri', 'Sa': 'Sat', 'So': 'Sun',
  'So # Uhr': 'Sun #:00',
  'Push-Erinnerung aktivieren': 'Turn on push reminder',
  'Erinnerung aktualisieren': 'Update reminder',
  'Erinnerung ausschalten': 'Turn off reminder',
  'Test senden': 'Send test',
  'Aktiv ✓ Du bekommst die Erinnerung zur gewählten Zeit.': 'Active ✓ You\'ll get the reminder at the chosen time.',
  'Gesendet – kommt gleich an.': 'Sent – arriving shortly.',
  'Kein aktives Gerät gefunden. Aktiviere die Erinnerung auf diesem Gerät erneut.': 'No active device found. Turn the reminder on again on this device.',
  'Benachrichtigungen sind blockiert. Erlaube sie in den Einstellungen deines Geräts für NoTilt.': 'Notifications are blocked. Allow them for NoTilt in your device settings.',
  'Erinnerungen brauchen ein Konto.': 'Reminders need an account.',
  'Melde dich an, um Erinnerungen zu nutzen.': 'Sign in to use reminders.',
  'Dieser Browser unterstützt keine Push-Nachrichten.': 'This browser doesn\'t support push notifications.',
  'Am iPhone: erst über Teilen → „Zum Home-Bildschirm“ installieren, dann hier aktivieren.': 'On iPhone: first install via Share → “Add to Home Screen”, then turn it on here.',

  /* Strategien */
  'Welche Strategien tradest du?': 'Which strategies do you trade?',
  'Tipp alle an, die du nutzt. Jede bekommt ihre eigene Checkliste – die kannst du gleich anpassen.': 'Tap all the ones you use. Each gets its own checklist – you can adjust it right away.',
  'Eigene Strategie': 'Custom strategy',
  'Von null selbst beschreiben': 'Describe it from scratch',
  'Deine eigene Strategie': 'Your own strategy',
  'Name': 'Name',
  'z. B. Orderblock-Retest': 'e.g. Order block retest',
  'Was muss erfüllt sein, bevor du einsteigst?': 'What has to be true before you enter?',
  'Das werden deine Fragen vor jedem Trade. Kurz und mit Ja/Nein zu beantworten – am besten # bis #': 'These become your questions before every trade. Short and answerable with yes/no – ideally # to #',
  'Das werden deine Fragen vor jedem Trade. Kurz und mit Ja/Nein zu beantworten – am besten # bis #.': 'These become your questions before every trade. Short and answerable with yes/no – ideally # to #.',
  'z. B. Kurs ist an meiner Zone': 'e.g. Price is at my zone',
  'Bedingung entfernen': 'Remove condition',
  '+ Bedingung hinzufügen': '+ Add condition',
  'Wohin kommt dein Stop?': 'Where does your stop go?',
  'z. B. hinter das letzte Tief': 'e.g. behind the last low',
  'Wo ist dein Ziel?': 'Where is your target?',
  'z. B. nächstes Hoch': 'e.g. next high',
  'Gib der Strategie einen Namen.': 'Give the strategy a name.',
  'Trag mindestens # Bedingungen ein – sonst ist es keine Checkliste.': 'Enter at least # conditions – otherwise it\'s not a checklist.',
  'Diesen Namen gibt es schon.': 'That name already exists.',
  'Nächste Strategie': 'Next strategy',
  'Deine Grundregeln': 'Your basic rules',
  'Die App hält dich an diese Grenzen – vor allem dann, wenn du es selbst gerade nicht willst.': 'The app holds you to these limits – especially when you don\'t want it to.',
  'Faustregel: #–# % Risiko pro Trade. So überstehst du auch eine Pechsträhne von # Verlusten.': 'Rule of thumb: #–# % risk per trade. That way you survive a losing streak of # trades too.',
  'Mindest-CRV': 'Minimum R:R',
  'Los geht\'s': 'Let\'s go',
  '● Einrichtung #/#': '● Setup #/#',
  '● Einrichtung #/# · Strategie #/#': '● Setup #/# · Strategy #/#',
  '● Strategie': '● Strategy',
  '● Neue Strategie': '● New strategy',
  'Strategie bearbeiten': 'Edit strategy',
  'Strategie löschen': 'Delete strategy',
  'Nochmal tippen – Trades bleiben im Journal': 'Tap again – trades stay in the journal',
  'Womit möchtest du starten?': 'What would you like to start with?',
  'Trendfolge': 'Trend Following',
  'Fair Value Gap': 'Fair Value Gap',
  'Support & Resistance': 'Support & Resistance',
  'Trendfolge / Breakout': 'Trend Following / Breakout',
  'Pullback': 'Pullback',
  'Range': 'Range',
  'Liquidity Sweep': 'Liquidity Sweep',
  'Rücksetzer in eine Preislücke': 'Pullback into a price gap',
  'Reaktion an einer wichtigen Zone': 'Reaction at a key zone',
  'Mitfahren, wenn ein Level bricht': 'Ride along when a level breaks',
  'Einstieg im Rücksetzer eines Trends': 'Entry on a pullback within a trend',
  'Seitwärtsmarkt von Rand zu Rand': 'Sideways market, edge to edge',
  'Fehlausbruch über ein offensichtliches Level': 'False break of an obvious level',
  /* deutsche Vorlagen-Texte (für alte Konten mit deutschen Strategien) */
  'Die Lücke liegt in Richtung des Trends auf dem höheren Zeitrahmen': 'The gap is in the direction of the higher-timeframe trend',
  'Der Kurs ist jetzt zurück in der Lücke': 'Price is back inside the gap now',
  'Reaktion mit Bestätigungskerze – nach Kerzenschluss': 'Reaction with a confirmation candle – after the candle closes',
  'Bis zum Ziel liegt kein starkes Hindernis': 'No strong obstacle between entry and target',
  'Hinter der Kerze, die die Lücke erzeugt hat': 'Behind the candle that created the gap',
  'Nächstes Hoch bzw. Tief mit Liquidität': 'Next high or low with liquidity',
  'Das Setup entspricht genau meiner getesteten Strategie': 'The setup matches my tested strategy exactly',
  'Der Kurs ist jetzt in meiner Zone – nicht schon weit weg': 'Price is in my zone now – not already far away',
  'Bestätigungssignal ist da – nach Kerzenschluss, nicht davor': 'Confirmation signal is there – after the candle closes, not before',
  'Der höhere Zeitrahmen spricht nicht gegen den Trade': 'The higher timeframe does not argue against the trade',

  /* Konto */
  '● Konto': '● Account',
  'Willkommen bei NoTilt': 'Welcome to NoTilt',
  'Melde dich an, damit dein Journal auf allen Geräten gleich ist.': 'Sign in so your journal is the same on all your devices.',
  'Konto erstellen': 'Create account',
  'Mit Konto sind Journal und Einstellungen auf Mac und iPhone synchron.': 'With an account, your journal and settings stay in sync across all your devices.',
  'Passwort vergessen': 'Forgot password',
  'Wir schicken dir einen Link, mit dem du ein neues Passwort setzt.': 'We\'ll send you a link to set a new password.',
  'Neues Passwort': 'New password',
  'Wähle ein neues Passwort mit mindestens # Zeichen.': 'Choose a new password with at least # characters.',
  'E-Mail': 'Email',
  'Passwort': 'Password',
  'Anmelden': 'Sign in',
  'Link senden': 'Send link',
  'Passwort speichern': 'Save password',
  'Neu hier? Konto erstellen': 'New here? Create an account',
  'Passwort vergessen?': 'Forgot your password?',
  'Ich habe schon ein Konto': 'I already have an account',
  'Ohne Konto nutzen (nur dieses Gerät)': 'Use without an account (this device only)',
  'E-Mail oder Passwort stimmt nicht.': 'Email or password is incorrect.',
  'Bitte bestätige zuerst deine E-Mail (Link im Postfach).': 'Please confirm your email first (link in your inbox).',
  'Diese E-Mail hat schon ein Konto. Melde dich an.': 'This email already has an account. Sign in.',
  'Das Passwort braucht mindestens # Zeichen.': 'The password needs at least # characters.',
  'Zu viele Versuche. Warte kurz und probier es dann nochmal.': 'Too many attempts. Wait a moment and try again.',
  'Keine Verbindung. Prüf dein Internet.': 'No connection. Check your internet.',
  'Fast geschafft: Wir haben dir eine E-Mail geschickt. Bestätige den Link und melde dich dann hier an.': 'Almost done: we\'ve sent you an email. Confirm the link, then sign in here.',
  'Wenn es ein Konto mit dieser E-Mail gibt, ist der Link jetzt unterwegs.': 'If there\'s an account with this email, the link is on its way.',
  'Konto': 'Account',
  'Angemeldet als': 'Signed in as',
  'Abmelden': 'Sign out',
  'Konto löschen': 'Delete account',
  'Nicht angemeldet – deine Daten liegen nur auf diesem Gerät.': 'Not signed in – your data is only on this device.',
  'Anmelden oder Konto erstellen': 'Sign in or create account',
  'Synchron mit deinen anderen Geräten': 'In sync with your other devices',
  'Erst online gehen – Änderungen nicht hochgeladen': 'Go online first – changes not uploaded yet',
  "Mit # % Wahrscheinlichkeit verdienst du zwischen # und # R pro Trade.": "With # % probability you earn between # and # R per trade.",
  "Dein Ø liegt mit # % Wahrscheinlichkeit zwischen # und # R pro Trade.": "With # % probability your average lies between # and # R per trade.",
  "Aktuell nicht profitabel": "Currently not profitable",
  "Im Plus – aber noch nicht sicher": "In profit – but not certain yet",
  "Deutlich besser als deine übliche Woche (Ø # R).": "Clearly better than your usual week (avg # R).",
  "Schwächer als deine übliche Woche (Ø # R) – prüf, ob es am Markt oder an dir lag.": "Weaker than your usual week (avg # R) – check whether it was the market or you.",
  "Deine Charts der Woche": "Your charts of the week",
  "Fertig: # Trades übernommen, Einstellungen geladen.": "Done: # trades imported, settings loaded.",
  "Löschen fehlgeschlagen – später nochmal": "Delete failed – try again later",
  "Die Datei enthält keine lesbaren Zeilen.": "The file contains no readable rows.",
  "Gerade keine Verbindung – Änderungen werden später hochgeladen": "No connection right now – changes will be uploaded later",
  "– # nötig, bis das Ergebnis kein Zufall mehr sein kann.": "– # needed before the result can't be luck anymore.",
  "Das hat nicht geklappt:": "That didn't work:",
  "Die Zone hat in der Vergangenheit klar reagiert": "The zone has clearly reacted in the past",
  "Der Kurs ist jetzt in der Zone – nicht schon weit weg": "Price is in the zone now – not already far away",
  "Bestätigungskerze: Hammer, Engulfing oder Schluss über/unter der Zone": "Confirmation candle: hammer, engulfing or close above/below the zone",
  "Der höhere Zeitrahmen spricht nicht dagegen": "The higher timeframe does not argue against it",
  "Hinter der Zone plus etwas Puffer": "Behind the zone plus a little buffer",
  "Nächste gegenüberliegende Zone": "Next opposite zone",
  "Klarer Trend: höhere Hochs und Tiefs (bzw. tiefere)": "Clear trend: higher highs and lows (or lower)",
  "Ausbruch mit Kerzenschluss über bzw. unter dem Level": "Breakout with a candle close above or below the level",
  "Direkt vor dem Kurs liegt kein großer Widerstand": "No major resistance right in front of price",
  "Momentum oder Volumen bestätigt den Ausbruch": "Momentum or volume confirms the breakout",
  "Hinter dem letzten Swing": "Behind the last swing",
  "Nachziehender Stop oder festes CRV": "Trailing stop or fixed R:R",
  "Der Trend auf dem höheren Zeitrahmen ist intakt": "The higher-timeframe trend is intact",
  "Der Kurs setzt an eine Zone oder gleitende Linie zurück": "Price pulls back to a zone or moving average",
  "Umkehrsignal nach Kerzenschluss": "Reversal signal after the candle closes",
  "Die Struktur ist nicht gebrochen": "Structure is not broken",
  "Unter bzw. über dem Tief des Rücksetzers": "Below or above the pullback low/high",
  "Letztes Hoch bzw. Tief des Trends": "Last high or low of the trend",
  "Die Range ist klar: oben und unten mindestens zweimal berührt": "The range is clear: top and bottom touched at least twice",
  "Der Kurs ist am Rand der Range – nicht in der Mitte": "Price is at the edge of the range – not in the middle",
  "Ablehnung am Rand nach Kerzenschluss": "Rejection at the edge after the candle closes",
  "Kein Ausbruch auf dem höheren Zeitrahmen": "No breakout on the higher timeframe",
  "Knapp außerhalb der Range": "Just outside the range",
  "Mitte oder andere Seite der Range": "Middle or other side of the range",
  "Offensichtliches Hoch bzw. Tief mit Stops dahinter": "Obvious high or low with stops behind it",
  "Docht holt die Liquidität und schließt wieder zurück": "A wick takes the liquidity and closes back inside",
  "Strukturbruch auf dem kleineren Zeitrahmen": "Structure break on the lower timeframe",
  "Einstieg im Rücksetzer, nicht im Impuls": "Entry on the pullback, not in the impulse",
  "Hinter dem Docht des Sweeps": "Behind the wick of the sweep",
  "Gegenüberliegende Liquidität": "Opposite liquidity",
  'Eine Woche allein sagt wenig – entscheidend ist die Spanne über viele Trades.': 'One week alone says little – what matters is the range over many trades.',
  'Im normalen Schwankungsbereich deiner Wochen (Ø # R).': 'Within the normal range of your weeks (avg # R).',
  'Noch zu wenige Wochen für einen Vergleich mit deinem Durchschnitt.': 'Not enough weeks yet to compare with your average.',
  '· danach': '· then',
  'Ausstiegskurs': 'Exit price',
  'Break-even': 'Break-even',
  'Anderer Ausstiegskurs? Anpassen': 'Different exit price? Adjust',
  'Hast du dich an deinen Plan gehalten?': 'Did you stick to your plan?',
  '✓ Ja, alles nach Plan': '✓ Yes, all by plan',
  '✗ Nein': '✗ No',
  'Welche Regel hast du gebrochen? Antippen.': 'Which rule did you break? Tap it.',
  'Was war der Fehler? (optional, ehrlich hilft dir am meisten)': 'What was the mistake? (optional – honesty helps you most)',
  'Notiz, Screenshot, Chart-Link (optional)': 'Note, screenshot, chart link (optional)',
  'Ausstiegskurs eingeben – die App rechnet das Ergebnis aus.': 'Enter the exit price – the app calculates the result.',
  'Offener Trade': 'Open trade',
  'Wie ist er ausgegangen?': 'How did it end?',
  '· seit # Min.': '· for # min',
  '· seit # Std.': '· for # h',
  "Einstellungen": "Settings",
  "? Kurz erklärt": "? Quick explainer",
  "R – dein Risiko als Maßeinheit": "R – your risk as a unit",
  "R ist das, was du pro Trade riskierst. Riskierst du # USDT, ist # R = # USDT. # R heißt: doppelt so viel gewonnen, wie du riskiert hast. So kannst du Trades vergleichen, egal wie groß dein Konto ist.": "R is what you risk per trade. If you risk # USDT, # R = # USDT. # R means you won twice what you risked. That way you can compare trades no matter how big your account is.",
  "CRV – Chance zu Risiko": "R:R – reward to risk",
  "Wie viel kannst du gewinnen im Vergleich zu dem, was du riskierst? CRV # heißt: Dein Ziel ist doppelt so weit weg wie dein Stop. Mit CRV # bist du schon bei jedem dritten Gewinner bei plus minus null.": "How much can you win compared to what you risk? R:R # means your target is twice as far away as your stop. With R:R # you already break even if every third trade is a winner.",
  "Bias – deine Richtung für heute": "Bias – your direction for today",
  "Long (steigend), Short (fallend) oder Neutral. Du suchst dann nur Trades in diese Richtung. Neutral heißt: heute kein Trade – und das ist völlig in Ordnung.": "Long (rising), short (falling) or neutral. You then only look for trades in that direction. Neutral means: no trade today – and that's completely fine.",
  "Wie oft du dich an deinen eigenen Plan gehalten hast – egal, ob der Trade Gewinn gebracht hat. Das ist die wichtigste Zahl, die du komplett selbst in der Hand hast.": "How often you stuck to your own plan – no matter whether the trade made money. It's the most important number that is completely in your control.",
  "Trefferquote": "Win rate",
  "Anteil deiner Trades mit Gewinn. Allein sagt sie wenig: Auch # % können profitabel sein, wenn deine Gewinner größer sind als deine Verlierer.": "Share of your trades that made money. On its own it says little: even # % can be profitable if your winners are bigger than your losers.",
  "Alle Gewinne geteilt durch alle Verluste. Über # heißt: Du verdienst Geld. Über # ist gut.": "All wins divided by all losses. Above # means you make money. Above # is good.",
  "Drawdown": "Drawdown",
  "Der größte Rückgang vom bisherigen Höchststand deines Kontos. Er zeigt, wie hart deine schlechteste Phase war.": "The largest drop from your account's previous peak. It shows how rough your worst stretch was.",
  "Ø R pro Trade": "Avg R per trade",
  "Wie viel R du im Schnitt pro Trade gewinnst oder verlierst. Über # heißt: Dein System verdient auf Dauer Geld.": "How much R you win or lose per trade on average. Above # means your system makes money over time.",
  "Edge – echter Vorteil oder Glück?": "Edge – real advantage or luck?",
  "Eine Edge ist ein echter Vorteil deines Systems. Ob du eine hast, zeigt sich erst nach vielen Trades – meist # bis # Vorher kann ein gutes Ergebnis auch Glück sein.": "An edge is a real advantage of your system. Whether you have one only shows after many trades – usually # to # Before that, a good result can also be luck.",
  "Der Preis, bei dem die Börse deine Position zwangsweise schließt, weil dein eingesetztes Geld aufgebraucht ist. Er muss immer hinter deinem Stop liegen.": "The price at which the exchange force-closes your position because your margin is used up. It must always be beyond your stop.",
  "Die Summe all deiner Ergebnisse in R – also unterm Strich.": "The sum of all your results in R – the bottom line.",
  "Erste Schritte": "First steps",
  "# von # erledigt": "# of # done",
  "Strategie eingerichtet": "Strategy set up",
  "Ersten Tages-Check machen": "Do your first daily check",
  "Ersten Trade über die Checkliste starten": "Start your first trade through the checklist",
  "Trade abschließen und Ergebnis eintragen": "Close the trade and log the result",
  "App auf den Home-Bildschirm holen": "Add the app to your home screen",
  "Ersten Wochenrückblick machen": "Do your first weekly review",
  "Jetzt →": "Now →",
  "Ausblenden": "Hide",
  "● App installieren": "● Install the app",
  "Hol dir NoTilt auf den Home-Bildschirm": "Get NoTilt on your home screen",
  "Dann öffnest du die App mit einem Tipp – wie jede andere App. Und du kannst Erinnerungen bekommen.": "Then you open it with one tap – like any other app. And you can get reminders.",
  "In Safari unten auf das Teilen-Symbol tippen": "In Safari, tap the Share icon at the bottom",
  "„Zum Home-Bildschirm“ wählen": "Choose “Add to Home Screen”",
  "Oben rechts „Hinzufügen“ tippen": "Tap “Add” in the top right",
  "In Chrome oben rechts auf ⋮ tippen": "In Chrome, tap ⋮ in the top right",
  "„App installieren“ oder „Zum Startbildschirm hinzufügen“ wählen": "Choose “Install app” or “Add to Home screen”",
  "In Chrome: rechts in der Adressleiste auf das Installieren-Symbol klicken": "In Chrome: click the install icon on the right of the address bar",
  "In Safari am Mac: Ablage → „Zum Dock hinzufügen“": "In Safari on Mac: File → “Add to Dock”",
  "Hab ich gemacht": "Done",
  "● Willkommen bei NoTilt": "● Welcome to NoTilt",
  "Wie lange tradest du schon?": "How long have you been trading?",
  "Damit die App nur zeigt, was du gerade brauchst. Du kannst es jederzeit in den Einstellungen ändern.": "So the app only shows what you need right now. You can change it later in settings.",
  "Ich fange gerade erst an": "I'm just getting started",
  "Seit ein paar Monaten": "For a few months",
  "Länger als ein Jahr": "More than a year",
  "Mehr Kennzahlen und Auswertungen": "More stats and analysis",
  "Weitere Grenzen (Chance-Risiko, Hebel, Pausen, News)": "More limits (R:R, leverage, breaks, news)",
  "Ansicht": "View",
  "Einfach": "Simple",
  "Alle Details": "All details",
  "Einfach: nur das Wichtigste, Details auf Wunsch. Alle Details: jede Kennzahl sofort sichtbar.": "Simple: only the essentials, details on request. All details: every stat visible right away.",
  "Stärken & Schwächen, Treffer, Drawdown": "Strengths & weaknesses, win rate, drawdown",
  'Später einrichten': 'Set up later',
  'Design & Töne': 'Design & sounds',
  'Daten: Backup & Löschen': 'Data: backup & delete',
  "#-Minuten-Gap, Einstieg in der Mitte, festes CRV": "#m gap, entry in the middle, fixed R:R",
  "Über bzw. unter dem letzten markanten Docht (hinter dem Gap)": "Above or below the last significant wick (beyond the gap)",
  "Festes CRV #–# (z. B. XRP: #)": "Fixed R:R #–# (e.g. XRP: #)",
  "Aktuelle Vorlage „Fair Value Gap“ übernehmen": "Use current “Fair Value Gap” template",
  "#-Minuten-Chart: Der Indikator zeigt ein neues Fair Value Gap": "#-minute chart: the indicator shows a new fair value gap",
  "Das Gap ist mindestens # % groß": "The gap is at least # % in size",
  "Der Move nach dem Gap hat mindestens CRV # (Ziel am letzten Hoch bzw. Tief vor dem Rücklauf)": "The move after the gap has at least R:R # (target at the last high or low before the pullback)",
  "Nicht mehr als # % mitigiert und außerhalb geschlossen": "Not more than # % mitigated and closed outside",
  "Nicht schon im Gap und danach wieder außerhalb geschlossen": "Not already closed inside the gap and then back outside",
  "Kein weiteres valides Gap direkt darunter bzw. darüber": "No other valid gap directly below or above",
  "Das Gap wurde noch nicht vollständig mitigiert": "The gap has not been fully mitigated yet",
  "Einstieg in der Mitte des Gaps (Limit- oder Trigger-Order)": "Entry in the middle of the gap (limit or trigger order)",
  "Keine News und kein Börsenstart gerade – sonst besonders vorsichtig": "No news and no market open right now – otherwise be extra careful",
  'Aktuelle Vorlage': 'Current template'
});

/* Zusammengesetzte Texte mit eigenen Inhalten (Coin, Strategie, Wochenziel) */
I18N_PARTS.push(
  [/^(\s*)Im Trade · /, '$1In trade · '],
  [/ · Einstieg /g, ' · Entry '], [/ · Ziel /g, ' · Target '],
  [/^(\s*)Nach Setup-Note:/, '$1By setup grade:'], [/^(\s*)Nach Strategie:/, '$1By strategy:'],
  [/^(\s*)Letzte Woche wolltest du: „Wenn (.*), dann (.*)\.“ Hat das geklappt\?/, '$1Last week you wanted: “If $2, then $3.” Did it work?'],
  [/^(\s*)Das hat nicht geklappt: /, '$1That didn\'t work: '],
  [/^(\s*)CRV ([\d.,]+) – dein Minimum ist ([\d.,]+)\./, '$1R:R $2 – your minimum is $3.'],
  [/ – kostet dich /g, ' – costs you '], [/ · seit (\d+) Min\./, ' · for $1 min'], [/ · seit (\d+) Std\./, ' · for $1 h'], [/ · zusammen/, ' · total']
);
/* ==================================================================
   Gewohnheit & Lernen
   – Disziplin-Serie (Prozess statt Ergebnis), Fehler und ihre Kosten,
     Wochenrückblick mit einem Wenn-dann-Ziel, ehrliche Edge-Spanne,
     Push-Erinnerungen zur festen Zeit
   ================================================================== */
const MISTAKES = [['fomo','FOMO-Einstieg'],['frueh','Zu früh raus'],['stop','Stop verschoben'],['gross','Position zu groß'],
  ['nach','Nachgelegt'],['kein','Kein echtes Setup'],['revenge','Revenge-Trade']];
const MIST_TXT = Object.fromEntries(MISTAKES);
const IFTHEN = {
  fomo:   ['ein Coin ohne mich losläuft', 'setze ich einen Alarm an meiner Zone statt hinterherzuspringen'],
  frueh:  ['ich früher raus will', 'schließe ich den Chart und lasse Stop und Ziel arbeiten'],
  stop:   ['der Kurs Richtung Stop läuft', 'fasse ich den Stop nicht an – er war Teil des Plans'],
  gross:  ['ich die Position eingebe', 'nutze ich nur die Größe, die die App berechnet hat'],
  nach:   ['ich nachlegen will', 'tippe ich den Impuls an und warte 5 Minuten'],
  kein:   ['mir langweilig ist', 'schließe ich die Charts, statt ein Setup zu suchen'],
  revenge:['ich gerade einen Verlust hatte', 'mache ich die volle Pause, bevor ich wieder auf den Chart schaue'],
  plan:   ['ich eine Regel brechen will', 'tippe ich zuerst den Impuls in der App an']
};
const IFTHEN_EN = {
  fomo:   ['a coin takes off without me', 'I set an alert at my zone instead of chasing it'],
  frueh:  ['I want to get out early', 'I close the chart and let stop and target do their job'],
  stop:   ['price moves toward my stop', "I don't touch the stop – it was part of the plan"],
  gross:  ['I enter the position', 'I only use the size the app calculated'],
  nach:   ['I want to add to the position', 'I tap the impulse and wait 5 minutes'],
  kein:   ["I'm bored", 'I close the charts instead of hunting for a setup'],
  revenge:['I just took a loss', 'I take the full break before I look at the chart again'],
  plan:   ['I want to break a rule', 'I tap the impulse in the app first']
};
const ifthen = k => LANG === 'de' ? IFTHEN[k] : LANG === 'en' ? IFTHEN_EN[k] : IFTHEN[k].map(x => tr(x));
const WEEKDAYS = [[1,'Mo'],[2,'Di'],[3,'Mi'],[4,'Do'],[5,'Fr'],[6,'Sa'],[7,'So']];

/* ---------- Datum-Helfer ---------- */
const dayKey = d => { const x = new Date(d); return x.getFullYear()+'-'+String(x.getMonth()+1).padStart(2,'0')+'-'+String(x.getDate()).padStart(2,'0'); };
function isoWeek(d = new Date()){
  const x = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate())); const day = x.getUTCDay() || 7;
  x.setUTCDate(x.getUTCDate() + 4 - day); const y0 = new Date(Date.UTC(x.getUTCFullYear(), 0, 1));
  return x.getUTCFullYear() + '-W' + String(Math.ceil(((x - y0) / 86400000 + 1) / 7)).padStart(2, '0');
}
const realTrades = () => cache.journal.filter(t => t.art !== 'import');

/* ---------- Tageslog: Tages-Check gemacht? ---------- */
function markDay(){
  const d = Object.assign({}, S.get('days', {})); d[today()] = {c: 1};
  const keys = Object.keys(d).sort(); while (keys.length > 400) delete d[keys.shift()];
  S.set('days', d);
}

/* ---------- Disziplin-Serie ----------
   Ein Tag zählt, wenn du den Tages-Check gemacht hast und jeder Trade des Tages regelkonform war.
   Tage ohne Aktivität sind neutral – nicht zu traden ist nie ein Fehler.
   Ein Joker pro Kalenderwoche fängt einen Ausrutscher ab. */
function streakInfo(){
  const days = S.get('days', {}), byDay = {};
  realTrades().forEach(t => { const k = dayKey(t.date); (byDay[k] = byDay[k] || []).push(t); });
  const active = [...new Set([...Object.keys(days), ...Object.keys(byDay)])].sort();
  const good = k => !!(days[k] && days[k].c) && (byDay[k] || []).every(t => t.plan !== false);
  // aktuelle Serie: rückwärts
  let cur = 0, jokerUsed = {}, usedThisWeek = false;
  for (let i = active.length - 1; i >= 0; i--){
    const k = active[i];
    if (good(k)) { cur++; continue; }
    if (k === today() && !(days[k] && days[k].c) && !(byDay[k] || []).some(t => t.plan === false)) continue;   // heute noch offen
    const wk = isoWeek(new Date(k + 'T12:00:00'));
    if (!jokerUsed[wk]) { jokerUsed[wk] = true; if (wk === isoWeek()) usedThisWeek = true; continue; }
    break;
  }
  // beste Serie: vorwärts mit gleicher Regel
  let best = 0, run = 0, jw = {};
  active.forEach(k => {
    if (good(k)) { run++; best = Math.max(best, run); return; }
    const wk = isoWeek(new Date(k + 'T12:00:00'));
    if (!jw[wk]) { jw[wk] = true; return; }
    run = 0;
  });
  return {cur, best: Math.max(best, cur), jokerLeft: !usedThisWeek, todayDone: good(today())};
}

/* ---------- Fehler und was sie kosten ----------
   Kosten = Anzahl × (Ø R ohne Fehler − Ø R mit Fehler). Eine Schätzung, kein Beweis – sie wird mit mehr Trades genauer. */
function mistakeCosts(arr){
  const avg = a => a.reduce((s, x) => s + x.R, 0) / a.length;
  const clean = arr.filter(t => Array.isArray(t.err) && !t.err.length && t.plan !== false);
  if (clean.length < 3) return [];
  const base = avg(clean);
  return MISTAKES.map(([k, l]) => {
    const w = arr.filter(t => Array.isArray(t.err) && t.err.includes(k));
    return w.length ? {k, l, n: w.length, avg: avg(w), cost: w.length * (base - avg(w))} : null;
  }).filter(x => x && x.n >= 2).sort((a, b) => b.cost - a.cost);
}
function mistakeRow(id, sel){
  return `<div class="chips mist" id="${id}">${MISTAKES.map(([k, l]) => `<button type="button" data-v="${k}" class="${(sel || []).includes(k) ? 'on' : ''}">${l}</button>`).join('')}</div>`;
}
const readMistakes = id => $$(`#${id} button.on`).map(b => b.dataset.v);

/* ---------- Edge: Zufall oder echt? ----------
   Bootstrap-Konfidenzintervall (95 %) für den Ø R pro Trade. Robust auch bei schiefen R-Verteilungen. */
function edgeCI(arr){
  const r = arr.map(t => +t.R).filter(isFinite), n = r.length;
  if (n < 5) return null;
  const mean = r.reduce((a, b) => a + b, 0) / n, B = 2000, means = new Float64Array(B);
  let seed = 12345; const rnd = () => (seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648;   // reproduzierbar
  for (let b = 0; b < B; b++){ let s = 0; for (let i = 0; i < n; i++) s += r[Math.floor(rnd() * n)]; means[b] = s / n; }
  means.sort();
  const lo = means[Math.floor(B * .025)], hi = means[Math.floor(B * .975)];
  const sd = Math.sqrt(r.reduce((a, x) => a + (x - mean) ** 2, 0) / (n - 1));
  const need = mean !== 0 && sd > 0 ? Math.ceil((1.96 * sd / Math.abs(mean)) ** 2) : Infinity;
  return {n, mean, lo, hi, more: lo > 0 || hi < 0 ? 0 : (isFinite(need) ? Math.max(1, need - n) : null)};
}
function edgeHtml(arr){
  const e = edgeCI(arr);
  if (!e) return `<div class="edge"><div class="lbl2" data-term="Edge">Edge oder Zufall?</div><div class="mutedtxt">Ab 5 Trades zeigt dir die App, in welcher Spanne dein echter Ø R pro Trade wahrscheinlich liegt.</div></div>`;
  const span = Math.max(Math.abs(e.lo), Math.abs(e.hi), .5) * 1.15, pos = v => 50 + 50 * v / span;
  const verdict = e.lo > 0 ? `<b class="good">Edge belegt</b> – selbst im ungünstigen Fall liegst du im Plus.`
    : e.hi < 0 ? `<b class="bad">So verliert das System</b> – auch im günstigen Fall liegt der Ø unter null.`
    : `<b>Noch offen</b> – ${e.more ? `ca. ${e.more > 999 ? 'sehr viele' : e.more} weitere Trades` : 'mehr Trades'} nötig, bis das Ergebnis kein Zufall mehr sein kann.`;
  return `<div class="edge"><div class="lbl2" data-term="Edge">Edge oder Zufall?</div>
    <div>Dein Ø liegt mit 95 % Wahrscheinlichkeit zwischen <b>${sgn(e.lo)} R</b> und <b>${sgn(e.hi)} R</b> pro Trade.</div>
    <div class="cibar" aria-hidden="true"><span class="zero" style="left:50%"></span>
      <span class="range ${e.lo > 0 ? 'good' : e.hi < 0 ? 'bad' : ''}" style="left:${pos(e.lo)}%;width:${pos(e.hi) - pos(e.lo)}%"></span>
      <span class="dot" style="left:${pos(e.mean)}%"></span></div>
    <div class="mutedtxt">${verdict}</div></div>`;
}

/* ---------- Wochenrückblick ---------- */
function weekStats(){
  const since = Date.now() - 7 * 86400000, arr = journalList().filter(t => Date.parse(t.date) >= since);
  return {arr, k: kpis(arr), costs: mistakeCosts(journalList().filter(t => Date.parse(t.date) >= Date.now() - 90 * 86400000))};
}
function reviewDue(){
  const r = S.get('reviews', []), last = r[0], wd = new Date().getDay();
  if (last && last.week === isoWeek()) return false;
  if (!cache.journal.length) return false;
  return wd === 0 || wd === 6 || wd === 1 || (last && Date.now() - last.at > 8 * 86400000);
}
function weeklyReview(){
  if (curView !== 'review') enter();
  stopTimers(); curView = 'review'; progress(null); closeModal();
  const {arr, k, costs} = weekStats(), s = streakInfo(), g = S.get('weekGoal', null);
  const top = costs[0];
  const brokenRule = RULES.map(([key, l]) => [key, l, arr.filter(x => x.rules && x.rules[key] === false).length]).sort((a, b) => b[2] - a[2])[0];
  const suggestKey = top ? top.k : (brokenRule && brokenRule[2] ? 'plan' : 'frueh');
  const sug = ifthen(suggestKey);
  st.rev = {rating: null, wenn: sug[0], dann: sug[1]};
  render(`<div class="card">
    ${backBtn('ret()')}<div class="tag">◷ Wochenrückblick · 2 Minuten</div>
    <div class="q">Deine Woche</div>
    <div class="jstats">
      <div class="kpi"><div class="v">${k.n}</div><div class="l">Trades</div></div>
      <div class="kpi ${k.exp > 0 ? 'good' : k.exp < 0 ? 'bad' : ''}"><div class="v">${k.n ? sgn(k.exp) : '–'}</div><div class="l">Ø R / Trade</div></div>
      <div class="kpi"><div class="v">${isFinite(k.rule) ? fmt(k.rule, 0) + ' %' : '–'}</div><div class="l">Regeltreue</div></div>
    </div>
    ${weekResultHtml(arr, k)}
    <p class="hint small">Disziplin-Serie: <b>${s.cur} ${s.cur === 1 ? 'Tag' : 'Tage'}</b> · Rekord ${s.best}</p>
    ${g && g.week !== isoWeek() ? `<div class="f"><label>Letzte Woche wolltest du: „Wenn ${esc(g.wenn)}, dann ${esc(g.dann)}.“ Hat das geklappt?</label>
      ${chipRow('rv_rate', [['ja','Ja'],['teils','Teilweise'],['nein','Nein']], null)}</div>` : ''}
    ${top ? `<div class="ins"><div class="lbl2">Dein teuerster Fehler (90 Tage)</div><div><b>${esc(top.l)}</b> – ${top.n}× · kostet dich geschätzt <b class="bad">${fmt(top.cost, 1)} R</b></div></div>`
      : '<p class="hint small">Markier nach jedem Trade, ob ein Fehler dabei war – dann zeigt dir der Rückblick, welcher dich am meisten kostet.</p>'}
    ${shotsStrip(arr)}
    <div class="lbl3 first" style="margin-top:14px">Ein Ziel für nächste Woche</div>
    <p class="hint small">Nur eins. Als Wenn-dann-Plan – so wird es im entscheidenden Moment automatisch abgerufen.</p>
    <div class="f"><label for="rv_wenn">Wenn …</label><input id="rv_wenn" value="${esc(sug[0])}" maxlength="120" autocomplete="off"></div>
    <div class="f"><label for="rv_dann">… dann …</label><input id="rv_dann" value="${esc(sug[1])}" maxlength="140" autocomplete="off"></div>
    <div class="chips small" id="rv_sug">${Object.entries(IFTHEN).filter(([key]) => key !== suggestKey).slice(0, 4).map(([key, v]) => `<button type="button" data-v="${key}">${esc(MIST_TXT[key] || 'Regel brechen')}</button>`).join('')}</div>
    <div class="row"><button class="btn ghost" onclick="ret()">Später</button><button class="btn pri" onclick="saveReview()">Ziel festlegen</button></div>
  </div>`);
  later(() => {
    bindChips('rv_rate', v => st.rev.rating = v);
    $$('#rv_sug button').forEach(b => b.onclick = () => { const v = ifthen(b.dataset.v); $('#rv_wenn').value = v[0]; $('#rv_dann').value = v[1]; });
    loadShotThumbs();
  });
}
function saveReview(){
  const wenn = $('#rv_wenn').value.trim().slice(0, 120), dann = $('#rv_dann').value.trim().slice(0, 140);
  if (!wenn || !dann){ ($('#rv_wenn').value ? $('#rv_dann') : $('#rv_wenn')).focus(); return; }
  const prev = S.get('weekGoal', null), {k} = weekStats();
  const list = S.get('reviews', []).slice();
  list.unshift({week: isoWeek(), at: Date.now(), n: k.n, exp: isFinite(k.exp) ? +k.exp.toFixed(2) : null, rule: isFinite(k.rule) ? Math.round(k.rule) : null,
    prevGoal: prev && prev.week !== isoWeek() ? prev : null, rating: st.rev && st.rev.rating || null});
  S.set('reviews', list.slice(0, 60));
  S.set('weekGoal', {week: isoWeek(), wenn, dann});
  confetti(); Snd.kept(); ret();
}
const goalLine = () => { const g = S.get('weekGoal', null); return g ? `<b>Wenn</b> ${esc(g.wenn)}, <b>dann</b> ${esc(g.dann)}.` : ''; };

/* ---------- Push-Erinnerungen ---------- */
const PUSH_URL = (TPC.SUPABASE_URL || '') + '/functions/v1/push';
const pushSupported = () => 'serviceWorker' in navigator && 'PushManager' in window && 'Notification' in window;
const isStandalone = () => matchMedia('(display-mode: standalone)').matches || navigator.standalone === true;
const isIOS = () => /iPhone|iPad|iPod/.test(navigator.userAgent);
function urlB64ToUint8(s){ s = s.replace(/-/g, '+').replace(/_/g, '/'); while (s.length % 4) s += '='; const b = atob(s), a = new Uint8Array(b.length); for (let i = 0; i < b.length; i++) a[i] = b.charCodeAt(i); return a; }
async function pushCall(body){
  const { data } = await sb.auth.getSession(); const tok = data.session && data.session.access_token;
  const r = await fetch(PUSH_URL, {method: 'POST', headers: {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + tok}, body: JSON.stringify(body)});
  return r.json().catch(() => ({}));
}
function reminderCfg(){ return Object.assign({time: '15:00', days: [1,2,3,4,5], review: true, on: false}, cfg.reminder || {}); }
function reminderHtml(){
  const r = reminderCfg();
  const blocked = !HAS_BACKEND ? 'Erinnerungen brauchen ein Konto.' : !Sync.ok ? 'Melde dich an, um Erinnerungen zu nutzen.'
    : isIOS() && !isStandalone() ? 'Am iPhone: erst über Teilen → „Zum Home-Bildschirm“ installieren, dann hier aktivieren.'
    : !pushSupported() ? 'Dieser Browser unterstützt keine Push-Nachrichten.' : '';
  return `<div class="lbl3">Erinnerung</div>
    <p class="hint small">Feste Zeit, fester Auslöser: <b>Wenn</b> es ${esc(r.time)} Uhr ist und ich an die Charts gehe, <b>dann</b> mache ich zuerst den Tages-Check.</p>
    <div class="grid2"><div class="f"><label for="rm_time">Uhrzeit</label><input id="rm_time" type="time" value="${esc(r.time)}"></div>
      <div class="f"><label>Wochenrückblick</label>${chipRow('rm_rev', [['an','So 18 Uhr'],['aus','Aus']], r.review ? 'an' : 'aus', 'small')}</div></div>
    <div class="f"><label>Tage</label><div class="chips small" id="rm_days">${WEEKDAYS.map(([d, l]) => `<button type="button" data-v="${d}" class="${r.days.includes(d) ? 'on' : ''}">${l}</button>`).join('')}</div></div>
    ${blocked ? `<p class="hint small warnc">${blocked}</p>` : `<div class="row" style="margin-top:0"><button class="btn ${r.on ? 'ghost' : 'pri'}" style="flex:1" id="rm_btn" onclick="pushToggle()">${r.on ? 'Erinnerung aktualisieren' : 'Push-Erinnerung aktivieren'}</button>
      ${r.on ? `<button class="btn ghost" style="flex:1" onclick="pushTest(this)">Test senden</button>` : ''}</div>
      ${r.on ? `<button class="linkbtn" onclick="pushOff()">Erinnerung ausschalten</button>` : ''}`}
    <p class="msg" id="rm_msg"></p>`;
}
function bindReminder(){ bindChips('rm_days', null, true); bindChips('rm_rev'); }
function readReminder(){
  const r = reminderCfg(), t = $('#rm_time');
  if (!t) return r;
  const days = $$('#rm_days button.on').map(b => +b.dataset.v);
  return Object.assign(r, {time: t.value || '15:00', days: days.length ? days : [1,2,3,4,5], review: (chipVal('rm_rev') || 'an') === 'an'});
}
async function pushToggle(){
  const msg = $('#rm_msg'), btn = $('#rm_btn'); const r = readReminder();
  const say = (t, ok) => { msg.textContent = t; msg.className = 'msg ' + (ok ? 'good' : 'bad'); };
  btn.disabled = true;
  try {
    const perm = await Notification.requestPermission();
    if (perm !== 'granted'){ btn.disabled = false; return say('Benachrichtigungen sind blockiert. Erlaube sie in den Einstellungen deines Geräts für NoTilt.'); }
    const reg = await navigator.serviceWorker.ready;
    let sub = await reg.pushManager.getSubscription();
    if (!sub){
      const { pub } = await fetch(PUSH_URL).then(x => x.json());
      sub = await reg.pushManager.subscribe({userVisibleOnly: true, applicationServerKey: urlB64ToUint8(pub)});
    }
    const res = await pushCall({action: 'subscribe', sub: sub.toJSON(), time: r.time, days: r.days, review: r.review,
      tz: Intl.DateTimeFormat().resolvedOptions().timeZone, lang: (typeof LANG !== 'undefined' && LANG !== 'de' ? 'en' : 'de')});
    if (!res.ok) throw new Error(res.error || 'Server');
    cfg.reminder = Object.assign(r, {on: true}); saveCfg();
    const t = await pushCall({action: 'test'}).catch(() => ({}));
    const sent = t.results && t.results.some(x => x >= 200 && x < 300);
    say(sent ? 'Aktiv ✓ Eine Testnachricht ist unterwegs – sie kommt in ein paar Sekunden.' : 'Aktiv ✓ Du bekommst die Erinnerung zur gewählten Zeit.', true);
    setTimeout(settings, 3000);
  } catch (e){ btn.disabled = false; say('Das hat nicht geklappt: ' + (e && e.message || e)); }
}
async function pushTest(btn){
  btn.disabled = true; const res = await pushCall({action: 'test'}).catch(() => ({}));
  const ok = res.results && res.results.some(s => s >= 200 && s < 300);
  const m = $('#rm_msg'); m.textContent = ok ? 'Gesendet – kommt gleich an.' : 'Kein aktives Gerät gefunden. Aktiviere die Erinnerung auf diesem Gerät erneut.'; m.className = 'msg ' + (ok ? 'good' : 'bad');
  btn.disabled = false;
}
async function pushOff(){
  try { const reg = await navigator.serviceWorker.ready, sub = await reg.pushManager.getSubscription();
    if (sub){ await pushCall({action: 'unsubscribe', endpoint: sub.endpoint}); await sub.unsubscribe(); } } catch (e){}
  cfg.reminder = Object.assign(reminderCfg(), {on: false}); saveCfg(); settings();
}

/* ---------- Screenshots (Supabase Storage, privat) ---------- */
const SHOT_BUCKET = 'shots';
const shotsReady = () => HAS_BACKEND && Sync.ok;
async function compressImage(file){
  const img = await createImageBitmap(file);
  const max = 1600, sc = Math.min(1, max / Math.max(img.width, img.height));
  const c = document.createElement('canvas'); c.width = Math.round(img.width * sc); c.height = Math.round(img.height * sc);
  c.getContext('2d').drawImage(img, 0, 0, c.width, c.height);
  return await new Promise(res => c.toBlob(res, 'image/jpeg', .82));
}
async function uploadShots(tradeId, files){
  const out = [];
  for (const f of [...files].slice(0, 4)){
    try {
      const blob = await compressImage(f), path = `${Sync.uid}/${tradeId}/${Date.now()}${Math.floor(Math.random()*1000)}.jpg`;
      const { error } = await sb.storage.from(SHOT_BUCKET).upload(path, blob, {contentType: 'image/jpeg', upsert: false});
      if (!error) out.push(path);
    } catch (e){}
  }
  return out;
}
async function attachShots(tradeId, files){
  if (!files || !files.length || !shotsReady()) return;
  const paths = await uploadShots(tradeId, files);
  const t = cache.journal.find(j => j.id === tradeId); if (!t || !paths.length) return;
  updateTrade(Object.assign({}, t, {shots: (t.shots || []).concat(paths).slice(0, 6)}));
}
async function removeShots(paths){ if (paths && paths.length && shotsReady()) { try { await sb.storage.from(SHOT_BUCKET).remove(paths); } catch (e){} } }
const shotUrlCache = {};
async function shotUrls(paths){
  const need = paths.filter(p => !shotUrlCache[p] || shotUrlCache[p].exp < Date.now());
  if (need.length && shotsReady()){
    const { data } = await sb.storage.from(SHOT_BUCKET).createSignedUrls(need, 3600);
    (data || []).forEach(d => { if (d.signedUrl) shotUrlCache[d.path] = {url: d.signedUrl, exp: Date.now() + 3300000}; });
  }
  return paths.map(p => shotUrlCache[p] && shotUrlCache[p].url).filter(Boolean);
}
function shotInput(id){
  if (!shotsReady()) return HAS_BACKEND ? '<p class="hint small">Screenshots speichern geht mit Konto.</p>' : '';
  return `<div class="f"><label>Chart-Screenshots (optional)</label>
    <label class="shotpick"><input type="file" id="${id}" accept="image/*" multiple hidden onchange="shotPicked(this)"><span>＋ Bild auswählen</span><em id="${id}_n"></em></label></div>`;
}
function shotPicked(inp){ const n = $('#' + inp.id + '_n'); if (n) n.textContent = inp.files.length ? inp.files.length + ' ausgewählt' : ''; }
function shotGrid(t){
  if (!t.shots || !t.shots.length) return '';
  return `<div class="shots" data-paths="${esc(JSON.stringify(t.shots))}">${t.shots.map(() => '<span class="shot ph"></span>').join('')}</div>`;
}
function shotsStrip(arr){
  const paths = arr.flatMap(t => t.shots || []).slice(0, 12);
  return paths.length ? `<div class="lbl3 first" style="margin-top:14px">Deine Charts der Woche</div><div class="shots" data-paths="${esc(JSON.stringify(paths))}">${paths.map(() => '<span class="shot ph"></span>').join('')}</div>` : '';
}
async function loadShotThumbs(){
  for (const box of $$('.shots[data-paths]')){
    const paths = JSON.parse(box.dataset.paths || '[]'), urls = await shotUrls(paths);
    box.innerHTML = urls.map(u => `<a class="shot" href="${esc(u)}" target="_blank" rel="noopener"><img src="${esc(u)}" alt="Chart-Screenshot" loading="lazy"></a>`).join('');
  }
}

function streakInfoModal(){
  const s = streakInfo();
  modal(`<div class="tag">◆ Disziplin-Serie</div><div class="q">${s.cur} ${s.cur===1?'Tag':'Tage'} in Folge</div>
    <p class="hint">Ein Tag zählt, wenn du vor dem Traden den Tages-Check gemacht und dich bei jedem Trade an deine Regeln gehalten hast. Gewinne spielen keine Rolle – nur dein Verhalten.</p>
    <p class="hint">Tage ohne Trade zählen nicht gegen dich. Nicht zu traden ist nie ein Fehler.</p>
    <p class="hint">Pro Woche fängt <b>ein Joker</b> einen Ausrutscher ab${s.jokerLeft ? ' – diese Woche ist er noch frei.' : ' – diese Woche ist er schon verbraucht.'} Rekord: <b>${s.best}</b>.</p>
    <div class="row"><button class="btn pri" onclick="closeModal()">Verstanden</button></div>`);
}
/* ==================================================================
   Trades von der Börse importieren (CSV von Bitget, Binance u. a.)
   – erkennt Spalten automatisch, rechnet Gewinn/Verlust in R um,
     fasst Teil-Ausführungen zusammen und überspringt Doppelte
   ================================================================== */
const IMP_COLS = {
  time:  {label:'Zeitpunkt', req:true,  keys:['closing time','close time','closed time','date(utc)','time(utc)','date','time','datum','zeit','created','updated']},
  coin:  {label:'Coin / Kontrakt', req:false, keys:['futures','symbol','contract','pair','trading pair','coin','ticker','asset','kontrakt','instrument','market']},
  side:  {label:'Richtung', req:false, keys:['direction','position side','side','richtung','type','position']},
  pnl:   {label:'Gewinn/Verlust (USDT)', req:true, keys:['net profits','net profit','net pnl','realized p/l','realized pnl','realised pnl','realized profit','closed pnl','closing pnl','pnl','profit','gewinn','ergebnis']},
  fee:   {label:'Gebühr (optional)', req:false, keys:['trading fee','fee','fees','commission','gebühr','gebuehr']}
};
let IMP = null;

function csvParse(text){
  text = text.replace(/^﻿/, '');
  const first = text.split(/\r?\n/)[0] || '';
  const delim = [',', ';', '\t'].map(d => [d, first.split(d).length]).sort((a, b) => b[1] - a[1])[0][0];
  const rows = []; let row = [], cell = '', q = false;
  for (let i = 0; i < text.length; i++){
    const c = text[i];
    if (q){ if (c === '"'){ if (text[i+1] === '"'){ cell += '"'; i++; } else q = false; } else cell += c; }
    else if (c === '"') q = true;
    else if (c === delim){ row.push(cell); cell = ''; }
    else if (c === '\n' || c === '\r'){ if (c === '\r' && text[i+1] === '\n') i++; row.push(cell); rows.push(row); row = []; cell = ''; }
    else cell += c;
  }
  if (cell !== '' || row.length){ row.push(cell); rows.push(row); }
  return rows.map(r => r.map(x => x.trim())).filter(r => r.some(Boolean));
}
function guessCols(head){
  const h = head.map(x => x.toLowerCase().replace(/\s+/g, ' ').trim()), used = new Set(), out = {};
  for (const [k, def] of Object.entries(IMP_COLS)){
    let best = -1;
    for (const key of def.keys){
      const i = h.findIndex((x, j) => !used.has(j) && (x === key || x.startsWith(key)));
      if (i >= 0){ best = i; break; }
    }
    if (best < 0) for (const key of def.keys){ const i = h.findIndex((x, j) => !used.has(j) && x.includes(key)); if (i >= 0){ best = i; break; } }
    if (best >= 0){ out[k] = best; used.add(best); }
  }
  return out;
}
function parseNum(v){
  let s = String(v ?? '').replace(/[^\d.,\-+eE]/g, '');
  if (!s) return NaN;
  if (s.includes('.') && s.includes(',')) s = s.lastIndexOf(',') > s.lastIndexOf('.') ? s.replace(/\./g, '').replace(',', '.') : s.replace(/,/g, '');
  else if (s.includes(',')) s = /^-?\d{1,3}(,\d{3}){2,}$/.test(s) ? s.replace(/,/g, '') : s.replace(',', '.');
  return parseFloat(s);
}
function parseTime(v, utc){
  const s = String(v || '').trim(); if (!s) return null;
  if (/^\d{12,13}$/.test(s)) return new Date(+s);
  if (/^\d{10}$/.test(s)) return new Date(+s * 1000);
  let m = s.match(/^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})[ T]?(\d{1,2})?:?(\d{2})?:?(\d{2})?/);
  let Y, Mo, D, h = 0, mi = 0, se = 0;
  if (m){ [Y, Mo, D] = [+m[1], +m[2], +m[3]]; h = +(m[4]||0); mi = +(m[5]||0); se = +(m[6]||0); }
  else if ((m = s.match(/^(\d{1,2})[./-](\d{1,2})[./-](\d{4})[ ,T]*(\d{1,2})?:?(\d{2})?:?(\d{2})?/))){
    let a = +m[1], b = +m[2]; if (b > 12 && a <= 12) [a, b] = [b, a];   // US-Format erkennen
    [D, Mo, Y] = [a, b, +m[3]]; h = +(m[4]||0); mi = +(m[5]||0); se = +(m[6]||0);
  } else { const d = new Date(s); return isNaN(d) ? null : d; }
  const d = utc ? new Date(Date.UTC(Y, Mo-1, D, h, mi, se)) : new Date(Y, Mo-1, D, h, mi, se);
  return isNaN(d) ? null : d;
}
const cleanCoin = s => { let c = String(s || '').toUpperCase().replace(/\s.*$/, '').replace(/[-_/:]/g, ''), prev;
  do { prev = c; c = c.replace(/(UMCBL|DMCBL|CMCBL|PERP|SWAP|USDT|USDC|BUSD|USD)$/, ''); } while (c !== prev && c.length > 2);
  return c.slice(0, 12); };
function sideToDir(v, fills){
  const s = String(v || '').toLowerCase();
  if (/long|多/.test(s)) return 'long';
  if (/short|空/.test(s)) return 'short';
  if (/buy|kauf/.test(s)) return fills ? 'short' : 'long';    // Schließ-Order „Kauf“ beendet einen Short
  if (/sell|verkauf/.test(s)) return fills ? 'long' : 'short';
  return '';
}
const hashId = s => { let h = 2166136261; for (let i = 0; i < s.length; i++){ h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } return 'imp' + (h >>> 0).toString(36); };

function buildImport(){
  const {rows, map, oneR, feeOff, utc} = IMP;
  const body = rows.slice(1);
  let recs = body.map(r => ({t: parseTime(r[map.time], utc), coin: map.coin >= 0 ? cleanCoin(r[map.coin]) : '', side: map.side >= 0 ? r[map.side] : '',
    pnl: parseNum(r[map.pnl]), fee: map.fee >= 0 ? Math.abs(parseNum(r[map.fee])) || 0 : 0})).filter(x => x.t && isFinite(x.pnl));
  const zero = recs.filter(x => x.pnl === 0).length, fills = recs.length > 4 && zero / recs.length > .3;
  if (fills){                                    // Ausführungen: nur Schließungen, zeitnah gleiche Coins zusammenfassen
    const open = {}, close = [], grouped = [];
    recs.sort((a, b) => a.t - b.t).forEach(x => {          // Gebühren der Eröffnung dem späteren Schließen zurechnen
      if (x.pnl === 0){ open[x.coin] = (open[x.coin] || 0) + x.fee; return; }
      close.push(Object.assign({}, x, {fee: x.fee + (open[x.coin] || 0)})); open[x.coin] = 0;
    });
    close.forEach(x => { const g = grouped[grouped.length - 1];
      if (g && g.coin === x.coin && x.t - g.t2 <= 120000){ g.pnl += x.pnl; g.fee += x.fee; g.t2 = x.t; } else grouped.push(Object.assign({}, x, {t2: x.t})); });
    recs = grouped;
  }
  const have = new Set(cache.journal.map(j => j.id));
  const trades = recs.map(x => {
    const pnl = x.pnl - (feeOff ? x.fee : 0), id = hashId(x.coin + '|' + x.t.toISOString() + '|' + x.pnl.toFixed(4));
    return {id, date: x.t.toISOString(), coin: x.coin, dir: sideToDir(x.side, fills), pnl: +pnl.toFixed(4), R: +(pnl / oneR).toFixed(2), art: 'import', checklist: false, strat: ''};
  });
  const fresh = trades.filter(t => !have.has(t.id));
  return {trades, fresh, dup: trades.length - fresh.length, fills};
}
function importOpen(){
  modal(`<div class="tag">⇩ Trades importieren</div>
    <div class="q">Von der Börse übernehmen</div>
    <p class="hint">Exportiere deinen Verlauf als CSV und wähle die Datei hier aus. Funktioniert mit Bitget, Binance und den meisten anderen Börsen.</p>
    <details class="howto"><summary>Wo finde ich den Export?</summary>
      <p><b>Bitget</b> (am Computer): Aufträge → Futures → Positions- oder Auftragsverlauf → Herunterladen. Alternativ im Profil unter „Datenexport“.</p>
      <p><b>Binance</b>: Aufträge → USDⓈ-M-Futures → Handelsverlauf → Exportieren.</p>
      <p>Die Menünamen können sich je nach Version leicht unterscheiden.</p></details>
    <label class="shotpick"><input type="file" id="imp_csv" accept=".csv,text/csv,text/plain" hidden onchange="importFile(this)"><span>＋ CSV-Datei auswählen</span><em id="imp_name"></em></label>
    <div id="imp_body"></div>
    <div class="row"><button class="btn ghost" style="flex:1" onclick="closeModal()">Abbrechen</button></div>`);
}
function importFile(inp){
  const f = inp.files && inp.files[0]; if (!f) return;
  $('#imp_name').textContent = f.name;
  const rd = new FileReader();
  rd.onload = () => {
    const rows = csvParse(String(rd.result || ''));
    if (rows.length < 2){ $('#imp_body').innerHTML = '<p class="msg bad">Die Datei enthält keine lesbaren Zeilen.</p>'; return; }
    const map = guessCols(rows[0]);
    IMP = {rows, map: Object.assign({time: -1, coin: -1, side: -1, pnl: -1, fee: -1}, map), oneR: +(cfg.konto * cfg.risk / 100).toFixed(2) || 10,
      feeOff: false, utc: rows[0].some(h => /utc/i.test(h))};
    const net = map.pnl >= 0 && /net/i.test(rows[0][map.pnl]);
    IMP.feeOff = map.fee >= 0 && !net;
    importRender();
  };
  rd.readAsText(f);
}
function importRender(){
  const {rows, map} = IMP, head = rows[0];
  const ok = map.time >= 0 && map.pnl >= 0;
  const res = ok ? buildImport() : null;
  const sel = k => `<select data-k="${k}"><option value="-1">–</option>${head.map((h, i) => `<option value="${i}" ${map[k] === i ? 'selected' : ''}>${esc(h || ('Spalte ' + (i+1)))}</option>`).join('')}</select>`;
  const sumR = res ? res.fresh.reduce((a, t) => a + t.R, 0) : 0;
  const span = res && res.fresh.length ? [res.fresh.reduce((a, t) => t.date < a ? t.date : a, res.fresh[0].date), res.fresh.reduce((a, t) => t.date > a ? t.date : a, res.fresh[0].date)] : null;
  $('#imp_body').innerHTML = `
    <div class="impmap">${Object.entries(IMP_COLS).map(([k, d]) => `<label>${d.label}${d.req ? ' *' : ''}${sel(k)}</label>`).join('')}</div>
    <div class="grid2"><div class="f"><label for="imp_r">1 R entspricht (USDT)</label><input id="imp_r" inputmode="decimal" value="${decStr(IMP.oneR)}"></div>
      <div class="f"><label>Gebühren abziehen</label>${chipRow('imp_fee', [['ja','Ja'],['nein','Nein']], IMP.feeOff ? 'ja' : 'nein', 'small')}</div></div>
    <p class="hint small">Dein Risiko pro Trade in USDT. Daraus wird jeder Gewinn oder Verlust in R umgerechnet – so sind alte und neue Trades vergleichbar.</p>
    ${!ok ? '<p class="msg bad">Ordne mindestens Zeitpunkt und Gewinn/Verlust einer Spalte zu.</p>'
      : `<div class="impsum"><b>${res.fresh.length}</b> Trades gefunden${res.dup ? ` · ${res.dup} schon im Journal` : ''}${res.fills ? ' · Teil-Ausführungen zusammengefasst' : ''}<br>
        ${span ? `${new Date(span[0]).toLocaleDateString(LOC())} – ${new Date(span[1]).toLocaleDateString(LOC())} · zusammen <b class="${sumR >= 0 ? 'good' : 'bad'}">${sgn(sumR, 1)} R</b>` : ''}</div>
        ${res.fresh.length ? `<div class="imptable">${res.fresh.slice(0, 5).map(t => `<div><span>${new Date(t.date).toLocaleDateString(LOC())}</span><span>${esc(t.coin || '–')} ${t.dir === 'long' ? '▲' : t.dir === 'short' ? '▼' : ''}</span><span class="${t.R >= 0 ? 'good' : 'bad'}">${sgn(t.R)} R</span></div>`).join('')}</div>` : ''}
        <p class="hint small">Importierte Trades zählen in deine Statistik. Für Regeltreue und Serie zählen sie nicht – dafür fehlen die Angaben aus der Checkliste. Du kannst sie einzeln nachpflegen.</p>
        <div class="row"><button class="btn pri" style="flex:1" ${res.fresh.length ? '' : 'disabled'} onclick="importGo()">${res.fresh.length} Trades importieren</button></div>`}`;
  $$('#imp_body select').forEach(s => s.onchange = () => { IMP.map[s.dataset.k] = +s.value; importRender(); });
  $('#imp_r').onchange = e => { const v = num(e.target.value); if (isFinite(v) && v > 0){ IMP.oneR = v; importRender(); } };
  bindChips('imp_fee', v => { IMP.feeOff = v === 'ja'; importRender(); });
}
function importGo(){
  const {fresh} = buildImport();
  fresh.forEach(t => { cache.journal.push(t); Sync.enqueue('put', t.id); });
  LS.set('journal', cache.journal); Sync.drain();
  closeModal(); jRange = 'all'; journal();
}
/* ==================================================================
   Profil: Wie stehe ich wirklich da?
   – Ergebnis in R, USDT und %, Monatskalender, profitabel ja/nein mit
     95-%-Spanne, Drawdown, Stärken, Disziplin
   ================================================================== */
let profMonth = null;
const riskUsdOf = t => isFinite(t.riskUsd) && t.riskUsd > 0 ? t.riskUsd : (cfg.konto * cfg.risk / 100);
const usdOf = t => isFinite(t.pnl) ? t.pnl : (+t.R || 0) * riskUsdOf(t);
const usd = (v, d=0) => (v > 0 ? '+' : '') + fmt(v, d) + ' USDT';
function drawdown(arr){                 // arr chronologisch
  let peak = 0, cum = 0, maxR = 0, cumU = 0, peakU = 0, maxU = 0;
  arr.forEach(t => { cum += +t.R || 0; peak = Math.max(peak, cum); maxR = Math.min(maxR, cum - peak);
    cumU += usdOf(t); peakU = Math.max(peakU, cumU); maxU = Math.min(maxU, cumU - peakU); });
  return {maxR, maxU, curR: cum - peak, curU: cumU - peakU};
}
function runs(arr){
  let w = 0, l = 0, bw = 0, bl = 0;
  arr.forEach(t => { if (t.R > 0){ w++; l = 0; } else if (t.R < 0){ l++; w = 0; } else { w = 0; l = 0; } bw = Math.max(bw, w); bl = Math.max(bl, l); });
  return {bw, bl};
}
function groupBest(arr, keyFn, min){
  const g = {}; arr.forEach(t => { const k = keyFn(t); if (k) (g[k] = g[k] || []).push(t); });
  const rows = Object.entries(g).filter(([, a]) => a.length >= min).map(([k, a]) => ({k, n: a.length, avg: a.reduce((s, t) => s + t.R, 0) / a.length}));
  rows.sort((a, b) => b.avg - a.avg);
  return rows;
}
const sessionOf = d => { const s = sessionsAt(new Date(d));
  return s.includes('New York') ? 'New York' : s.includes('London') ? 'London' : s.includes('Asia') ? 'Asien' : 'Nacht'; };

/* ---------- Woche: profitabel? ---------- */
function weekResultHtml(arr, k){
  if (!k.n) return '<p class="hint small">Diese Woche keine Trades – auch das kann die richtige Entscheidung sein.</p>';
  const r = k.sum, u = arr.reduce((s, t) => s + usdOf(t), 0), base = Math.max(1, cfg.konto - u), pct = u / base * 100;
  const clean = isFinite(k.rule) ? k.rule >= 80 : true;
  const [cls, msg] = r > 0 && clean ? ['good', 'Profitabel und regelkonform – genau so soll es sein.']
    : r > 0 ? ['warn', 'Plus trotz Regelbrüchen. Vorsicht: Glück wiederholt sich nicht zuverlässig.']
    : r <= 0 && clean ? ['neutral', 'Minus trotz sauberer Umsetzung – das ist normale Schwankung, dein Prozess stimmt.']
    : ['bad', 'Minus mit Regelbrüchen – hier liegt dein größter Hebel.'];
  // Vergleich mit früheren Wochen
  const weeks = {}; journalList().forEach(t => { const w = isoWeek(new Date(t.date)); if (w !== isoWeek()) weeks[w] = (weeks[w] || 0) + (+t.R || 0); });
  const vals = Object.values(weeks);
  let ctx = 'Noch zu wenige Wochen für einen Vergleich mit deinem Durchschnitt.';
  if (vals.length >= 4){
    const m = vals.reduce((a, b) => a + b, 0) / vals.length, sd = Math.sqrt(vals.reduce((a, v) => a + (v - m) ** 2, 0) / (vals.length - 1)) || 1, z = (r - m) / sd;
    ctx = z >= 1 ? `Deutlich besser als deine übliche Woche (Ø ${sgn(m, 1)} R).` : z <= -1 ? `Schwächer als deine übliche Woche (Ø ${sgn(m, 1)} R) – prüf, ob es am Markt oder an dir lag.`
      : `Im normalen Schwankungsbereich deiner Wochen (Ø ${sgn(m, 1)} R).`;
  }
  return `<div class="wres ${cls}"><div class="wnum"><b>${sgn(r, 1)} R</b><span>≈ ${usd(u)} · ${pct >= 0 ? '+' : ''}${fmt(pct, 1)} %</span></div>
    <div>${msg}</div><div class="mutedtxt"><span>${ctx}</span> <span>Eine Woche allein sagt wenig – entscheidend ist die Spanne über viele Trades.</span></div></div>`;
}

/* ---------- Profil-Seite ---------- */
function profile(){
  stopTimers(); curView = 'profile'; progress(null); closeModal();
  const all = journalList().slice().reverse();                 // chronologisch
  const k = kpis(all), e = edgeCI(all), dd = drawdown(all), rn = runs(all), s = streakInfo();
  const totalU = all.reduce((a, t) => a + usdOf(t), 0), start = Math.max(1, cfg.konto - totalU), pct = totalU / start * 100;
  const verdict = !e ? ['', 'Noch zu wenige Trades für ein Urteil', 'Ab 5 Trades rechnet die App, ab etwa 30–50 wird es belastbar.']
    : e.lo > 0 ? ['good', 'Du tradest nachweislich profitabel', `Mit 95 % Wahrscheinlichkeit verdienst du zwischen ${sgn(e.lo)} und ${sgn(e.hi)} R pro Trade.`]
    : e.hi < 0 ? ['bad', 'Aktuell nicht profitabel', `Dein Ø liegt mit 95 % Wahrscheinlichkeit zwischen ${sgn(e.lo)} und ${sgn(e.hi)} R pro Trade.`]
    : [e.mean > 0 ? 'warn' : '', e.mean > 0 ? 'Im Plus – aber noch nicht sicher' : 'Noch nicht profitabel', `Spanne ${sgn(e.lo)} bis ${sgn(e.hi)} R pro Trade${e.more ? ` · ca. ${e.more > 999 ? 'sehr viele' : e.more} weitere Trades bis zur Gewissheit` : ''}.`];
  const strat = groupBest(all, t => t.strat, 5), coin = groupBest(all, t => t.coin, 3), sess = groupBest(all, t => sessionOf(t.date), 5);
  const last30 = all.filter(t => Date.parse(t.date) > Date.now() - 30 * 864e5), prev30 = all.filter(t => { const d = Date.parse(t.date); return d <= Date.now() - 30 * 864e5 && d > Date.now() - 60 * 864e5; });
  const r30 = kpis(last30).rule, rp = kpis(prev30).rule, mc = mistakeCosts(all).filter(m => m.cost > 0)[0];
  const best = (rows, label) => rows.length >= 2 ? `<div class="prow"><span>${label}</span><b class="good">${esc(rows[0].k)}</b><em>${sgn(rows[0].avg)} R · ${rows[0].n}×</em></div>
      <div class="prow"><span></span><b class="bad">${esc(rows[rows.length - 1].k)}</b><em>${sgn(rows[rows.length - 1].avg)} R · ${rows[rows.length - 1].n}×</em></div>` : '';
  render(`<div class="card">
    <div class="tag">● Profil</div>
    <div class="q">Wie stehst du da?</div>
    <div class="verdict ${verdict[0]}"><b>${verdict[1]}</b><span>${verdict[2]}</span></div>
    <div class="jstats">
      <div class="kpi ${totalU > 0 ? 'good' : totalU < 0 ? 'bad' : ''}"><div class="v">${k.n ? usd(totalU) : '–'}</div><div class="l">Ergebnis</div></div>
      <div class="kpi ${pct > 0 ? 'good' : pct < 0 ? 'bad' : ''}"><div class="v">${k.n ? (pct >= 0 ? '+' : '') + fmt(pct, 1) + ' %' : '–'}</div><div class="l">vom Startkonto</div></div>
      <div class="kpi ${k.sum > 0 ? 'good' : k.sum < 0 ? 'bad' : ''}"><div class="v">${k.n ? sgn(k.sum, 1) + ' R' : '–'}</div><div class="l" data-term="R">${k.n} Trades</div></div>
      ${simple() ? '' : `<div class="kpi"><div class="v">${k.n ? fmt(k.win, 0) + ' %' : '–'}</div><div class="l" data-term="Treffer">Treffer</div></div>
      <div class="kpi bad"><div class="v">${k.n ? fmt(dd.maxR, 1) + ' R' : '–'}</div><div class="l" data-term="DD">Max. Drawdown</div></div>
      <div class="kpi"><div class="v">${isFinite(k.pf) ? fmt(k.pf) : k.pf === Infinity ? '∞' : '–'}</div><div class="l" data-term="PF">Profit-Faktor</div></div>`}
    </div>
    <div class="eqwrap"><canvas id="eq" height="150" aria-label="Kapitalkurve in R"></canvas></div>
    ${k.n ? `<p class="hint small">Größter Rückgang vom Hoch: <b>${fmt(dd.maxR, 1)} R</b> (≈ ${usd(dd.maxU)})${dd.curR < 0 ? ` · aktuell ${fmt(dd.curR, 1)} R unter dem Hoch` : ' · du stehst am Hoch'}. Längste Serie: ${rn.bw} Gewinne, ${rn.bl} Verluste in Folge.</p>` : ''}
    <div class="lbl3">Kalender</div>
    <div id="cal"></div>
    ${(() => { const sw = best(strat, 'Strategie') + best(coin, 'Coin') + best(sess, 'Tageszeit') || '<p class="hint small">Ab ein paar Trades pro Strategie, Coin und Tageszeit siehst du hier, wo deine Edge liegt.</p>';
      return simple() ? `<details class="more"><summary>Stärken & Schwächen, Treffer, Drawdown</summary><div class="jstats">
        <div class="kpi"><div class="v">${k.n ? fmt(k.win, 0) + ' %' : '–'}</div><div class="l" data-term="Treffer">Treffer</div></div>
        <div class="kpi bad"><div class="v">${k.n ? fmt(dd.maxR, 1) + ' R' : '–'}</div><div class="l" data-term="DD">Max. Drawdown</div></div>
        <div class="kpi"><div class="v">${isFinite(k.pf) ? fmt(k.pf) : k.pf === Infinity ? '∞' : '–'}</div><div class="l" data-term="PF">Profit-Faktor</div></div></div>${sw}</details>`
        : `<div class="lbl3">Stärken & Schwächen</div>${sw}`; })()}
    <div class="lbl3">Disziplin</div>
    <div class="prow"><span>Serie</span><b>${s.cur} ${s.cur === 1 ? 'Tag' : 'Tage'}</b><em>Rekord ${s.best}</em></div>
    <div class="prow"><span data-term="Regeltreue">Regeltreue 30 Tage</span><b>${isFinite(r30) ? fmt(r30, 0) + ' %' : '–'}</b><em>${isFinite(r30) && isFinite(rp) ? (r30 >= rp ? '▲ ' : '▼ ') + 'vorher ' + fmt(rp, 0) + ' %' : ''}</em></div>
    ${mc ? `<div class="prow"><span>Teuerster Fehler</span><b class="bad">${esc(mc.l)}</b><em>≈ ${fmt(mc.cost, 1)} R</em></div>` : ''}
    <div class="row"><button class="btn ghost" style="flex:1" onclick="weeklyReview()">Wochenrückblick</button><button class="btn ghost" style="flex:1" onclick="journal()">Zum Journal</button></div>
  </div>`);
  later(() => { drawEquity(all); calendar(); });
}
function calendar(){
  const box = $('#cal'); if (!box) return;
  const now = new Date(); if (!profMonth) profMonth = new Date(now.getFullYear(), now.getMonth(), 1);
  const y = profMonth.getFullYear(), m = profMonth.getMonth(), days = new Date(y, m + 1, 0).getDate(), lead = (new Date(y, m, 1).getDay() + 6) % 7;
  const byDay = {}; journalList().forEach(t => { const d = new Date(t.date); if (d.getFullYear() === y && d.getMonth() === m){ const k = d.getDate(); byDay[k] = byDay[k] || {r: 0, n: 0, u: 0}; byDay[k].r += +t.R || 0; byDay[k].n++; byDay[k].u += usdOf(t); } });
  const vals = Object.values(byDay), mR = vals.reduce((a, v) => a + v.r, 0), mU = vals.reduce((a, v) => a + v.u, 0), mN = vals.reduce((a, v) => a + v.n, 0);
  const max = Math.max(1, ...vals.map(v => Math.abs(v.r)));
  const name = profMonth.toLocaleDateString(LOC(), {month: 'long', year: 'numeric'});
  const wd = LANG === 'de' ? ['Mo','Di','Mi','Do','Fr','Sa','So'] : LANG === 'en' ? ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
    : [0,1,2,3,4,5,6].map(i => new Date(2024, 0, 1 + i).toLocaleDateString(LOC(), {weekday: 'short'}));
  let cells = wd.map(w => `<span class="cw">${w}</span>`).join('') + '<span class="ce"></span>'.repeat(lead);
  for (let d = 1; d <= days; d++){
    const v = byDay[d], a = v ? Math.min(1, .25 + .75 * Math.abs(v.r) / max) : 0;
    const bg = v ? (v.r >= 0 ? `rgba(52,211,153,${a})` : `rgba(248,113,113,${a})`) : '';
    cells += `<span class="cd ${v ? 'has' : ''}" style="${bg ? 'background:' + bg : ''}" title="${v ? sgn(v.r, 1) + ' R · ' + v.n + ' Trades' : ''}"><i>${d}</i>${v ? `<b>${sgn(v.r, 1)}</b>` : ''}</span>`;
  }
  box.innerHTML = `<div class="calhead"><button onclick="calMove(-1)" aria-label="Vorheriger Monat">‹</button><b>${name}</b><button onclick="calMove(1)" aria-label="Nächster Monat">›</button></div>
    <div class="calgrid">${cells}</div>
    <p class="hint small">${mN ? `Monat: <b class="${mR >= 0 ? 'good' : 'bad'}">${sgn(mR, 1)} R</b> ≈ ${usd(mU)} · ${mN} Trades` : 'Keine Trades in diesem Monat.'}</p>`;
}
function calMove(d){ profMonth = new Date(profMonth.getFullYear(), profMonth.getMonth() + d, 1); calendar(); }
/* ==================================================================
   Einsteigerfreundlich: einfache Ansicht, Begriffserklärungen, Erste Schritte
   – Progressive Disclosure (Nielsen): erst das Nötige, Details auf Wunsch
   – Weniger gleichzeitige Auswahl (Hick-Hyman, Iyengar & Lepper 2000)
   – Sichtbarer Fortschritt mit erstem Haken geschenkt (Nunes & Drèze 2006, Kivetz et al. 2006)
   – Fachbegriffe dort erklären, wo sie auftauchen, statt vorab in einem Tutorial
   ================================================================== */

/* Einfache Ansicht: Standard für neue Nutzer; wer schon viele Trades hat, sieht alles */
const simple = () => cfg.mode ? cfg.mode === 'simple' : cache.journal.length < 10;

/* ---------- Begriffe ---------- */
const TERMS = {
  R: ['R – dein Risiko als Maßeinheit', 'R ist das, was du pro Trade riskierst. Riskierst du 10 USDT, ist 1 R = 10 USDT. +2 R heißt: doppelt so viel gewonnen, wie du riskiert hast. So kannst du Trades vergleichen, egal wie groß dein Konto ist.'],
  CRV: ['CRV – Chance zu Risiko', 'Wie viel kannst du gewinnen im Vergleich zu dem, was du riskierst? CRV 2 heißt: Dein Ziel ist doppelt so weit weg wie dein Stop. Mit CRV 2 bist du schon bei jedem dritten Gewinner bei plus minus null.'],
  Bias: ['Bias – deine Richtung für heute', 'Long (steigend), Short (fallend) oder Neutral. Du suchst dann nur Trades in diese Richtung. Neutral heißt: heute kein Trade – und das ist völlig in Ordnung.'],
  Regeltreue: ['Regeltreue', 'Wie oft du dich an deinen eigenen Plan gehalten hast – egal, ob der Trade Gewinn gebracht hat. Das ist die wichtigste Zahl, die du komplett selbst in der Hand hast.'],
  Treffer: ['Trefferquote', 'Anteil deiner Trades mit Gewinn. Allein sagt sie wenig: Auch 40 % können profitabel sein, wenn deine Gewinner größer sind als deine Verlierer.'],
  PF: ['Profit-Faktor', 'Alle Gewinne geteilt durch alle Verluste. Über 1 heißt: Du verdienst Geld. Über 1,5 ist gut.'],
  DD: ['Drawdown', 'Der größte Rückgang vom bisherigen Höchststand deines Kontos. Er zeigt, wie hart deine schlechteste Phase war.'],
  Exp: ['Ø R pro Trade', 'Wie viel R du im Schnitt pro Trade gewinnst oder verlierst. Über 0 heißt: Dein System verdient auf Dauer Geld.'],
  Edge: ['Edge – echter Vorteil oder Glück?', 'Eine Edge ist ein echter Vorteil deines Systems. Ob du eine hast, zeigt sich erst nach vielen Trades – meist 30 bis 50. Vorher kann ein gutes Ergebnis auch Glück sein.'],
  Liq: ['Liquidation', 'Der Preis, bei dem die Börse deine Position zwangsweise schließt, weil dein eingesetztes Geld aufgebraucht ist. Er muss immer hinter deinem Stop liegen.'],
  NetR: ['Netto R', 'Die Summe all deiner Ergebnisse in R – also unterm Strich.']
};
function explain(k){
  const t = TERMS[k]; if (!t) return;
  modal(`<div class="tag">? Kurz erklärt</div><div class="q">${esc(t[0])}</div><p class="hint">${esc(t[1])}</p>
    <div class="row"><button class="btn pri" onclick="closeModal()">Verstanden</button></div>`);
}
document.addEventListener('click', e => {
  const el = e.target.closest && e.target.closest('[data-term]');
  if (!el) return; e.preventDefault(); e.stopPropagation(); explain(el.dataset.term);
}, true);

/* ---------- Erste Schritte ---------- */
function firstSteps(){
  const J = cache.journal, days = S.get('days', {}) || {};
  return [
    ['Strategie eingerichtet', true, ''],
    ['Ersten Tages-Check machen', Object.keys(days).length > 0 || !!S.get('lastBias', null), 'dayCheck()'],
    ['Ersten Trade über die Checkliste starten', J.some(t => t.checklist) || (S.get('stops', []) || []).length > 0 || openTrades().length > 0, 'start()'],
    ['Trade abschließen und Ergebnis eintragen', J.some(t => t.checklist), "go('journal')"],
    ['App auf den Home-Bildschirm holen', isStandalone() || !!LS.get('installDone', false), 'installHelp()'],
    ['Ersten Wochenrückblick machen', (S.get('reviews', []) || []).length > 0, 'weeklyReview()']
  ];
}
function firstStepsHtml(){
  if (LS.get('fsHide', false)) return '';
  const s = firstSteps(), done = s.filter(x => x[1]).length;
  if (done === s.length) return '';
  const next = s.findIndex(x => !x[1]);
  return `<div class="fsteps">
    <div class="fshead"><b>Erste Schritte</b><span>${done} von ${s.length} erledigt</span></div>
    <div class="fsbar"><i style="width:${Math.round(done / s.length * 100)}%"></i></div>
    ${s.map((x, i) => `<button class="fsitem ${x[1] ? 'ok' : ''} ${i === next ? 'next' : ''}" ${x[1] || !x[2] ? 'disabled' : `onclick="${x[2]}"`}><span class="fsbox">${x[1] ? '✓' : i + 1}</span><span>${x[0]}</span>${!x[1] && i === next ? '<em>Jetzt →</em>' : ''}</button>`).join('')}
    <button class="linkbtn fshide" onclick="LS.set('fsHide', true); home()">Ausblenden</button>
  </div>`;
}
function installHelp(){
  const ios = /iPhone|iPad|iPod/.test(navigator.userAgent), android = /Android/.test(navigator.userAgent);
  const steps = ios ? ['In Safari unten auf das Teilen-Symbol tippen', '„Zum Home-Bildschirm“ wählen', 'Oben rechts „Hinzufügen“ tippen']
    : android ? ['In Chrome oben rechts auf ⋮ tippen', '„App installieren“ oder „Zum Startbildschirm hinzufügen“ wählen']
    : ['In Chrome: rechts in der Adressleiste auf das Installieren-Symbol klicken', 'In Safari am Mac: Ablage → „Zum Dock hinzufügen“'];
  modal(`<div class="tag">● App installieren</div><div class="q">Hol dir NoTilt auf den Home-Bildschirm</div>
    <p class="hint">Dann öffnest du die App mit einem Tipp – wie jede andere App. Und du kannst Erinnerungen bekommen.</p>
    <ol class="steps">${steps.map(x => `<li>${x}</li>`).join('')}</ol>
    <div class="row"><button class="btn ghost" onclick="closeModal()">Später</button><button class="btn pri" onclick="LS.set('installDone', true); closeModal(); home()">Hab ich gemacht</button></div>`);
}

/* ---------- Navigation unten: aktiven Tab markieren, in Abläufen ausblenden ---------- */
function markTab(){
  document.body.dataset.view = curView;
  const map = {home:'home', journal:'journal', profile:'profile', settings:'settings', review:'profile', intrade:'home', cool:'home', stop:'home'};
  document.querySelectorAll('.tabbar button').forEach(b => b.classList.toggle('on', b.dataset.v === map[curView]));
}

/* ---------- Hinweis beim ersten Start: keine Anlageberatung, Risiko, 18+ ----------
   Muss aktiv bestätigt werden (drei Haken). Version hochzählen, wenn sich der Text ändert. */
const DISC_V = 1;
const needsDisclaimer = () => (+LS.get('discV', 0) || 0) < DISC_V && !((+cfg.discV || 0) >= DISC_V);
const DISC_ITEMS = [
  'Ich verstehe: NoTilt ist ein Werkzeug für Disziplin und Selbstbeobachtung. Die App gibt keine Anlage- oder Finanzberatung und keine Kauf- oder Verkaufsempfehlungen.',
  'Mir ist klar, dass gehebelter Handel mit Kryptowährungen zum Totalverlust des eingesetzten Geldes führen kann. Jede Entscheidung treffe ich selbst und auf eigenes Risiko.',
  'Ich bin mindestens 18 Jahre alt.'
];
function disclaimer(){
  stopTimers(); curView = 'disc'; progress(null); closeModal();
  render(`<div class="card ob disc1">
    <div class="tag">● Bevor es losgeht</div>
    <div class="q">Wichtiger Hinweis</div>
    <p class="hint">Strategie-Vorlagen, Beispiele und Auswertungen in der App dienen nur zum Lernen und zur Selbstkontrolle. Vergangene Ergebnisse – auch deine eigenen – garantieren nichts für die Zukunft. Bitte bestätige:</p>
    <div class="checks">${DISC_ITEMS.map(t => `<button class="ck" onclick="this.classList.toggle('on');discUpd()"><span class="box"></span><span>${t}</span></button>`).join('')}</div>
    <div class="row"><button class="btn pri" id="discok" style="flex:1" disabled onclick="discAccept()">Verstanden – weiter</button></div>
    <p class="disc"><a href="impressum.html">Impressum</a> · <a href="datenschutz.html">Datenschutz</a> · <a href="#" onclick="langPicker();return false" translate="no">🌐 ${langName()}</a></p>
  </div>`);
}
function discUpd(){ const b = $('#discok'); if (b) b.disabled = $$('.disc1 .ck').some(x => !x.classList.contains('on')); }
function discAccept(){
  LS.set('discV', DISC_V); cfg.discV = DISC_V; cfg.discAt = new Date().toISOString(); saveCfg(); home();
}

/* ---------- Beta-Hinweis ---------- */
function betaInfo(){
  modal(`<div class="tag">● Beta</div><div class="q">Du nutzt eine frühe Version von NoTilt</div>
    <p class="hint">Die App wird gerade mit den ersten Tradern getestet. Manches kann sich noch ändern, und hier und da kann etwas haken. Deine Daten bleiben dabei erhalten – am sichersten mit einem Konto.</p>
    <p class="hint">Danke, dass du dabei bist!</p>
    <div class="row"><button class="btn pri" onclick="closeModal()">Alles klar</button></div>`);
}
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

let st = {}, fi = 0, timers = [], quoteI = Math.floor(Math.random()*6);
let jTab = 'stats', jRange = '30', jStrat = 'alle';

/* ------------------------------------------------------------------
   Hilfen
------------------------------------------------------------------ */
const $ = s => document.querySelector(s);
const $$ = s => [...document.querySelectorAll(s)];
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const today = () => { const d = new Date(); return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0'); };
const isToday = iso => new Date(iso).toDateString() === new Date().toDateString();
const fmt = (n, d=2) => isFinite(n) ? Number(n).toLocaleString(LOC(),{minimumFractionDigits:d,maximumFractionDigits:d}) : '–';
const sgn = (n, d=2) => isFinite(n) ? (n>0?'+':'') + fmt(n,d) : '–';
/* Zahlen lesen: Komma oder Punkt als Dezimaltrenner; Tausender-Trenner werden erkannt (1.234,5 und 1,234.5) */
const num = v => { let x = String(v ?? '').trim().replace(/\s/g, '');
  if (x.includes(',') && x.includes('.')) x = x.lastIndexOf(',') > x.lastIndexOf('.') ? x.replace(/\./g, '') : x.replace(/,/g, '');
  else if (DEC === '.' && /^\d{1,3}(,\d{3})+$/.test(x)) x = x.replace(/,/g, '');
  return parseFloat(x.replace(',', '.')); };
const decStr = v => DEC === '.' ? String(v) : String(v).replace('.', ',');
const mmss = ms => { const s = Math.ceil(ms/1000); return `${String(Math.floor(s/60)).padStart(2,'0')}:${String(s%60).padStart(2,'0')}`; };
const coinFmt = s => String(s||'').toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,12);
/* Zeiten: Sessions und US-Börsenstart in der Zeitzone der jeweiligen Börse – stimmt weltweit und auch in den Wochen,
   in denen Europa und die USA die Uhr zu unterschiedlichen Terminen umstellen. News-Zeit = Ortszeit des Nutzers. */
function tzMin(zone, d = new Date()){
  const p = new Intl.DateTimeFormat('en-GB',{timeZone:zone,hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(d);
  return (+p.find(x=>x.type==='hour').value % 24)*60 + +p.find(x=>x.type==='minute').value;
}
const localMin = (d = new Date()) => d.getHours()*60 + d.getMinutes();
function sessionsAt(d = new Date()){
  const s = [], tk = tzMin('Asia/Tokyo', d), ld = tzMin('Europe/London', d), ny = tzMin('America/New_York', d);
  if (tk>=480 && tk<1020) s.push('Asia'); if (ld>=480 && ld<990) s.push('London'); if (ny>=510 && ny<960) s.push('New York');
  return s;
}
function session(){ const s = sessionsAt(); return s.length ? s.join(' + ') : 'ruhig'; }
const NY_OPEN = 570;   /* 9:30 New York */
const nyWorkday = () => !/Sat|Sun/.test(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',weekday:'short'}).format(new Date()));
function usOpenLocal(){ const m = ((NY_OPEN + localMin() - tzMin('America/New_York')) % 1440 + 1440) % 1440;
  return String(Math.floor(m/60)).padStart(2,'0') + ':' + String(m%60).padStart(2,'0'); }
const nearUSOpen = () => { const t = tzMin('America/New_York'); return nyWorkday() && t>=NY_OPEN-30 && t<=NY_OPEN+60; };
const todayTrades = () => cache.journal.filter(j => isToday(j.date));
const tradesToday = () => todayTrades().length;
/* Mehrere offene Trades gleichzeitig: 'active' ist eine Liste (alte Einzel-Trades werden übernommen) */
const openTrades = () => { const a = S.get('active', null); return !a ? [] : (Array.isArray(a) ? a : [Object.assign({id: 'o' + (a.start || 1)}, a)]).filter(x => x && x.entry !== undefined); };
const setOpen = arr => S.set('active', arr.length ? arr : null);
const getOpen = id => { const o = openTrades(); return o.find(x => x.id === id) || o[o.length - 1] || null; };
const usedToday = () => tradesToday() + openTrades().length;
let curOpen = null;
const sinceTxt = a => { const m = Math.max(1, Math.round((Date.now() - a.start) / 60000)); return m < 120 ? `seit ${m} Min.` : `seit ${Math.round(m/60)} Std.`; };
const endBtns = id => `<button class="btn green" onclick="endTrade('tp','${id}')">Profit</button><button class="btn red" onclick="endTrade('sl','${id}')">Loss</button><button class="btn ghost" onclick="endTrade('manuell','${id}')">Manuell</button>`;
const todayR = () => todayTrades().reduce((a,x) => a + (+x.R||0), 0);
const cooldownLeft = () => Math.max(0, S.get('cooldownUntil', 0) - Date.now());
const biasToday = () => { const b = S.get('lastBias', null); return b && b.date === today() ? b : null; };
const strats = () => strategies().map(s => s.name);
function newsBlock(){
  const b = biasToday(); if (!b || !b.newsTime) return null;
  const [h, m] = b.newsTime.split(':').map(Number); if (!isFinite(h)) return null;
  const diff = localMin() - (h*60+m);
  return (diff >= -cfg.newsWin && diff <= cfg.newsWin) ? b.newsTime : null;
}
function usOpenBlock(){ const t = tzMin('America/New_York'); return nyWorkday() && cfg.usOpenBlock>0 && t>=NY_OPEN && t<NY_OPEN+cfg.usOpenBlock; }
function recentCoins(){
  const out = [], b = biasToday();
  (b && b.fokus || []).forEach(c => out.includes(c) || out.push(c));
  journalList().slice(0,20).forEach(j => j.coin && !out.includes(j.coin) && out.push(j.coin));
  return out.slice(0,6);
}
function stopTimers(){ timers.forEach(clearInterval); timers.forEach(clearTimeout); timers = []; }
let lastKey = '';
function render(html, cls=''){
  const stage = $('#stage'), old = stage.firstElementChild;
  const key = curView + ':' + (curView === 'flow' ? fi : '') + ':' + cls, same = old && key === lastKey; lastKey = key;
  if (same && old._html === html){ saveNav(); markTab(); return; }   /* gleicher Inhalt: nichts neu zeichnen, kein Flackern */
  if (same){ const y = window.scrollY; stage.innerHTML = `<div class="view ${cls}">${html}</div>`; stage.firstElementChild._html = html; window.scrollTo(0, y); saveNav(); markTab(); return; }
  const show = () => { stage.innerHTML = `<div class="view ${cls}">${html}</div>`; stage.firstElementChild._html = html; window.scrollTo({top:0,behavior:'smooth'}); saveNav(); markTab(); };
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
function go(v){
  if (v === 'settings' && curView !== 'settings') settingsFrom = here();
  stopTimers(); closeModal(); retStack = []; ({home, journal, settings, review: weeklyReview, profile})[v]();
}

/* Rückweg: Unteransichten (Tages-Check, Wochenrückblick, Stopp, Anmelden) merken sich, woher du kamst,
   und bringen dich nach Speichern oder Abbrechen genau dorthin zurück – mit allen Eingaben. */
let retStack = [], settingsFrom = null;
function here(){
  if (curView === 'flow'){ const f = fi, fn = () => { fi = f; step(); }; fn.flow = true; fn.v = 'flow'; return fn; }
  const m = {journal, settings, profile, intrade: inTrade}; return m[curView] || home;
}
/* Einstellungen: Zurück speichert die Grenzen und bringt dich dorthin, woher du gekommen bist. */
function settingsBack(){ saveSettings(true); stopTimers(); closeModal(); const f = settingsFrom || home; settingsFrom = null; f(); }
function enter(){ retStack.push(here()); if (retStack.length > 8) retStack.shift(); }
function ret(){ stopTimers(); closeModal(); (retStack.pop() || home)(); }
function rerender(){ const f = ({home, journal, settings, profile})[curView]; (f || home)(); }

/* Wo du warst, überlebt Neuladen und das Schließen der App durch iOS (Checkliste 10 Min., Tabs 30 Min.). */
function saveNav(){ LS.set('nav', {v: curView, fi, st: curView === 'flow' ? st : null, thenStart: !!(st.day && st.day.thenStart), jTab, y: Math.round(window.scrollY || 0), t: Date.now(),
  sf: settingsFrom === journal ? 'journal' : settingsFrom === profile ? 'profile' : ''}); }
addEventListener('visibilitychange', () => { if (document.visibilityState === 'hidden') saveNav(); });
addEventListener('pagehide', saveNav);
function restoreNav(){
  const n = LS.get('nav', null); if (!n || !n.t) return false;
  const age = Date.now() - n.t;
  if (needsOnboarding()) return false;
  if (n.v === 'flow' && n.st && age < 10*60000){
    const b = biasToday();
    if (!b || b.b === 'neutral' || cooldownLeft() > 0 || usedToday() >= cfg.maxTrades || todayR() <= -cfg.lossLimit) return false;
    st = n.st; if (st.dir !== b.b){ st.dir = b.b; st.setupOk = false; st.ticks = null; }
    fi = Math.max(0, Math.min(+n.fi || 0, FLOW.length - 1)); step(); return true;
  }
  if (openTrades().length){ inTrade(); return true; }
  if (age > 30*60000) return false;
  if (n.v === 'day'){ dayCheck(n.thenStart); return true; }
  const f = ({journal, settings, profile})[n.v]; if (!f) return false;
  if (n.v === 'journal' && n.jTab) jTab = n.jTab;
  if (n.v === 'settings') settingsFrom = ({journal, profile})[n.sf] || null;
  f(); if (n.y) setTimeout(() => window.scrollTo(0, n.y), 300); return true;
}

/* ------------------------------------------------------------------
   Start
------------------------------------------------------------------ */
function home(){
  if (needsDisclaimer()) return disclaimer();
  curView = 'home'; progress(null);
  if (needsOnboarding()) return onboarding();
  const n = usedToday(), open = openTrades(), cd = cooldownLeft(), r = todayR(), b = biasToday();
  const locked = cd>0 || n>=cfg.maxTrades || r <= -cfg.lossLimit;
  const biasChip = !b ? `<button class="chip warn" onclick="dayCheck()">Tages-Check <b>offen</b></button>`
    : `<button class="chip" onclick="dayCheck()">Bias <b style="color:${b.b==='long'?'var(--green)':b.b==='short'?'var(--red)':'var(--mut)'}">${b.b==='long'?'Long':b.b==='short'?'Short':'Neutral'}</b> ✎</button>`;
  const openRows = open.length ? `<div class="openlist">${open.map(a => `<button class="openrow" onclick="inTrade('${a.id}')"><span class="livedot"></span><b>${esc(a.coin||'Trade')} ${a.dir==='long'?'LONG':'SHORT'}</b><span>offen · ${sinceTxt(a)}</span><em>Öffnen →</em></button>`).join('')}</div>` : '';
  render(`<div class="home"><div class="hcol hl">
    ${openRows}
    <h1>${open.length ? 'Noch ein Trade?' : 'Bereit für einen Trade?'}</h1>
    <p class="sub">Unter einer Minute. Erst die Checkliste, dann der Trade.</p>
    <div class="bigwrap">
      ${locked?'':'<div class="ring"></div><div class="ring r2"></div><div class="ring r3"></div>'}
      <button class="bigbtn ${locked?'locked':''}" onclick="start()">TRADEN<small>${cd>0?'PAUSE':locked?'LIMIT':!b?'TAGES-CHECK':'CHECKLISTE'}</small></button>
    </div>
    <div class="stats">
      ${biasChip}
      <span class="chip ${n>=cfg.maxTrades?'warn':''}">Trades <b>${n}/${cfg.maxTrades}</b></span>
      <span class="chip ${r<0?'warn':''}" data-term="R">Heute <b>${sgn(r,1)} R</b></span>
      ${simple() ? '' : `<span class="chip">Session <b>${session()}</b></span>`}
      ${b && b.newsTime ? `<span class="chip">News <b>${esc(b.newsTime)}</b></span>` : ''}
      ${cd>0?`<span class="chip warn">Pause <b id="cdh">${mmss(cd)}</b></span>`:''}
      ${(() => { const sk = streakInfo(); return sk.cur || sk.best ? `<button class="chip streak ${sk.todayDone?'done':''}" onclick="streakInfoModal()">Serie <b>${sk.cur}</b></button>` : ''; })()}
    </div>
    </div><div class="hcol hr">
    ${reviewDue() ? `<button class="reviewcta" onclick="weeklyReview()"><b>Wochenrückblick</b><span>2 Minuten – was hat funktioniert, was nicht? Ein Ziel für nächste Woche.</span></button>` : ''}
    ${S.get('weekGoal', null) ? `<p class="goal">${goalLine()}</p>` : ''}
    ${b && b.fokus && b.fokus.length ? `<p class="fokus">Fokus heute: ${b.fokus.map(c=>`<b>${esc(c)}</b>`).join(' · ')}</p>` : ''}
    ${firstStepsHtml()}
    ${simple() ? '' : `<p class="quote" id="quote">${QUOTES[quoteI]}</p>`}
    <button class="linkbtn" onclick="addManual()">Trade ohne Checkliste nachtragen</button>
  </div></div>`);
  timers.push(setInterval(()=>{ const q = $('#quote'); if(!q) return; q.style.opacity = 0;
    setTimeout(()=>{ quoteI=(quoteI+1)%QUOTES.length; q.textContent = QUOTES[quoteI]; q.style.opacity=1; }, 500); }, 7000));
  if (cd>0) timers.push(setInterval(()=>{ const c=$('#cdh'); if(!c) return; const l=cooldownLeft(); if(l<=0){ stopTimers(); Snd.gong(); home(); } else c.textContent=mmss(l); },1000));
}

function start(){
  stopTimers();
  if (needsOnboarding()) return onboarding();
  if (cooldownLeft()>0) return cooldownView();
  if (usedToday()>=cfg.maxTrades) return stopView('limit');
  if (todayR() <= -cfg.lossLimit) return stopView('verlustlimit');
  const b = biasToday();
  if (!b) return dayCheck(true);
  if (b.b==='neutral') return stopView('neutral');
  const nb = newsBlock(); if (nb) return stopView('news', `News um ${nb} Uhr. Kein neuer Trade ${cfg.newsWin} Minuten davor und danach.`);
  if (usOpenBlock()) return stopView('usopen', `Die US-Börse hat um ${usOpenLocal()} Uhr geöffnet. In den ersten ${cfg.usOpenBlock} Minuten keine neuen Trades.`);
  const o = openTrades();
  if (o.length && !start._ok){
    const rTot = o.reduce((x, a) => x + (+a.risk || cfg.risk), 0);
    return modal(`<div class="tag" style="color:var(--gold)">⚠ Schon ${o.length === 1 ? 'ein Trade' : o.length + ' Trades'} offen</div>
      <div class="q">Noch einen dazu?</div>
      <p class="hint">Gerade riskierst du ${fmt(rTot,2)} %. Mit einem weiteren Trade sind es ca. ${fmt(rTot + cfg.risk,2)} % gleichzeitig. Laufen die Trades in dieselbe Richtung, verlieren sie oft auch zusammen.</p>
      <div class="row"><button class="btn ghost" onclick="closeModal()">Lieber nicht</button><button class="btn pri" onclick="closeModal(); start._ok = true; start(); start._ok = false;">Zur Checkliste</button></div>`);
  }
  const ls = S.get('lastStrat', ''); st = {dir: b.b, risk: cfg.risk, strat: strats().includes(ls) ? ls : strats()[0]}; fi = 0; step();
}
function step(){ stopTimers(); curView = 'flow'; progress(fi); ({sigh, kopf, setup, plan, commit})[FLOW[fi]](); }
function next(){ fi++; step(); }
/* Einen Schritt zurück – Eingaben bleiben erhalten. Aus dem ersten Schritt geht es zurück zum Start. */
function back(){ if (fi <= 0) return home(); fi--; step(); }
const backBtn = (fn = 'back()') => `<button class="backbtn" type="button" onclick="${fn}" aria-label="Zurück">← Zurück</button>`;
function angstBack(){ st.risk = cfg.risk; step(); }

/* ------------------------------------------------------------------
   Tages-Check (einmal pro Tag) – Routine vor dem Handelstag
------------------------------------------------------------------ */
function dayCheck(thenStart, keep){
  if (!keep && curView !== 'day') enter();
  stopTimers(); curView = 'day'; progress(null);
  const b = biasToday() || {};
  st.day = {struktur: b.struktur||'', news: b.news||'', newsTime: b.newsTime||'', b: b.b||'', thenStart: !!thenStart};
  render(`<div class="card">
    ${backBtn('ret()')}<div class="tag">◷ Tages-Check · einmal pro Tag</div>
    <div class="q">Wie ist die Lage heute?</div>
    <p class="hint">Routine: Was ist die letzten Tage passiert? Daily → 4h → 30m, Liquidität markieren, News prüfen.</p>
    <div class="f"><label>Struktur auf Daily / 4h (primäre Swing Levels)</label>
      <div class="seg" id="d_str"><button data-v="bull">Bullisch</button><button data-v="bear">Bärisch</button><button data-v="unklar">Unklar</button></div></div>
    <div class="f"><label>Wichtige News heute (3 Sterne)?</label>
      <div class="seg" id="d_news"><button data-v="nein">Keine</button><button data-v="ja">Ja, um …</button></div>
      <input id="d_time" type="time" value="${esc(st.day.newsTime)}" ${st.day.news==='ja'?'':'hidden'} aria-label="Uhrzeit der News"></div>
    <div class="f"><label data-term="Bias">Dein Bias heute</label>
      <div class="seg" id="d_bias"><button data-v="long" class="lg">Long</button><button data-v="short" class="sh">Short</button><button data-v="neutral">Neutral</button></div></div>
    <div class="f"><label for="d_fokus">Fokus-Coins mit Alert (optional, mit Komma)</label>
      <input id="d_fokus" placeholder="z. B. XRP, SOL, ENA" value="${esc((b.fokus||[]).join(', '))}" autocomplete="off"></div>
    <div class="f"><label for="d_note">Gedanken für heute (optional)</label>
      <input id="d_note" placeholder="z. B. Montag, erster Börsenstart der Woche – erst beobachten" value="${esc(b.note||'')}" autocomplete="off"></div>
    <div class="row"><button class="btn ghost" onclick="ret()">Abbrechen</button><button class="btn pri" id="d_go" onclick="saveDay()">Bias speichern</button></div>
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
  markDay();
  S.set('lastBias', {b:d.b, date:today(), struktur:d.struktur, news:d.news, newsTime: d.news==='ja' ? d.newsTime : '', fokus, note: $('#d_note').value.trim().slice(0,300)});
  const fn = retStack.pop();
  if (d.b === 'neutral'){ retStack = []; return stopView('neutral'); }
  if (d.thenStart) return start();
  /* Zurück in die Checkliste: neue Richtung übernehmen, Setup für die neue Richtung neu abhaken */
  if (fn && fn.flow && st.dir !== d.b){ st.dir = d.b; st.setupOk = false; st.ticks = null; }
  (fn || home)();
}

/* ------------------------------------------------------------------
   1 · Atmen
------------------------------------------------------------------ */
function sigh(){
  render(`<div class="card breath">
    ${backBtn()}<div class="tag">◎ Atmen · 9 Sekunden</div>
    <div class="q">Ein tiefer Seufzer</div>
    <p class="hint">Durch die Nase einatmen, kurz nachatmen, dann lange durch den Mund aus.${cfg.sound!==false ? ' Mit Ton kannst du dabei die Augen schließen.' : ''}</p>
    <div class="circle"><div class="core" id="core"></div>
      <svg viewBox="0 0 216 216" aria-hidden="true"><circle id="arc" cx="108" cy="108" r="104" stroke-dasharray="653" stroke-dashoffset="653"/></svg>
      <div class="lab" id="blab">Einatmen</div></div>
    <div class="row"><button class="btn ghost" onclick="home()">Abbrechen</button><button class="btn ghost" style="flex:1" onclick="next()">Überspringen</button></div>
  </div>`);
  const P = [[0,2600,'Tief einatmen',.82],[2600,3600,'Nochmal kurz nach',1],[3600,9000,'Langsam ausatmen …',.42]];
  const t0 = Date.now() + 250; let lastPh = null;
  timers.push(setInterval(() => {
    const core = $('#core'); if (!core) return;
    const el = Date.now()-t0; if (el < 0) return;
    const ph = P.find(p => el>=p[0] && el<p[1]);
    if (ph && ph !== lastPh){ lastPh = ph; Snd[['inhale','topup','exhale'][P.indexOf(ph)]](ph[1]-ph[0]); }
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
    ${backBtn()}<div class="tag">● Kopf</div>
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
    ${backBtn('angstBack()')}<div class="tag" style="color:var(--gold)">▲ Achtung</div>
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
    ${backBtn()}<div class="tag">● Setup</div>
    <div class="q">Alles erfüllt?</div>
    <div class="seg" id="dir"><button data-v="long" class="lg ${st.dir==='long'?'on':''}">▲ Long</button><button data-v="short" class="sh ${st.dir==='short'?'on':''}">▼ Short</button></div>
    <div class="f"><label>Strategie</label>${chipRow('strat', strats(), st.strat)}</div>
    ${nearUSOpen()?`<p class="note">⚠ US-Börsenstart (${usOpenLocal()} Uhr) in Reichweite: oft ein kurzer Liquidity Grab.</p>`:''}
    <div class="checks" id="cks">${ckHtml(st.strat)}</div>
    ${st.setupOk ? '<p class="hint small">Du hast das Setup schon bestätigt – Haken sind gesetzt.</p>' : ''}
    <div class="row"><button class="btn ghost" onclick="stopView('setup')">Etwas fehlt</button><button class="btn pri" id="cgo" disabled onclick="st.setupOk=true;next()">Setup bestätigt</button></div>
  </div>`);
  later(()=>{
    $$('#dir button').forEach(b => b.onclick = () => { if (b.dataset.v !== biasToday().b) stopView('gegenbias'); });
    bindChips('strat', v => { st.strat = v; st.setupOk = false; S.set('lastStrat', v); $('#cks').innerHTML = ckHtml(v); ckUpd(); });
    if (st.setupOk) $$('.ck').forEach(c => c.classList.add('on'));
    else if (st.ticks && st.ticksFor === st.strat) $$('.ck').forEach((c, i) => c.classList.toggle('on', st.ticks.includes(i)));
    ckUpd();
  });
}
function ckUpd(){ const g = $('#cgo'); if (!g) return; g.disabled = $$('.ck.on').length < $$('.ck').length;
  st.ticks = $$('.ck').map((c, i) => c.classList.contains('on') ? i : -1).filter(i => i >= 0); st.ticksFor = st.strat; }

/* ------------------------------------------------------------------
   4 · Plan
------------------------------------------------------------------ */
function plan(){
  const d = st.dir, coins = recentCoins();
  if (!st.coin && coins.length) st.coin = coins[0];
  render(`<div class="card">
    ${backBtn()}<div class="tag">● Plan</div>
    <div class="q">${d==='long'?'<span style="color:var(--green)">Long</span>':'<span style="color:var(--red)">Short</span>'} – Einstieg, Stop, Ziel</div>
    <div class="f"><label for="coin">Coin</label>
      <div class="coinrow"><input id="coin" value="${esc(st.coin||'')}" placeholder="z. B. XRP" autocomplete="off" autocapitalize="characters">
      ${coins.length ? chipRow('coins', coins, st.coin, 'small') : ''}</div></div>
    <div class="grid3">
      <div class="f"><label for="entry">Einstieg</label><input id="entry" inputmode="decimal" autocomplete="off" value="${isFinite(st.entry)&&st.entry>0?decStr(st.entry):''}"></div>
      <div class="f"><label for="sl">Stop-Loss</label><input id="sl" inputmode="decimal" autocomplete="off" value="${isFinite(st.sl)&&st.sl>0?decStr(st.sl):''}"></div>
      <div class="f"><label for="tp">Ziel</label><input id="tp" inputmode="decimal" autocomplete="off" value="${isFinite(st.tp)&&st.tp>0?decStr(st.tp):''}"></div>
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
    <div class="kpi ${show?(crv>=cfg.minCRV?'good':'bad'):''}"><div class="v">${show?fmt(crv):'–'}</div><div class="l" data-term="CRV">CRV</div></div>
    <div class="kpi"><div class="v">${isFinite(riskAmt)?fmt(riskAmt):'–'}</div><div class="l">Risiko USDT</div></div>
    <div class="kpi"><div class="v">${show?fmt(vol,0):'–'}</div><div class="l">Position USDT</div></div>
    <div class="kpi ${show&&fail==='liq'?'bad':''}"><div class="v">${show?fmt(liq,4):'–'}</div><div class="l" data-term="Liq">Liquidation</div></div>`;
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
    ${backBtn()}<div class="tag">● Los</div>
    <div class="f"><label>Wie gut ist das Setup? Jetzt bewerten, nicht erst nach dem Ergebnis.</label>
      <div class="grades" id="grade">${GRADES.map(g=>`<button data-v="${g[0]}"><b>${g[0]}</b><span>${esc(g[1])}</span></button>`).join('')}</div></div>
    <div class="ifthen">
      ${S.get('weekGoal', null) ? `<div class="wg">${goalLine()}<small>Dein Wochenziel</small></div>` : ''}
      <div><b>Wenn</b> der Kurs gegen mich läuft, <b>dann</b> bleibt der Stop, wo er ist.</div>
      <div><b>Wenn</b> ich früher raus oder nachlegen will, <b>dann</b> tippe ich den Impuls an, statt zu handeln.</div>
      <div><b>Wenn</b> der Trade vorbei ist, <b>dann</b> mache ich ${cfg.cooldownLoss} Minuten Pause.</div>
    </div>
    <p class="hint" style="margin-top:14px">${esc(st.coin||'')} ${st.dir==='long'?'LONG':'SHORT'} · ${esc(st.strat)} · Einstieg ${fmt(st.entry,4)} · Stop ${fmt(st.sl,4)} · Ziel ${fmt(st.tp,4)} · ${fmt(st.crv)}R</p>
    <div class="row"><button class="btn green" id="go" disabled onclick="enterTrade()">Stop & Ziel gesetzt – ich bin drin</button></div>
    <div class="row" style="margin-top:8px"><button class="btn ghost" style="flex:1" onclick="home()">Doch nicht eingestiegen</button></div>
  </div>`);
  later(()=> { $$('#grade button').forEach(b => { if (b.dataset.v === st.grade){ b.classList.add('on'); $('#go').disabled = false; }
    b.onclick = () => { st.grade = b.dataset.v; $$('#grade button').forEach(x => x.classList.toggle('on', x===b)); $('#go').disabled = false; }; }); });
}
function enterTrade(){
  curView = 'intrade';
  progress(4); $$('.phase').forEach(p=>p.classList.add('done'));
  const id = 'o' + Date.now().toString(36);
  setOpen([...openTrades(), {id, dir:st.dir, coin:st.coin||'', strat:st.strat, grade:st.grade, entry:st.entry, sl:st.sl, tp:st.tp, crv:st.crv,
    riskAmt:st.riskAmt, risk:st.risk, feel:st.feel||'', start:Date.now()}]);
  setTimeout(() => inTrade(id), 500);
}

/* ------------------------------------------------------------------
   Im Trade
------------------------------------------------------------------ */
function inTrade(id){
  stopTimers(); curView = 'intrade'; progress(null); closeModal();
  const a = getOpen(id || curOpen); if (!a) return home();
  curOpen = a.id; const others = openTrades().filter(x => x.id !== a.id);
  render(`<div class="card">
    ${backBtn('home()')}
    ${others.length ? `<div class="chips small opensw">${openTrades().map(x => `<button class="${x.id===a.id?'on':''}" onclick="inTrade('${x.id}')">${esc(x.coin||'Trade')} ${x.dir==='long'?'L':'S'}</button>`).join('')}</div>` : ''}
    <div class="live"><span class="livedot"></span><span class="tag" style="margin:0;color:var(--green)">Im Trade · ${esc(a.coin||'')} ${a.dir==='long'?'LONG':'SHORT'} · ${esc(a.strat||'')}</span></div>
    <div class="clock" id="clock">00:00:00</div>
    <div class="plan">
      <div class="kpi"><div class="v">${fmt(a.entry,4)}</div><div class="l">Einstieg</div></div>
      <div class="kpi bad"><div class="v">${fmt(a.sl,4)}</div><div class="l">Stop Loss</div></div>
      <div class="kpi good"><div class="v">${fmt(a.tp,4)}</div><div class="l">Take Profit</div></div>
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
    <div class="ends">${endBtns(a.id)}</div>
    <div class="row" style="margin-top:14px"><button class="btn ghost" style="flex:1" onclick="start()">+ Weiteren Trade über die Checkliste</button></div>
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
  setOpen(openTrades().map(x => x.id === curOpen ? Object.assign({}, x, {impulse: (x.impulse||0) + 1}) : x));
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
/* Nach dem Trade: Einstieg, Stop und Ziel sind bekannt – nur noch beantworten, wie er ausging.
   Ergebnis rechnet die App (R und USDT); „Plan eingehalten?“ wird bewusst gefragt, nicht vorausgefüllt. */
function rFromExit(a, exit){ const d = a.dir === 'long' ? 1 : -1, risk = Math.abs(a.entry - a.sl); return risk > 0 && isFinite(exit) ? d * (exit - a.entry) / risk + 0 : NaN; }
function endTrade(art, id){
  stopTimers(); curView = 'end'; closeModal();
  const a = getOpen(id || curOpen); if (!a) return home();
  curOpen = a.id;
  st.end = {id: a.id, art, R: art==='tp' ? a.crv : art==='sl' ? -1 : NaN, exit: art==='tp' ? a.tp : art==='sl' ? a.sl : null, kept: null};
  const title = {tp:'Profit – Take Profit erreicht', sl:'Loss – Stop Loss getroffen. Ein Datenpunkt.', manuell:'Manuell geschlossen'}[art];
  render(`<div class="card">
    ${backBtn(`inTrade('${a.id}')`)}<div class="tag">● Nach dem Trade</div>
    <div class="q">${title}</div>
    <p class="hint small">${esc(a.coin||'')} ${a.dir==='long'?'LONG':'SHORT'} · Einstieg ${fmt(a.entry,4)} · SL ${fmt(a.sl,4)} · TP ${fmt(a.tp,4)}</p>
    <div class="resbox" id="resbox"></div>
    <div class="f" id="exitf" ${art==='manuell'?'':'hidden'}><label for="exit">Ausstiegskurs</label>
      <div class="coinrow"><input id="exit" inputmode="decimal" autocomplete="off" placeholder="z. B. ${fmt(a.entry,4)}"><button type="button" class="mini" id="be">Break-even</button></div></div>
    ${art!=='manuell' ? '<button type="button" class="linkbtn" id="adj">Anderer Ausstiegskurs? Anpassen</button>' : ''}
    <div class="lbl3">Hast du dich an deinen Plan gehalten?</div>
    <div class="row keptrow"><button class="btn green" id="k_yes">✓ Ja, alles nach Plan</button><button class="btn ghost" id="k_no">✗ Nein</button></div>
    <div id="broke" hidden>
      <div class="f"><label>Welche Regel hast du gebrochen? Antippen.</label>${rulesRow('rules', {})}</div>
      <div class="f"><label>Was war der Fehler? (optional, ehrlich hilft dir am meisten)</label>${mistakeRow('mist', [])}</div>
    </div>
    <details class="more"><summary>Notiz, Screenshot, Chart-Link (optional)</summary>
      <div class="f"><label for="note">1 % besser beim nächsten Mal (optional)</label><input id="note" placeholder="z. B. auf Kerzenschluss warten" autocomplete="off"></div>
      ${shotInput('shotf')}
      <div class="f"><label for="link">Chart-Link (optional, z. B. TradingView)</label><input id="link" type="url" placeholder="https://www.tradingview.com/x/…" autocomplete="off"></div>
    </details>
    <div class="row" id="saverow" hidden><button class="btn pri" onclick="saveTrade('${art}')">Speichern</button></div>
  </div>`);
  later(()=>{
    bindChips('rules', null, true); bindChips('mist', null, true);
    const upd = () => { const R = st.end.R, box = $('#resbox'); if (!box) return;
      box.className = 'resbox ' + (isFinite(R) ? (R > 0 ? 'good' : R < 0 ? 'bad' : '') : '');
      box.innerHTML = isFinite(R) ? `<b>${sgn(R)} R</b><span>${isFinite(a.riskAmt) ? '≈ ' + sgn(R * a.riskAmt) + ' USDT' : ''}</span>` : '<span>Ausstiegskurs eingeben – die App rechnet das Ergebnis aus.</span>'; };
    const ex = $('#exit');
    ex.oninput = () => { ex.style.borderColor = ''; const v = num(ex.value); st.end.exit = isFinite(v) ? v : null; st.end.R = rFromExit(a, v); upd(); };
    $('#be').onclick = () => { ex.value = decStr(a.entry); ex.oninput(); };
    if ($('#adj')) $('#adj').onclick = () => { $('#exitf').hidden = false; $('#adj').hidden = true; ex.value = decStr(st.end.exit); ex.focus(); };
    $('#k_yes').onclick = () => {
      if (!isFinite(st.end.R)){ $('#exitf').hidden = false; ex.focus(); ex.style.borderColor = 'var(--red)'; return; }
      st.end.kept = true; $$('#rules button').forEach(b => b.classList.add('on')); $$('#mist button').forEach(b => b.classList.remove('on')); saveTrade(art); };
    $('#k_no').onclick = () => { st.end.kept = false; $('#broke').hidden = false; $('#saverow').hidden = false;
      $('#k_no').classList.add('on'); $('#k_yes').classList.remove('green'); $('#k_yes').classList.add('ghost'); };
    upd(); if (art === 'manuell') ex.focus();
  });
}
function cleanLink(v){ v = String(v||'').trim(); return /^https:\/\/[^\s]+$/.test(v) ? v.slice(0,300) : ''; }
function saveTrade(art){
  const a = getOpen(st.end && st.end.id); if (!a) return home();
  const R = st.end && isFinite(st.end.R) ? +st.end.R.toFixed(2) : NaN;
  if (!isFinite(R)){ const ex = $('#exit'); if (ex){ $('#exitf').hidden = false; ex.focus(); ex.style.borderColor='var(--red)'; } return; }
  const rules = readRules('rules'), files = $('#shotf') ? [...$('#shotf').files] : [];
  const said = st.end ? st.end.kept : null, exitPx = st.end && isFinite(st.end.exit) ? st.end.exit : null;
  const t = {date:new Date().toISOString(), coin:a.coin||'', strat:a.strat||'', grade:a.grade||'', dir:a.dir, entry:a.entry, sl:a.sl, tp:a.tp, R,
    art, exit: exitPx, rules, plan: said === false ? false : Object.values(rules).every(Boolean), err: readMistakes('mist'), feel:a.feel||'', impulse:a.impulse||0, risk:a.risk||null, riskUsd: isFinite(a.riskAmt) ? +(+a.riskAmt).toFixed(2) : null,
    note:$('#note').value.trim().slice(0,500), link: cleanLink($('#link').value), checklist:true};
  addTrade(t); attachShots(t.id, files);
  setOpen(openTrades().filter(x => x.id !== a.id)); curOpen = null;
  const mins = R<0 ? cfg.cooldownLoss : cfg.cooldownWin;
  if (mins>0) S.set('cooldownUntil', Date.now()+mins*60000);
  const kept = said !== false && Object.values(rules).every(Boolean) && !readMistakes('mist').length; st.end = null;
  if (kept){ confetti(); Snd.kept(); }
  cooldownView(R, kept);
}
function cooldownView(R, kept){
  stopTimers(); curView = 'cool'; progress(null);
  const loss = R!==undefined ? R<0 : null;
  render(`<div class="card" style="text-align:center">
    <div class="tag" style="color:var(--blue)">◎ Pause</div>
    <div class="q">${loss===true?'Handy weg. Kurz raus.':loss===false?'Gespeichert. Kein Übermut-Trade.':'Du bist noch in der Pause.'}</div>
    <p class="hint">${loss===true?'Nach Verlusten steigt die Risikobereitschaft messbar. Die Pause schützt dich davor.':'Auch nach Gewinnen: Der nächste Trade kommt nur über die Checkliste.'}</p>
    ${kept===true ? '<p class="keptmsg">✓ Plan eingehalten – das zählt, nicht das Ergebnis.</p>' : ''}
    <div class="timer" id="cdt" style="color:var(--blue)">${mmss(cooldownLeft())}</div>
    <div class="row"><button class="btn ghost" style="flex:1" onclick="home()">Start</button><button class="btn ghost" style="flex:1" onclick="journal()">Journal</button></div>
  </div>`);
  let rang = cooldownLeft() <= 0;
  timers.push(setInterval(()=>{ const c=$('#cdt'); if(!c) return; const l=cooldownLeft(); c.textContent = l>0?mmss(l):'Fertig'; if (l<=0 && !rang){ rang = true; Snd.gong(); } }, 1000));
}

/* ------------------------------------------------------------------
   Stopp
------------------------------------------------------------------ */
/* Zurück nur bei Stopps, die auf eine Eingabe zurückgehen. Gefühls-Stopps (FOMO, Gier, Revenge, Langeweile)
   bleiben ohne Rückweg – sonst wählt man einfach „ruhig“ und die Unterbrechung wirkt nicht mehr. */
const STOP_BACK = ['setup', 'gegenbias', 'crv', 'liq'];
function stopView(k, text){
  const canBack = curView === 'flow' && STOP_BACK.includes(k);
  if (canBack) enter(); else if (curView !== 'day') retStack = [];
  stopTimers(); curView = 'stop'; progress(null);
  const s = STOPS[k];
  Snd.block();
  if (s.cool) S.set('cooldownUntil', Math.max(S.get('cooldownUntil',0), Date.now()+cfg.cooldownLoss*60000));
  const log = S.get('stops', []).slice(); log.unshift({k, date:new Date().toISOString()}); S.set('stops', log.slice(0,150));
  render(`<div class="card">
    ${canBack ? backBtn('ret()') : ''}<svg class="xicon" viewBox="0 0 64 64" aria-hidden="true"><path d="M18 18 L46 46"/><path d="M46 18 L18 46" style="animation-delay:.35s"/></svg>
    <div class="tag">■ Kein Trade</div>
    <div class="q">${esc(s.t)}</div>
    <p class="hint">${esc(text || s.x)}</p>
    <div class="henry"><b>Merke:</b> ${esc(s.h)}</div>
    ${s.cool?`<p class="hint" style="margin-top:14px">${cfg.cooldownLoss} Minuten Pause gestartet.</p>`:''}
    <div class="row"><button class="btn pri" onclick="go('home')">Verstanden</button></div>
    ${k==='neutral'||k==='gegenbias' ? '<div class="row" style="margin-top:8px"><button class="btn ghost" style="flex:1" onclick="dayCheck(false, true)">Tages-Check neu machen</button></div>' : ''}
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
    rule: (() => { const r = arr.filter(x => typeof x.plan === 'boolean'); return r.length ? r.filter(x => x.plan).length / r.length * 100 : NaN; })()};
}
function insights(arr){
  const out = [], avg = a => a.length ? a.reduce((s,x)=>s+x.R,0)/a.length : NaN, MIN = 3;
  const kept = arr.filter(x=>x.plan===true), broke = arr.filter(x=>x.plan===false);
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
  const mc = mistakeCosts(arr).filter(m => m.cost > 0);
  if (mc.length) out.unshift(`Teuerster Fehler: <b>${esc(mc[0].l)}</b> – ${mc[0].n}× · kostet dich ca. <b class="bad">${fmt(mc[0].cost,1)} R</b>` + (mc[1] ? ` · danach <span>${esc(mc[1].l)}</span> (${fmt(mc[1].cost,1)} R)` : ''));
  return out;
}
function journal(){
  curView = 'journal'; progress(null); closeModal();
  const arr = filtered(), k = kpis(arr), stops = S.get('stops', []);
  const RANGES = [['7','7 Tage'],['30','30 Tage'],['all','Alles']];
  const openCard = openTrades().map(act => `<div class="opencard"><div><b>Offener Trade</b> · ${esc(act.coin||'')} ${act.dir==='long'?'LONG':'SHORT'} · ${sinceTxt(act)}</div>
      <p class="hint small" style="margin:4px 0 10px">Wie ist er ausgegangen?</p>
      <div class="ends">${endBtns(act.id)}</div></div>`).join('');
  const head = openCard + `<div class="tag">● Journal</div>
    <div class="tabs"><button class="${jTab==='stats'?'on':''}" onclick="jTab='stats';journal()">Übersicht</button><button class="${jTab==='list'?'on':''}" onclick="jTab='list';journal()">Trades (${arr.length})</button></div>
    ${chipRow('jr', RANGES, jRange, 'small')}${simple() && new Set(cache.journal.map(t => t.strat)).size < 2 ? '' : chipRow('js', [['alle','Alle Strategien'], ...strats()], jStrat, 'small')}`;
  let body;
  if (jTab==='stats'){
    const ins = insights(arr), sm = simple();
    const kTrades = `<div class="kpi"><div class="v">${k.n}</div><div class="l">Trades</div></div>`,
      kWin = `<div class="kpi"><div class="v">${k.n?fmt(k.win,0)+' %':'–'}</div><div class="l" data-term="Treffer">Treffer</div></div>`,
      kExp = `<div class="kpi ${k.exp>0?'good':k.exp<0?'bad':''}"><div class="v">${k.n?sgn(k.exp):'–'}</div><div class="l" data-term="Exp">Ø R / Trade</div></div>`,
      kPf = `<div class="kpi"><div class="v">${isFinite(k.pf)?fmt(k.pf):k.pf===Infinity?'∞':'–'}</div><div class="l" data-term="PF">Profit-Faktor</div></div>`,
      kNet = `<div class="kpi ${k.sum>0?'good':k.sum<0?'bad':''}"><div class="v">${k.n?sgn(k.sum,1):'–'}</div><div class="l" data-term="NetR">Netto R</div></div>`,
      kRule = `<div class="kpi"><div class="v">${k.n?fmt(k.rule,0)+' %':'–'}</div><div class="l" data-term="Regeltreue">Regeltreue</div></div>`;
    const deep = `<div class="jdeep">${edgeHtml(arr)}
      <div class="ins"><div class="lbl2">Erkenntnisse</div>${ins.length ? ins.map(i=>`<div>${i}</div>`).join('') : '<div class="mutedtxt">Ab ein paar Trades pro Gruppe zeigt dir die App hier, was bei dir funktioniert – z. B. Plan gehalten vs. gebrochen, S- vs. B-Setups, ruhig vs. nervös.</div>'}</div></div>`;
    body = (sm ? `<div class="jstats three">${kTrades}${kRule}${kNet}</div>` : `<div class="jstats">${kTrades}${kWin}${kExp}${kPf}${kNet}${kRule}</div>`) + `
      <div class="eqwrap"><canvas id="eq" height="150" aria-label="Kapitalkurve in R"></canvas></div>
      ${sm ? `<details class="more"><summary>Mehr Kennzahlen und Auswertungen</summary><div class="jstats">${kWin}${kExp}${kPf}</div>${deep}</details>` : deep}
      <p class="hint small">Von der Checkliste verhinderte Trades: <b>${stops.length}</b></p>
      <button class="reviewcta small" onclick="weeklyReview()"><b>Wochenrückblick</b><span>Woche auswerten und ein Ziel festlegen</span></button>`;
  } else {
    body = `<div class="jlist">${arr.length ? arr.map(x=>`<button class="jitem" onclick="editTrade('${x.id}')">
      <span class="d" style="color:${x.dir==='long'?'var(--green)':'var(--red)'}">${x.dir==='long'?'▲':'▼'}</span>
      <div class="m"><div><b>${esc(x.coin||'–')}</b> · ${esc(x.strat||'–')}${x.grade?` · ${esc(x.grade)}`:''}${x.art==='import'?' · <span class="mutedtxt">importiert</span>':x.checklist?'':' · <span class="warnc">ohne Checkliste</span>'}${x.plan===false?' · <span class="warnc">Regel gebrochen</span>':''}${x.err&&x.err.length?' · <span class="warnc">'+x.err.map(e=>esc(MIST_TXT[e]||e)).join(', ')+'</span>':''}${x.shots&&x.shots.length?' · ▣':''}</div>
      <div class="n">${new Date(x.date).toLocaleString(LOC(),{dateStyle:'short',timeStyle:'short'})}${x.note?' · '+esc(x.note):''}</div></div>
      <span class="r" style="color:${x.R>0?'var(--green)':x.R<0?'var(--red)':'var(--mut)'}">${sgn(x.R)} R</span></button>`).join('')
      : '<div class="empty">Keine Trades in diesem Zeitraum. Trades aus der Checkliste landen hier automatisch – andere kannst du nachtragen.</div>'}</div>`;
  }
  render(`<div class="card">${head}${body}
    <div class="row jrow"><button class="btn ghost" onclick="addManual()">+ Trade nachtragen</button><button class="btn ghost" onclick="importOpen()">⇩ Von Börse importieren</button><button class="btn ghost" onclick="exportCSV()">CSV exportieren</button></div>
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
    <div class="f"><label>Fehler</label>${mistakeRow('e_mist', t.err||[])}</div>
    ${shotGrid(t)}${shotInput('e_shot')}
    <div class="f"><label for="e_note">Notiz</label><input id="e_note" value="${esc(t.note||'')}" autocomplete="off"></div>
    <div class="f"><label for="e_link">Chart-Link</label><input id="e_link" type="url" value="${esc(t.link||'')}" placeholder="https://…" autocomplete="off"></div>
    ${t.link?`<p class="hint small"><a href="${esc(t.link)}" target="_blank" rel="noopener">Chart öffnen ↗</a></p>`:''}
    <div class="row"><button class="btn ghost" onclick="closeModal()">Abbrechen</button><button class="btn pri" onclick="${isNew?'saveManual()':`saveEdit('${t.id}')`}">Speichern</button></div>
    ${isNew?'':`<div class="row" style="margin-top:8px"><button class="btn ghost danger" style="flex:1" onclick="delTrade('${t.id}', this)">Trade löschen</button></div>`}`;
}
function bindForm(){ bindChips('e_dir'); bindChips('e_strat'); bindChips('e_rules', null, true); bindChips('e_mist', null, true); loadShotThumbs(); }
const chipVal = id => $(`#${id} button.on`)?.dataset.v;
function editTrade(id){
  const t = cache.journal.find(j => j.id===id); if (!t) return;
  modal(tradeForm(t, false)); bindForm();
}
function saveEdit(id){
  const t = cache.journal.find(j => j.id===id); if (!t) return;
  const R = num($('#e_r').value); if (!isFinite(R)){ $('#e_r').style.borderColor='var(--red)'; return; }
  const rules = readRules('e_rules');
  const files = $('#e_shot') ? [...$('#e_shot').files] : [];
  updateTrade(Object.assign({}, t, {coin: coinFmt($('#e_coin').value), R, strat: chipVal('e_strat')||t.strat, rules, plan: Object.values(rules).every(Boolean),
    err: readMistakes('e_mist'), note: $('#e_note').value.trim().slice(0,500), link: cleanLink($('#e_link').value)}));
  attachShots(id, files);
  closeModal(); journal();
}
let delArmed = 0;
function delTrade(id, btn){
  if (Date.now()-delArmed > 4000){ delArmed = Date.now(); btn.textContent = 'Nochmal tippen zum Löschen'; return; }
  const t = cache.journal.find(j => j.id===id); removeShots(t && t.shots);
  removeTrade(id); closeModal(); journal();
}
function addManual(){ modal(tradeForm({}, true)); bindForm(); }
function saveManual(){
  const R = num($('#e_r').value); if (!isFinite(R)){ $('#e_r').style.borderColor='var(--red)'; return; }
  const d = new Date($('#e_date').value); const rules = readRules('e_rules');
  const nt = {date: (isNaN(d) ? new Date() : d).toISOString(), coin: coinFmt($('#e_coin').value), dir: chipVal('e_dir')||'long', strat: chipVal('e_strat')||'',
    R, rules, plan: Object.values(rules).every(Boolean), err: readMistakes('e_mist'), note: $('#e_note').value.trim().slice(0,500), link: cleanLink($('#e_link').value), checklist:false, art:'nachgetragen', riskUsd: +(cfg.konto*cfg.risk/100).toFixed(2)};
  addTrade(nt); attachShots(nt.id, $('#e_shot') ? [...$('#e_shot').files] : []);
  closeModal(); rerender();
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
    ${backBtn('settingsBack()')}<div class="tag">⚙︎ Einstellungen</div>
    <div class="q">Deine Regeln</div>
    <p class="hint">Einmal in Ruhe festlegen, nicht mitten im Trade.</p>
    ${stratListHtml(true)}
    ${reminderHtml()}
    <div class="lbl3">Deine Grenzen</div>
    ${(() => { const fld = f => `<div class="f"><label for="${f[0]}">${f[1]}</label><input id="${f[0]}" inputmode="decimal" value="${f[2]}"></div>`, basic = ['s_konto','s_risk','s_max','s_loss'];
      return `<div class="grid2">${F.filter(f => basic.includes(f[0])).map(fld).join('')}</div>
      <details class="more"><summary>Weitere Grenzen (Chance-Risiko, Hebel, Pausen, News)</summary><div class="grid2">${F.filter(f => !basic.includes(f[0])).map(fld).join('')}</div></details>`; })()}
    <div class="f tog"><label>Ansicht</label>${chipRow('s_mode', [['simple','Einfach'],['pro','Alle Details']], simple() ? 'simple' : 'pro', 'small')}
      <p class="hint small" style="margin:6px 0 0">Einfach: nur das Wichtigste, Details auf Wunsch. Alle Details: jede Kennzahl sofort sichtbar.</p></div>
    <div class="f tog"><label>Sprache / Language</label><button type="button" class="langbtn" onclick="langPicker()"><span translate="no">${langName()}</span><em>›</em></button></div>
    <details class="more"><summary>Design & Töne</summary>
    <div class="f tog"><label>Design</label>${chipRow('s_theme', [['cyan','Cyan (Standard)'],['gold','Gold']], document.documentElement.dataset.theme === 'gold' ? 'gold' : 'cyan', 'small')}</div>
    <div class="f tog"><label>Töne (Atmen, Pause vorbei, Stopp, Plan eingehalten)</label><div class="sndrow">${chipRow('s_sound', [['an','An'],['aus','Aus']], cfg.sound===false ? 'aus' : 'an', 'small')}<button class="mini" type="button" onclick="cfg.sound=true;Snd.demo()">▶ Ton testen</button></div>
      <p class="hint small" style="margin:6px 0 0">Am iPhone: Lautlos-Schalter aus und Lautstärke hoch.</p></div>
    </details>
    <div class="row"><button class="btn pri" id="s_save" onclick="saveSettings()">Speichern</button></div>
    <div class="acct">${accountBlock()}</div>
    <details class="more"><summary>Daten: Backup & Löschen</summary>
    <div class="row" style="margin-top:8px"><button class="btn ghost" style="flex:1" onclick="exportBackup()">Backup speichern</button><button class="btn ghost" style="flex:1" onclick="$('#imp').click()">Backup laden</button></div>
    <input type="file" id="imp" accept=".json,application/json" hidden onchange="importBackup(this)">
    <p class="msg" id="impmsg"></p>
    <div class="row" style="margin-top:8px"><button class="btn ghost danger" style="flex:1" onclick="if(confirmReset(this))home()">Journal & Daten löschen</button></div>
    </details>
    <p class="disc">NoTilt ist ein Werkzeug für Disziplin und Selbstbeobachtung – keine Anlageberatung. Gehebelter Handel mit Kryptowährungen kann zum Totalverlust des eingesetzten Kapitals führen.<br><a href="impressum.html">Impressum</a> · <a href="datenschutz.html">Datenschutz</a></p>
  </div>`);
  later(() => {
    bindChips('s_theme', v => { LS.set('theme', v); location.href = location.pathname + '?theme=' + v; });
    bindChips('s_sound', v => { cfg.sound = v==='an'; saveCfg(); if (cfg.sound) Snd.kept(); });
    bindChips('s_mode', v => { cfg.mode = v; saveCfg(); }); bindReminder(); });
}
function saveSettings(quiet){
  if (!$('#s_max')) return;
  const g = id => num($('#'+id).value);
  const pos = (v, d) => isFinite(v) && v>0 ? v : d, nn = (v, d) => isFinite(v) && v>=0 ? Math.round(v) : d;
  Object.assign(cfg, {maxTrades: Math.round(pos(g('s_max'),2)), lossLimit: pos(g('s_loss'),2), minCRV: pos(g('s_crv'),2), risk: pos(g('s_risk'),1),
    hebel: pos(g('s_lev'),5), konto: pos(g('s_konto'),1000), cooldownLoss: nn(g('s_cl'),15), cooldownWin: nn(g('s_cw'),5),
    newsWin: nn(g('s_nw'),30), usOpenBlock: nn(g('s_us'),10)});
  saveCfg(); if (quiet) return;
  const b = $('#s_save'); if (b){ b.textContent = '✓ Gespeichert'; b.classList.add('saved'); setTimeout(() => { if (b.isConnected){ b.textContent = 'Speichern'; b.classList.remove('saved'); } }, 1600); }
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
const THEME_RGB = document.documentElement.dataset.theme === 'gold' ? '245,197,66' : '0,229,255';
(function bgfx(){
  const c = $('#bgfx'), x = c.getContext('2d'); let w, h, P;
  const size = () => { w = c.width = innerWidth*devicePixelRatio; h = c.height = innerHeight*devicePixelRatio;
    P = Array.from({length: Math.min(50, Math.floor(innerWidth/22))}, () => ({x:Math.random()*w, y:Math.random()*h, v:(.15+Math.random()*.35)*devicePixelRatio, r:(Math.random()*1.6+.4)*devicePixelRatio, a:Math.random()*.5+.1})); };
  size(); addEventListener('resize', size);
  if (matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  (function loop(){ x.clearRect(0,0,w,h);
    for (const p of P){ p.y -= p.v; if (p.y < -5){ p.y = h+5; p.x = Math.random()*w; }
      x.beginPath(); x.arc(p.x,p.y,p.r,0,7); x.fillStyle = `rgba(${THEME_RGB},${p.a*.5})`; x.fill(); }
    requestAnimationFrame(loop); })();
})();
function confetti(){
  if (matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  const c = $('#confetti'), x = c.getContext('2d'); c.width = innerWidth; c.height = innerHeight;
  const cols = document.documentElement.dataset.theme === 'gold' ? ['#f5c542','#34d399','#60a5fa','#ffffff','#ffdf7a'] : ['#00e5ff','#20c58a','#4f8cff','#ffffff','#8af4ff'];
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
   Töne: nur zur Führung (Atmen, Pause vorbei, Stopp, Plan eingehalten)
   – nie für Gewinne. Erzeugt im Browser, keine Audiodateien.
------------------------------------------------------------------ */
const Snd = {
  ctx: null,
  on(){ return cfg.sound !== false; },
  unlock(){
    if (this.ctx || !(window.AudioContext || window.webkitAudioContext)) return;
    try {
      if (navigator.audioSession) navigator.audioSession.type = 'ambient';   /* iPhone: respektiert den Lautlos-Schalter */
      this.ctx = new (window.AudioContext || window.webkitAudioContext)();
    } catch(e){ this.ctx = null; }
  },
  ready(){ if (!this.on()) return null; this.unlock(); const c = this.ctx; if (!c) return null; if (c.state === 'suspended') c.resume(); return c; },
  /* weicher Ton mit Ein- und Ausblenden, optional gleitende Tonhöhe */
  tone({f=440, to=null, dur=1, type='sine', vol=.15, at=0, attack=.08, release=.3, over=true}){
    const c = this.ready(); if (!c) return;
    const t = c.currentTime + at, o = c.createOscillator(), g = c.createGain(), lp = c.createBiquadFilter();
    lp.type = 'lowpass'; lp.frequency.value = 1800;
    o.type = type; o.frequency.setValueAtTime(f, t);
    if (to) o.frequency.exponentialRampToValueAtTime(to, t + dur);
    g.gain.setValueAtTime(0.0001, t);
    g.gain.exponentialRampToValueAtTime(vol, t + attack);
    g.gain.setValueAtTime(vol, t + Math.max(attack, dur - release));
    g.gain.exponentialRampToValueAtTime(0.0001, t + dur);
    o.connect(lp); lp.connect(g); g.connect(c.destination);
    o.start(t); o.stop(t + dur + .05);
    if (over){   /* leiser Oberton eine Oktave höher: klingt auch auf kleinen Lautsprechern */
      const o2 = c.createOscillator(), g2 = c.createGain();
      o2.type = 'sine'; o2.frequency.setValueAtTime(f*2, t); if (to) o2.frequency.exponentialRampToValueAtTime(to*2, t + dur);
      g2.gain.value = .35; o2.connect(g2); g2.connect(lp); o2.start(t); o2.stop(t + dur + .05);
    }
  },
  /* Atemgeräusch: gefiltertes Rauschen, das an- und abschwillt – wie echtes Ein- und Ausatmen */
  noise(c){
    if (this._nb) return this._nb;
    const len = c.sampleRate * 2, b = c.createBuffer(1, len, c.sampleRate), d = b.getChannelData(0);
    let b0 = 0, b1 = 0, b2 = 0;                           /* rosa Rauschen: weicher als weißes */
    for (let i = 0; i < len; i++){ const w = Math.random()*2-1;
      b0 = .99765*b0 + w*.0990460; b1 = .96300*b1 + w*.2965164; b2 = .57000*b2 + w*1.0526913;
      d[i] = (b0 + b1 + b2 + w*.1848) * .2; }
    return this._nb = b;
  },
  /* Raum: Hall mit kurzer Vorverzögerung und dunklem Nachklang – lässt das Atmen weiter weg klingen */
  room(c){
    if (this._rv && this._rv.context === c) return this._rv;
    const len = Math.floor(c.sampleRate * 1.6), ir = c.createBuffer(2, len, c.sampleRate);
    for (let ch = 0; ch < 2; ch++){ const d = ir.getChannelData(ch); let lp = 0;
      for (let i = 0; i < len; i++){ const k = .25 + .6 * (i/len);            /* wird nach hinten dunkler */
        lp += k * ((Math.random()*2-1) - lp); d[i] = lp * Math.pow(1 - i/len, 2.2); } }
    const pre = c.createDelay(.1); pre.delayTime.value = .028;
    const cv = c.createConvolver(); cv.buffer = ir;
    const wet = c.createGain(); wet.gain.value = 1.1;
    pre.connect(cv); cv.connect(wet); wet.connect(c.destination);
    return this._rv = pre;
  },
  /* sanfte Hüllkurve ohne harte Kanten: steigt weich an, klingt weich aus */
  env(peakAt){
    const n = 128, a = new Float32Array(n);
    for (let i = 0; i < n; i++){ const x = i/(n-1);
      a[i] = x < peakAt ? Math.pow(Math.sin(Math.PI/2 * x/peakAt), 2) : Math.pow(Math.cos(Math.PI/2 * (x-peakAt)/(1-peakAt)), 2); }
    a[0] = 0; a[n-1] = 0; return a;
  },
  breath({dur, lp0, lp1, hp=500, vol=.35, peakAt=.5}){
    const c = this.ready(); if (!c) return;
    const t = c.currentTime + .02, src = c.createBufferSource(), hi = c.createBiquadFilter(), lo = c.createBiquadFilter(), lo2 = c.createBiquadFilter(), g = c.createGain();
    src.buffer = this.noise(c); src.loop = true;
    hi.type = 'highpass'; hi.frequency.value = hp; hi.Q.value = .5;                 /* kein Brummen */
    lo.type = 'lowpass'; lo.Q.value = .4; lo2.type = 'lowpass'; lo2.Q.value = .4;     /* flach, ohne Resonanz – kein „Schnupfen“ */
    lo.frequency.setValueAtTime(lp0, t); lo.frequency.linearRampToValueAtTime(lp1, t + dur);
    lo2.frequency.setValueAtTime(lp0*1.4, t); lo2.frequency.linearRampToValueAtTime(lp1*1.4, t + dur);
    const curve = this.env(peakAt).map(v => v * vol);
    g.gain.setValueAtTime(0, t); g.gain.setValueCurveAtTime(curve, t, dur);
    src.connect(hi); hi.connect(lo); lo.connect(lo2); lo2.connect(g);
    const dry = c.createGain(); dry.gain.value = .32;          /* wenig Direktschall, viel Raum = Abstand */
    g.connect(dry); dry.connect(c.destination); g.connect(this.room(c));
    src.start(t, Math.random()); src.stop(t + dur + .05);
  },
  /* echte Atem-Aufnahme (CC0, Freesound #336532 „Breathing, calm, mouth exhale“ von giddster) – bearbeitet: entrauscht, gestreckt, Raum */
  files: {inhale:'sounds/atem_ein.mp3', topup:'sounds/atem_nach.mp3', exhale:'sounds/atem_aus.mp3'},
  bufs: {}, loading: false,
  load(){
    const c = this.ctx; if (!c || this.loading) return; this.loading = true;
    Object.entries(this.files).forEach(([k, u]) => fetch(u).then(r => r.arrayBuffer()).then(ab => c.decodeAudioData(ab))
      .then(b => { this.bufs[k] = b; }).catch(() => {}));
  },
  sample(k, vol=1){
    const c = this.ready(); if (!c || !this.bufs[k]) return false;
    const src = c.createBufferSource(), g = c.createGain(); src.buffer = this.bufs[k]; g.gain.value = vol;
    src.connect(g); g.connect(c.destination); src.start(c.currentTime + .01); return true;
  },
  inhale(ms){ if (this.sample('inhale', .95)) return; this.breath({dur:ms/1000, lp0:800, lp1:1900, hp:350, vol:.7, peakAt:.7}); },      /* ruhig durch die Nase ein */
  topup(ms){ if (this.sample('topup', .9)) return; this.breath({dur:ms/1000*.85, lp0:1400, lp1:2000, hp:450, vol:.5, peakAt:.45}); },  /* kurz nachziehen */
  exhale(ms){ if (this.sample('exhale', .95)) return; this.breath({dur:ms/1000, lp0:1100, lp1:450, hp:180, vol:.6, peakAt:.18}); },      /* lang und locker durch den Mund aus */
  /* Pause vorbei: sanfter Gong aus wenigen Obertönen */
  gong(){
    [[523,.16,3.2],[1046,.06,2.4],[1569,.03,1.6],[784,.05,2.8]].forEach(([f,v,d]) =>
      this.tone({f, dur:d, vol:v, attack:.01, release:d*.9, over:false}));
  },
  /* Stopp: ruhig und tief, kein Alarm */
  block(){ this.tone({f:349, to:294, dur:.7, type:'triangle', vol:.15, attack:.02, release:.5}); },
  /* Plan eingehalten: warmer, kurzer Zweiklang */
  kept(){ this.tone({f:659, dur:.5, vol:.13, attack:.02, release:.4}); this.tone({f:784, dur:.8, vol:.12, at:.13, attack:.02, release:.6}); },
  demo(){ this.kept(); setTimeout(() => this.gong(), 1100); }
};
/* iPhone/Safari geben Audio erst nach einem echten Tippen frei: bei jedem Tippen prüfen und ggf. freischalten */
function sndWake(){
  Snd.unlock(); const c = Snd.ctx; if (!c) return;
  Snd.load();
  if (c.state !== 'running'){
    c.resume();
    try { const b = c.createBuffer(1, 1, 22050), src = c.createBufferSource(); src.buffer = b; src.connect(c.destination); src.start(0); } catch(e){}
  }
}
['click','touchend','keydown'].forEach(ev => addEventListener(ev, sndWake, {passive:true, capture:true}));
/* ------------------------------------------------------------------
   Strategien: Vorlagen, eigene Checklisten, Einrichtung
------------------------------------------------------------------ */
const TEMPLATES = [
  {en:{"name": "Fair Value Gap", "desc": "30m gap, entry in the middle, fixed R:R", "checks": ["30-minute chart: the indicator shows a new fair value gap", "The gap is at least 0.5% in size", "The move after the gap has at least R:R 2 (target at the last high or low before the pullback)", "Not more than 20% mitigated and closed outside", "Not already closed inside the gap and then back outside", "No other valid gap directly below or above", "The gap has not been fully mitigated yet", "Entry in the middle of the gap (limit or trigger order)", "No news and no market open right now – otherwise be extra careful"], "stop": "Above or below the last significant wick (beyond the gap)", "target": "Fixed R:R 2–3 (e.g. XRP: 3)"}, name:'Fair Value Gap', icon:'▤', desc:'30-Minuten-Gap, Einstieg in der Mitte, festes CRV',
   checks:['30-Minuten-Chart: Der Indikator zeigt ein neues Fair Value Gap', 'Das Gap ist mindestens 0,5 % groß', 'Der Move nach dem Gap hat mindestens CRV 2 (Ziel am letzten Hoch bzw. Tief vor dem Rücklauf)', 'Nicht mehr als 20 % mitigiert und außerhalb geschlossen', 'Nicht schon im Gap und danach wieder außerhalb geschlossen', 'Kein weiteres valides Gap direkt darunter bzw. darüber', 'Das Gap wurde noch nicht vollständig mitigiert', 'Einstieg in der Mitte des Gaps (Limit- oder Trigger-Order)', 'Keine News und kein Börsenstart gerade – sonst besonders vorsichtig'],
   stop:'Über bzw. unter dem letzten markanten Docht (hinter dem Gap)', target:'Festes CRV 2–3 (z. B. XRP: 3)'},
  {en:{"name": "Support & Resistance", "desc": "Reaction at a key zone", "checks": ["The zone has clearly reacted in the past", "Price is in the zone now – not already far away", "Confirmation candle: hammer, engulfing or close above/below the zone", "The higher timeframe does not argue against it"], "stop": "Behind the zone plus a little buffer", "target": "Next opposite zone"}, name:'Support & Resistance', icon:'☰', desc:'Reaktion an einer wichtigen Zone',
   checks:['Die Zone hat in der Vergangenheit klar reagiert','Der Kurs ist jetzt in der Zone – nicht schon weit weg','Bestätigungskerze: Hammer, Engulfing oder Schluss über/unter der Zone','Der höhere Zeitrahmen spricht nicht dagegen'],
   stop:'Hinter der Zone plus etwas Puffer', target:'Nächste gegenüberliegende Zone'},
  {en:{"name": "Trend Following / Breakout", "desc": "Ride along when a level breaks", "checks": ["Clear trend: higher highs and lows (or lower)", "Breakout with a candle close above or below the level", "No major resistance right in front of price", "Momentum or volume confirms the breakout"], "stop": "Behind the last swing", "target": "Trailing stop or fixed R:R"}, name:'Trendfolge / Breakout', icon:'↗', desc:'Mitfahren, wenn ein Level bricht',
   checks:['Klarer Trend: höhere Hochs und Tiefs (bzw. tiefere)','Ausbruch mit Kerzenschluss über bzw. unter dem Level','Direkt vor dem Kurs liegt kein großer Widerstand','Momentum oder Volumen bestätigt den Ausbruch'],
   stop:'Hinter dem letzten Swing', target:'Nachziehender Stop oder festes CRV'},
  {en:{"name": "Pullback", "desc": "Entry on a pullback within a trend", "checks": ["The higher-timeframe trend is intact", "Price pulls back to a zone or moving average", "Reversal signal after the candle closes", "Structure is not broken"], "stop": "Below or above the pullback low/high", "target": "Last high or low of the trend"}, name:'Pullback', icon:'↩', desc:'Einstieg im Rücksetzer eines Trends',
   checks:['Der Trend auf dem höheren Zeitrahmen ist intakt','Der Kurs setzt an eine Zone oder gleitende Linie zurück','Umkehrsignal nach Kerzenschluss','Die Struktur ist nicht gebrochen'],
   stop:'Unter bzw. über dem Tief des Rücksetzers', target:'Letztes Hoch bzw. Tief des Trends'},
  {en:{"name": "Range", "desc": "Sideways market, edge to edge", "checks": ["The range is clear: top and bottom touched at least twice", "Price is at the edge of the range – not in the middle", "Rejection at the edge after the candle closes", "No breakout on the higher timeframe"], "stop": "Just outside the range", "target": "Middle or other side of the range"}, name:'Range', icon:'⇆', desc:'Seitwärtsmarkt von Rand zu Rand',
   checks:['Die Range ist klar: oben und unten mindestens zweimal berührt','Der Kurs ist am Rand der Range – nicht in der Mitte','Ablehnung am Rand nach Kerzenschluss','Kein Ausbruch auf dem höheren Zeitrahmen'],
   stop:'Knapp außerhalb der Range', target:'Mitte oder andere Seite der Range'},
  {en:{"name": "Liquidity Sweep", "desc": "False break of an obvious level", "checks": ["Obvious high or low with stops behind it", "A wick takes the liquidity and closes back inside", "Structure break on the lower timeframe", "Entry on the pullback, not in the impulse"], "stop": "Behind the wick of the sweep", "target": "Opposite liquidity"}, name:'Liquidity Sweep', icon:'⤓', desc:'Fehlausbruch über ein offensichtliches Level',
   checks:['Offensichtliches Hoch bzw. Tief mit Stops dahinter','Docht holt die Liquidität und schließt wieder zurück','Strukturbruch auf dem kleineren Zeitrahmen','Einstieg im Rücksetzer, nicht im Impuls'],
   stop:'Hinter dem Docht des Sweeps', target:'Gegenüberliegende Liquidität'}
];
const sid = () => 's' + Date.now().toString(36) + Math.floor(Math.random()*1e4).toString(36);
const tplByName = n => { const q = String(n||'').toLowerCase().trim(); if (!q) return null;
  const names = t => [t.name, t.en && t.en.name, LANG !== 'de' && LANG !== 'en' && tr(t.name)].filter(Boolean).map(x => x.toLowerCase());
  return TEMPLATES.find(t => names(t).includes(q)) || TEMPLATES.find(t => names(t).some(x => x.startsWith(q))) || null; };
/* Vorlagen in der Sprache des Nutzers anlegen – gespeichert wird, was er sieht und bearbeitet */
const tplL = t => LANG === 'de' ? t : (LANG === 'en' && t.en) ? Object.assign({icon: t.icon}, t.en)
  : {icon: t.icon, name: tr(t.name), desc: tr(t.desc || ''), checks: (t.checks || []).map(c => tr(c)), stop: tr(t.stop || ''), target: tr(t.target || '')};
function fromTemplate(t){ const l = tplL(t); return {id: sid(), name: l.name, checks: l.checks.slice(), stop: l.stop||'', target: l.target||''}; }
function strategies(){
  if (Array.isArray(cfg.strategies) && cfg.strategies.length) return cfg.strategies;
  /* Umzug von der alten Version: eine Liste für alle Strategien */
  const names = (Array.isArray(cfg.strats) && cfg.strats.length) ? cfg.strats : DEFAULTS.strats;
  const custom = Array.isArray(cfg.checks) && cfg.checks.length && cfg.checks.join('|') !== DEFAULT_CHECKS.join('|') ? cfg.checks : null;
  cfg.strategies = names.map(n => { const t0 = tplByName(n), t = t0 ? tplL(t0) : {name:n, checks:defChecks()};
    return {id: sid(), name: t0 ? t.name : n, checks: (custom || t.checks).slice(), stop: t.stop||'', target: t.target||''}; });
  return cfg.strategies;
}
const stratBy = name => strategies().find(s => s.name === name) || strategies()[0];
const defChecks = () => LANG === 'de' ? DEFAULT_CHECKS : LANG === 'en' ? DEFAULT_CHECKS_EN : DEFAULT_CHECKS.map(c => tr(c));
const checksFor = name => { const s = stratBy(name); return s && s.checks.length ? s.checks : defChecks(); };
function ckHtml(name){ return checksFor(name).map(c=>`<button class="ck" onclick="this.classList.toggle('on');ckUpd()"><span class="box"></span><span>${esc(c)}</span></button>`).join(''); }
const needsOnboarding = () => !cfg.onboarded && !cache.journal.length && !(Array.isArray(cfg.strategies) && cfg.strategies.length);

/* Einrichtung beim ersten Start: 1) Strategien wählen 2) Fragen prüfen 3) Grundregeln */
let ob = null;
function onboarding(stage=0){
  stopTimers(); curView = 'onboard'; progress(null); closeModal();
  if (!ob) ob = {pick: new Set(), list: [], i: 0};
  if (stage===0){
    render(`<div class="card ob">
      <div class="tag">● Willkommen bei NoTilt</div>
      <div class="q">Wie lange tradest du schon?</div>
      <p class="hint">Damit die App nur zeigt, was du gerade brauchst. Du kannst es jederzeit in den Einstellungen ändern.</p>
      <div class="opts">
        <button class="opt" onclick="cfg.mode='simple';onboarding(1)"><span class="k">1</span>Ich fange gerade erst an</button>
        <button class="opt" onclick="cfg.mode='simple';onboarding(1)"><span class="k">2</span>Seit ein paar Monaten</button>
        <button class="opt" onclick="cfg.mode='pro';onboarding(1)"><span class="k">3</span>Länger als ein Jahr</button>
      </div>
      <div class="row"><button class="btn ghost" style="flex:1" onclick="obSkip()">Später einrichten</button></div>
    </div>`);
  }
  if (stage===1){
    render(`<div class="card ob">
      <div class="tag">● Einrichtung 1/3</div>
      <div class="q">Welche Strategien tradest du?</div>
      <p class="hint">Tipp alle an, die du nutzt. Jede bekommt ihre eigene Checkliste – die kannst du gleich anpassen.</p>
      <div class="tpls">${TEMPLATES.map((t,i)=>`<button class="tpl ${ob.pick.has(i)?'on':''}" data-i="${i}"><span class="ti">${t.icon}</span><b>${esc(tplL(t).name)}</b><small>${esc(tplL(t).desc)}</small></button>`).join('')}
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
      <p class="hint small">Faustregel: ${fmt(0.5,1)}–1 % Risiko pro Trade. So überstehst du auch eine Pechsträhne von 10 Verlusten.</p>
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
  const tp = tplByName(s.name);
  modal(`<div class="tag">● Strategie</div>${stratEditor(s, false)}
    ${tp ? `<button type="button" class="linkbtn" onclick="tplInto('${id}')">Aktuelle Vorlage „${esc(tplL(tp).name)}“ übernehmen</button>` : ''}
    <div class="row"><button class="btn ghost" onclick="closeModal()">Abbrechen</button><button class="btn pri" onclick="saveStrat('${id}')">Speichern</button></div>
    ${strategies().length > 1 ? `<div class="row" style="margin-top:8px"><button class="btn ghost danger" style="flex:1" onclick="delStrat('${id}', this)">Strategie löschen</button></div>` : ''}`);
  growAll();
}
/* Fragen, Stop und Ziel aus der aktuellen Vorlage ins Formular übernehmen – gespeichert wird erst mit „Speichern“ */
function tplInto(id){
  const s = strategies().find(x => x.id===id), t = s && tplByName(s.name); if (!t) return;
  const l = tplL(t); $('#se_checks').innerHTML = l.checks.map(c => ckRow(c)).join(''); $('#se_stop').value = l.stop || ''; $('#se_tp').value = l.target || ''; growAll();
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
    <div class="tpls">${TEMPLATES.filter(t => !have.has(t.name) && !(t.en && have.has(t.en.name))).map(t=>`<button class="tpl" onclick="newStrat('${esc(t.name)}')"><span class="ti">${t.icon}</span><b>${esc(tplL(t).name)}</b><small>${esc(tplL(t).desc)}</small></button>`).join('')}
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
let acctFrom = false;
function accountView(mode='login'){
  if (curView === 'settings'){ retStack = [settings]; acctFrom = true; } else if (curView !== 'account') acctFrom = false;
  stopTimers(); curView = 'account'; progress(null); closeModal();
  const T = {
    login:  ['Willkommen bei NoTilt', 'Melde dich an, damit dein Journal auf allen Geräten gleich ist.', 'Anmelden'],
    signup: ['Konto erstellen', 'Mit Konto sind Journal und Einstellungen auf Mac und iPhone synchron.', 'Konto erstellen'],
    reset:  ['Passwort vergessen', 'Wir schicken dir einen Link, mit dem du ein neues Passwort setzt.', 'Link senden'],
    newpw:  ['Neues Passwort', 'Wähle ein neues Passwort mit mindestens 8 Zeichen.', 'Passwort speichern']
  }[mode];
  render(`<div class="card auth">
    ${acctFrom ? backBtn('ret()') : ''}<div class="tag">● Konto</div>
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
      LS.set('localMode', false); acctFrom ? ret() : home(); return;
    }
    if (mode==='signup'){
      const { data, error } = await sb.auth.signUp({email:mail, password:pw, options:{emailRedirectTo: back}});
      if (error) throw error;
      if (data.session){ LS.set('localMode', false); acctFrom ? ret() : home(); return; }
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
  await removeShots(cache.journal.flatMap(t => t.shots || []));
  try { const reg = await navigator.serviceWorker.ready, sub = await reg.pushManager.getSubscription(); if (sub) await sub.unsubscribe(); } catch (e){}
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
  const data = {app:'notilt', v:1, exported:new Date().toISOString(), cfg:JSON.parse(JSON.stringify(cfg)),
    journal: journalList(), stops: S.get('stops', []), lastStrat: S.get('lastStrat', null)};
  saveFile(`notilt_backup_${today()}.json`, JSON.stringify(data, null, 1), 'application/json');
}
function importBackup(inp){
  const f = inp.files && inp.files[0]; if (!f) return;
  const msg = $('#impmsg');
  const r = new FileReader();
  r.onload = () => {
    try {
      const d = JSON.parse(r.result);
      if (!d || !['notilt','trade-pilot'].includes(d.app) || !Array.isArray(d.journal)) throw new Error('format');
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
    } catch(e){ msg.textContent = 'Das ist keine gültige NoTilt-Backup-Datei.'; msg.className = 'msg bad'; }
    inp.value = '';
  };
  r.readAsText(f);
}

/* ------------------------------------------------------------------
   Start der App
------------------------------------------------------------------ */
async function boot(){
  startI18n();
  if (!HAS_BACKEND){ if (needsDisclaimer() || !restoreNav()) home(); startSync(); return; }
  let session = null;
  try { session = (await sb.auth.getSession()).data.session; } catch(e){}
  if (!session && needsLogin()) accountView('login'); else if (needsDisclaimer()) home(); else if (new URLSearchParams(location.search).get('view') === 'review' && cache.journal.length) weeklyReview(); else if (!restoreNav()) home();
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
