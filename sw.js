/* Offline-Modus: online immer die neueste Version laden, offline die gespeicherte nutzen. */
const CACHE = 'tp-v16';
const CORE = ['./', 'index.html', 'style.css', 'style-cyan.css', 'app.js', 'config.js', 'vendor/supabase.js', 'manifest.webmanifest',
  'icons/icon-192.png', 'icons/icon-512.png', 'icons/apple-touch-icon.png', 'icons/favicon-32.png', 'sounds/atem_ein.mp3', 'sounds/atem_nach.mp3', 'sounds/atem_aus.mp3'];
self.addEventListener('install', e => { e.waitUntil(caches.open(CACHE).then(c => c.addAll(CORE)).then(() => self.skipWaiting())); });
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', e => {
  const req = e.request, url = new URL(req.url);
  if (req.method !== 'GET' || url.origin !== location.origin) return;
  e.respondWith(
    fetch(req, {cache: 'no-cache'}).then(res => {
      if (res.ok) { const copy = res.clone(); caches.open(CACHE).then(c => c.put(req, copy)); }
      return res;
    }).catch(() => caches.match(req, {ignoreSearch: true}).then(r => r || caches.match('index.html')))
  );
});

/* Push-Erinnerungen */
self.addEventListener('push', e => {
  let d = {}; try { d = e.data ? e.data.json() : {}; } catch (_) { d = {body: e.data && e.data.text()}; }
  e.waitUntil(self.registration.showNotification(d.title || 'Trade Pilot', {
    body: d.body || '', tag: d.tag || 'tp', renotify: true, icon: 'icons/icon-192.png', badge: 'icons/favicon-32.png',
    data: {url: d.url || './'}
  }));
});
self.addEventListener('notificationclick', e => {
  e.notification.close();
  const url = new URL((e.notification.data && e.notification.data.url) || './', self.registration.scope).href;
  e.waitUntil(clients.matchAll({type: 'window', includeUncontrolled: true}).then(list => {
    for (const c of list){ if (c.url.startsWith(self.registration.scope)) { c.focus(); return c.navigate ? c.navigate(url) : null; } }
    return clients.openWindow(url);
  }));
});
