# NoTilt

Trading-Checkliste und Journal als Web-App (PWA) – installierbar auf Mac und iPhone, offline nutzbar, Sync über Supabase.

## Einmalige Einrichtung
1. **Supabase:** SQL Editor → New query → Inhalt von `schema.sql` einfügen → Run.
2. **Supabase:** Authentication → URL Configuration → *Site URL* = `https://grippiv.github.io/trade-pilot/`.
3. **Zugangsdaten:** Project URL und anon/publishable Key in `config.js` eintragen (nie den secret/service_role Key).
4. **GitHub Pages:** Settings → Pages → Source: *Deploy from a branch* → Branch `main`, Ordner `/ (root)` → Save.
5. **Rechtliches:** Platzhalter in `impressum.html` und `datenschutz.html` ausfüllen, bevor die App öffentlich geteilt wird.

## Installieren
- **iPhone:** Seite in Safari öffnen → Teilen → *Zum Home-Bildschirm*.
- **Mac (Chrome):** Adressleiste → Installieren-Symbol, oder Menü → Streamen, speichern und teilen → *Seite als App installieren*.
- **Mac (Safari):** Ablage → *Zum Dock hinzufügen*.

Updates kommen automatisch beim nächsten Öffnen.

## Quellen
- Atemgeräusch: „Breathing, calm, mouth exhale“ von giddster, Freesound #336532, Lizenz CC0 – bearbeitet (entrauscht, gestreckt, stereo).
