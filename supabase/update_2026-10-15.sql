-- NoTilt Update 15.10.2026: Push vor wichtigen News + Feedback aus der App
-- Einmal komplett im Supabase SQL Editor ausführen.

-- 1) Push 30 Min. vor wichtigen US-News (für alle mit aktiver Erinnerung, abschaltbar in den Einstellungen)
alter table public.push_subs add column if not exists news boolean not null default true;
alter table public.push_subs add column if not exists last_news text;

-- 2) Feedback: jeder darf senden, niemand außer dir (Dashboard) darf lesen
create table if not exists public.feedback (
  id bigserial primary key,
  created_at timestamptz not null default now(),
  kind text not null default 'idea' check (kind in ('bug','idea','praise')),
  text text not null check (char_length(text) between 3 and 2000),
  contact text check (contact is null or char_length(contact) <= 200),
  lang text check (lang is null or char_length(lang) <= 8),
  ver text check (ver is null or char_length(ver) <= 30),
  view text check (view is null or char_length(view) <= 30),
  ua text check (ua is null or char_length(ua) <= 200),
  user_id uuid references auth.users(id) on delete set null
);
alter table public.feedback enable row level security;
revoke all on public.feedback from anon, authenticated;
grant insert on public.feedback to anon, authenticated;
grant usage on sequence public.feedback_id_seq to anon, authenticated;
drop policy if exists "feedback_insert" on public.feedback;
create policy "feedback_insert" on public.feedback for insert to anon, authenticated
  with check (user_id is null or user_id = auth.uid());
