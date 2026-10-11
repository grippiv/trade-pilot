-- Trade Pilot: Push-Erinnerungen
create table if not exists public.push_subs (
  id bigserial primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  endpoint text not null unique,
  p256dh text not null,
  auth text not null,
  remind_time text,
  remind_days int[] not null default '{1,2,3,4,5}',
  review boolean not null default true,
  tz text not null default 'Europe/Berlin',
  lang text not null default 'de',
  last_daily text,
  last_review text,
  created_at timestamptz not null default now()
);
alter table public.push_subs enable row level security;   -- keine Policies: nur die Server-Funktion darf lesen/schreiben
revoke all on public.push_subs from anon, authenticated;

create table if not exists public.push_keys (
  id int primary key default 1,
  pub text not null,
  priv_jwk jsonb not null
);
alter table public.push_keys enable row level security;
revoke all on public.push_keys from anon, authenticated;

create extension if not exists pg_net;
create extension if not exists pg_cron;
select cron.unschedule('tp-push-tick') where exists (select 1 from cron.job where jobname = 'tp-push-tick');
select cron.schedule('tp-push-tick', '* * * * *',
  $$ select net.http_post(
       url := 'https://ukhjaztibijjdzlkbcwz.supabase.co/functions/v1/push',
       body := '{"action":"tick"}'::jsonb,
       headers := '{"Content-Type":"application/json"}'::jsonb,
       timeout_milliseconds := 20000) $$);
