-- NoTilt: Datenbank einrichten
-- In Supabase: SQL Editor → New query → alles einfügen → Run

create table if not exists public.tp_state (
  user_id    uuid primary key references auth.users(id) on delete cascade,
  data       jsonb  not null default '{}'::jsonb,
  updated_at bigint not null default 0
);

create table if not exists public.tp_trades (
  user_id    uuid   not null references auth.users(id) on delete cascade,
  id         text   not null check (char_length(id) <= 40),
  data       jsonb  not null,
  updated_at bigint not null default 0,
  primary key (user_id, id)
);

-- Jeder sieht und ändert nur seine eigenen Daten
alter table public.tp_state  enable row level security;
alter table public.tp_trades enable row level security;

drop policy if exists "eigener state"  on public.tp_state;
drop policy if exists "eigene trades"  on public.tp_trades;
create policy "eigener state" on public.tp_state  for all to authenticated
  using ((select auth.uid()) = user_id) with check ((select auth.uid()) = user_id);
create policy "eigene trades" on public.tp_trades for all to authenticated
  using ((select auth.uid()) = user_id) with check ((select auth.uid()) = user_id);

grant select, insert, update, delete on public.tp_state, public.tp_trades to authenticated;
revoke all on public.tp_state, public.tp_trades from anon;

-- Live-Sync zwischen Geräten
do $$ begin
  alter publication supabase_realtime add table public.tp_state;
exception when duplicate_object then null; end $$;

-- Konto selbst löschen (DSGVO: Recht auf Löschung)
create or replace function public.delete_my_account()
returns void language sql security definer set search_path = ''
as $$ delete from auth.users where id = auth.uid(); $$;
revoke execute on function public.delete_my_account() from public, anon;
grant execute on function public.delete_my_account() to authenticated;
