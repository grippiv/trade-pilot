-- NoTilt: private Chart-Screenshots
insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values ('shots', 'shots', false, 3145728, array['image/jpeg','image/png','image/webp'])
on conflict (id) do update set public = false, file_size_limit = 3145728, allowed_mime_types = array['image/jpeg','image/png','image/webp'];

drop policy if exists "shots lesen"   on storage.objects;
drop policy if exists "shots anlegen" on storage.objects;
drop policy if exists "shots löschen" on storage.objects;
create policy "shots lesen" on storage.objects for select to authenticated
  using (bucket_id = 'shots' and (storage.foldername(name))[1] = (select auth.uid())::text);
create policy "shots anlegen" on storage.objects for insert to authenticated
  with check (bucket_id = 'shots' and (storage.foldername(name))[1] = (select auth.uid())::text);
create policy "shots löschen" on storage.objects for delete to authenticated
  using (bucket_id = 'shots' and (storage.foldername(name))[1] = (select auth.uid())::text);
