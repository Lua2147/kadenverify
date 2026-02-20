-- KadenVerify: Supabase schema for verified email results/cache.
-- Apply this in Supabase SQL Editor (or via Supabase CLI migrations).

create table if not exists public.verified_emails (
  email text primary key,
  normalized text not null,
  reachability text not null,
  is_deliverable boolean,
  is_catch_all boolean,
  is_disposable boolean not null default false,
  is_role boolean not null default false,
  is_free boolean not null default false,
  mx_host text,
  smtp_code integer not null default 0,
  smtp_message text,
  provider text,
  domain text,
  verified_at timestamptz not null default now(),
  error text
);

create index if not exists idx_verified_emails_reachability
  on public.verified_emails (reachability);

create index if not exists idx_verified_emails_domain
  on public.verified_emails (domain);

create index if not exists idx_verified_emails_verified_at
  on public.verified_emails (verified_at);

alter table public.verified_emails enable row level security;

-- No policies are created here on purpose:
-- the API service should use the service role key (bypasses RLS).

