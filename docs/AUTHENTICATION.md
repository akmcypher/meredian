# Community authentication and onboarding

Account onboarding (migration 0077) extends the existing organizations, memberships and profiles; it does not replace the tenant model or change global roles.

## Registration and workspace lifecycle

The supplied local configuration enables email signup, requires email confirmation and enforces 12-character passwords containing uppercase, lowercase and digits. The UI reads the actual Auth service policy, so a running service with signup disabled continues to show administrator provisioning.

Supabase sends verification and recovery email. A confirmed auth.users insert/update provisions the workspace transactionally. A profile row lock serializes retries; an existing authorized membership is reused. New owners receive a workspace-scoped owner membership and no global role. Failed provisioning rolls back confirmation and can retry. Existing profiles, memberships and platform administrator roles are preserved.

The callback exchanges PKCE codes or validates a token hash, clears URL parameters, checks the verified account and resolves its authorized workspace. PKCE links require the originating browser's verifier; an invalid/expired or different-device link must return to sign-in or request a fresh recovery email. Onboarding persists workspace details, a personal first-source preference and completion. Only owners/admins can edit shared workspace details; members can finish their own setup.

## Deployment configuration

For a new local installation, follow LOCAL_DEV.md and use http://localhost:8080. A running local Auth service must be restarted to pick up changes to supabase/config.toml. Never reset an existing database to apply this change.

For hosted installations, explicitly configure Supabase Auth email signup and confirmation, password length/character policy and rate limits. Set Site URL to the actual HTTPS application origin and allow only the exact /auth/callback and /auth/callback?flow=recovery URLs. Local development allows these paths on localhost:8080. Configure a production SMTP sender, verified sending domain and delivery monitoring; application alert SMTP variables do not configure Supabase Auth email delivery. Test delivery and both callbacks before enabling public registration. Do not rely on a provider's built-in test mailer in production.

The browser receives only VITE_SUPABASE_URL and VITE_SUPABASE_PUBLISHABLE_KEY. Server keys remain server-only. VITE_AUTH_GOOGLE_ENABLED is false unless the corresponding provider is configured. Configure SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY and SUPABASE_SERVICE_ROLE_KEY for the same project.

Migration 0077 installs a PostgREST pre-request active-account check while preserving an existing database-configured hook. Verify the effective pgrst.db_pre_request setting and config reload in the target deployment. This protects definer RPCs as well as ordinary Data API requests. Restrictive active-account RLS also protects public tables and storage objects; existing workspace/owner policies still apply. Server middleware independently verifies the user through Auth.

## Administration and retention

Use the existing controlled bootstrap_platform_admin flow only for installation administration; it is never invoked by registration. Team invitations are not available yet; the platform invitation API assigns global roles and is meant for administrators only. Account profile editing and password recovery are available. Account deactivation/deletion remains an administrator-managed retention operation; no self-service destructive deletion is exposed.

References: [local Auth configuration](https://supabase.com/docs/guides/local-development/cli/config), [email testing and SMTP](https://supabase.com/docs/guides/auth/testing-auth-flows), [PKCE](https://supabase.com/docs/guides/auth/sessions/pkce-flow), [PostgREST pre-request configuration](https://docs.postgrest.org/en/stable/references/configuration.html).
