# Contributing to Meridian

Thank you for helping.

## During the preview

Bug reports, questions and ideas are welcome as GitHub issues and discussions. Code
contributions open once a contributor licence agreement is in place; this file will then say
how to sign it. Contributions will be accepted under Apache-2.0.

## What must never be in a contribution

- **Secrets**: API keys, tokens, passwords, private keys, connection strings with credentials,
  `.env` files. Use `.env.example` with placeholders. A leaked secret must be rotated even after
  it is removed.
- **Customer or personal data**: real names, e-mail addresses, records or exports. Use reserved
  domains (`example.com`, `.example`, `.test`) and synthetic data.
- **Internal URLs and identifiers**: hosted project refs, internal hostnames, dashboards,
  ticket links.
- **The same in AI prompts**: do not paste secrets, customer data or internal URLs into an AI
  tool, and review generated code like any other code.

## Development

See `docs/LOCAL_DEV.md`. Before you open a pull request:

```bash
bun x eslint .
bun run build
bun x tsc --noEmit
RUN_TENANT_E2E=0 bun x vitest run
```

Changes to authorization, row-level security, server functions or worker APIs need tests that
prove both the allowed and the denied cases (see `src/test/tenant-isolation.e2e.test.ts`).

## Pull requests

- Keep each pull request to one concern, with a clear description of the problem and the fix.
- Add or update tests; a bug fix includes a regression test.
- New dependencies need a permissive licence and a reason.
- Pin GitHub Actions by full commit SHA and container images by digest.

## Reporting security issues

Do not open an issue. Follow `SECURITY.md`.

## Conduct

This project follows the Contributor Covenant (`CODE_OF_CONDUCT.md`).
