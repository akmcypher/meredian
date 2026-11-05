# Security policy

## Reporting a vulnerability

**Do not open a public issue, discussion or pull request for a security problem.**

Open the repository's **Security** tab and choose **Report a vulnerability**.
See [GitHub's configuration requirements](https://docs.github.com/en/code-security/how-tos/report-and-fix-vulnerabilities/configure-vulnerability-reporting/configure-for-a-repository).
Include:

- the affected version or commit, and the deployment mode (local stack, self-hosted, demo mode);
- what an attacker can do and what they need first (an account, a role, network position);
- reproduction steps or a proof of concept, using test data only;
- any logs or screenshots, with secrets and personal data removed.

Never include real credentials, customer data or production URLs in a report. If you found a
live secret, say where it is and we will rotate it; do not use it.

## What happens next

| Step                                                             | Target                                               |
| ---------------------------------------------------------------- | ---------------------------------------------------- |
| Acknowledge the report                                           | 3 business days                                      |
| Triage: confirm, assign severity (CVSS v4) and tell you the plan | 7 days                                               |
| Fix released: critical / high                                    | 30 days                                              |
| Fix released: medium / low                                       | 90 days                                              |
| Public advisory                                                  | when the fix is released, or at 90 days by agreement |

We coordinate disclosure with you, credit you in the advisory unless you prefer otherwise, and
request a CVE through the GitHub security advisory. If a fix needs longer, we tell you why and
agree a new date. If a vulnerability is being exploited, we may publish mitigations sooner.

## Supported versions

Meridian is pre-1.0. Security fixes go to the latest minor release line and `main`.

| Version            | Supported   |
| ------------------ | ----------- |
| latest `0.x` minor | yes         |
| older `0.x` minors | no; upgrade |

## Security release procedure (maintainers)

1. Work in a private fork created from the draft GitHub security advisory; never on a public
   branch before release.
2. Add a regression test that fails before the fix. Run the unit and live (tenant isolation)
   suites locally (`docs/LOCAL_DEV.md`).
3. Get review from a second maintainer inside the advisory fork.
4. Merge, tag `vX.Y.Z` and publish the release notes. (Signed release artefacts with an SBOM
   and provenance are planned and not yet automated in this repository.)
5. Publish the advisory with affected and fixed versions, severity, workarounds and credit.
6. If secrets or keys were exposed, rotate them before publishing and say so in the advisory.

## Scope

In scope: this repository's code, migrations, workflows and deploy/ assets. Out of scope:
third-party services and images (report those upstream), denial of service by volume alone,
and findings that need a compromised administrator account in a single-tenant install: a
platform administrator can read every organization's data and connection secrets, so run one
install per trust boundary.

Security features are never gated behind a licence.
