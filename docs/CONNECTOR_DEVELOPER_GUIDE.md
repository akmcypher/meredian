# Connect SDK developer guide

The original v1 SDK, executable setup, JSON actor contracts, dependency/license boundary and truthful release gaps are documented in [deploy/connect/README.md](../deploy/connect/README.md).

Start a new actor from `deploy/connect/template.py`, use `sdk.py` for newline JSON messages, and implement spec/check/discover/read. Persist checkpoints only through destination acknowledgment after immutable bronze commit. Adapter errors must use safe codes; never log credentials or raw source records. Test full extraction, cursor ties, restart and schema refusal against a real fixture before changing capability status. Do not mark an adapter Available from a template or mocked test.

Release requires actual immutable image digests, signing, vulnerability/license scan, SBOM and isolated runtime acceptance. The scripts are scaffolding, not evidence those gates have executed. Do not add third-party CDKs without version-specific licensing review.