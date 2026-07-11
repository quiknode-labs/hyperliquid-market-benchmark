# Security policy

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability or exposed
credential. Use GitHub's private vulnerability reporting for this repository.
Include the affected revision, impact, reproduction steps, and any suggested
mitigation.

We will acknowledge a complete report as soon as practical, investigate it,
and coordinate disclosure after a fix is available. Benchmark disagreements,
methodology questions, and ordinary defects belong in public issues.

## Credential boundary

The collector reads provider and Axiom ingest credentials only from its process
environment. It never includes them in telemetry, logs, deterministic IDs, or
the persistent outbox filename. The repository contains no production
credentials, internal fleet inventory, private hostnames, or tenant endpoint.
