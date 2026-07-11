#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

tracked=$(git ls-files -co --exclude-standard)
test -n "$tracked"

forbidden='quicklee-|ansible_user[[:space:]]*=[[:space:]]*batman|vmselect\.infra|infra\.quiknode|150\.136\.|54\.225\.|3\.71\.|18\.181\.|13\.250\.|54\.193\.|35\.198\.|34\.21\.|34\.102\.|35\.243\.|34\.126\.|130\.61\.|155\.248\.|152\.69\.|64\.181\.|(^|[^A-Za-z0-9])(xapt|xaat|sk)[_-][A-Za-z0-9_-]{8,}|cz7zDD'

scan_targets=$(printf '%s\n' "$tracked" | grep -v '^scripts/check-public-surface\.sh$' || true)
if test -n "$scan_targets" && printf '%s\n' "$scan_targets" | xargs grep -EnI "$forbidden"; then
  echo "public-surface check found a private identifier or credential pattern" >&2
  exit 1
fi

tenant_hosts=$(
  if test -n "$scan_targets"; then
    printf '%s\n' "$scan_targets" |
      xargs grep -Eho '[a-z0-9][a-z0-9-]*\.hype-mainnet\.quiknode\.pro' |
      sort -u |
      grep -Ev '^(your-endpoint|example-guide-demo)\.hype-mainnet\.quiknode\.pro$' || true
  fi
)
if test -n "$tenant_hosts"; then
  echo "public-surface check found a concrete Quicknode tenant hostname" >&2
  exit 1
fi

if find . -type f \( -name '.env' -o -name '*.pem' -o -name '*.key' -o -name '*.p12' \) -print -quit | grep -q .; then
  echo "public-surface check found a forbidden credential file" >&2
  exit 1
fi

echo "public surface contains no known private fleet or credential patterns"
