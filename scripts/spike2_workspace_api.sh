#!/usr/bin/env bash
# SPIKE 2 — what automation surface does Free Edition actually expose?
#
# NOT blocking, but it decides how Phases 11 and 12 are built. Free Edition
# documents "no account console, no account-level APIs" — it says nothing
# definite about workspace-level API, service principals, secret scopes or
# Asset Bundles, and those are what CI/CD needs.
#
#   ./scripts/spike2_workspace_api.sh
#
# Prerequisite: databricks CLI installed and `databricks auth login` completed
# against the Free Edition workspace host.
set -uo pipefail   # deliberately NOT -e: every probe should run even if one fails

PROFILE="${DATABRICKS_CONFIG_PROFILE:-DEFAULT}"
pass=0; fail=0
RESULTS=()

probe() {
  local name="$1"; shift
  if "$@" >/tmp/np_spike2.out 2>&1; then
    echo "  PASS  ${name}"
    RESULTS+=("PASS|${name}")
    ((pass++))
  else
    echo "  FAIL  ${name}"
    echo "        $(head -2 /tmp/np_spike2.out | tr '\n' ' ')"
    RESULTS+=("FAIL|${name}")
    ((fail++))
  fi
}

echo "Spike 2 — Free Edition automation surface (profile: ${PROFILE})"
echo

echo "--- workspace REST API ---"
probe "current user"            databricks current-user me -p "${PROFILE}"
probe "list catalogs"           databricks catalogs list -p "${PROFILE}"
probe "list schemas"            databricks schemas list workspace -p "${PROFILE}"
probe "list warehouses"         databricks warehouses list -p "${PROFILE}"
probe "list jobs"               databricks jobs list -p "${PROFILE}"
probe "list external locations" databricks external-locations list -p "${PROFILE}"
probe "list volumes"            databricks volumes list workspace default -p "${PROFILE}"

echo
echo "--- identity: needed for CI/CD ---"
# Expected to FAIL: service principal management is account-level, and Free
# Edition has no account console. If it fails, CI/CD authenticates with a
# personal access token stored as a GitHub secret, and CI_CD.md must say so
# plainly rather than implying a service principal was used.
probe "list service principals" databricks service-principals list -p "${PROFILE}"

echo
echo "--- secrets: needed to avoid credentials in code ---"
# If secret scopes work, pipeline credentials live in Databricks. If not,
# everything comes from GCP Secret Manager via the job's environment.
probe "list secret scopes"      databricks secrets list-scopes -p "${PROFILE}"

echo
echo "--- Asset Bundles: the Phase 12 deployment mechanism ---"
if [[ -f "bundle/databricks.yml" ]]; then
  probe "bundle validate" databricks bundle validate -p "${PROFILE}"
else
  echo "  SKIP  bundle validate (bundle/databricks.yml not created until Phase 11)"
fi

echo
echo "--- Terraform provider (workspace-scoped only) ---"
if command -v terraform >/dev/null 2>&1; then
  echo "  terraform $(terraform version -json 2>/dev/null | head -c 60)"
else
  echo "  SKIP  terraform not installed"
fi

rm -f /tmp/np_spike2.out

echo
echo "==================== SUMMARY ===================="
printf '%s\n' "${RESULTS[@]}" | column -t -s'|'
echo
echo "passed: ${pass}   failed: ${fail}"
cat <<'EOF'

How to read this:

  Catalogs/schemas/jobs/warehouses PASS
      -> Terraform (workspace-scoped) and Asset Bundles are viable. Phases
         11-12 proceed as designed.

  Service principals FAIL
      -> Expected. CI/CD uses a PAT in GitHub secrets. Document it honestly in
         CI_CD.md: "Free Edition has no account console, so a service
         principal is not available; in production this would be a service
         principal, never a personal token."

  Secret scopes FAIL
      -> Credentials come from GCP Secret Manager instead. Update SECURITY.md
         §4 accordingly.

Record the outcome in docs/PHASE2-SETUP.md before starting Phase 3.
EOF
