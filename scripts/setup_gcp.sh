#!/usr/bin/env bash
# GCP setup for the landing zone. Run once, in Phase 2.
#
# Creates: project, budget alert, GCS bucket, Secret Manager secrets, ADC.
# Everything here stays inside the Always Free tier. The $1 budget alert is
# created BEFORE any billable resource, deliberately.
#
#   ./scripts/setup_gcp.sh
#
# Prerequisites: gcloud CLI installed and `gcloud auth login` completed.
set -euo pipefail

# ---- configuration ---------------------------------------------------------
PROJECT_ID="${GCP_PROJECT_ID:-northpeak-lakehouse-$RANDOM}"
BUCKET="${GCS_BUCKET:-${PROJECT_ID}-landing}"

# Always Free Cloud Storage is limited to these three regions, Standard class.
# Any other region bills from the first byte. This is cost trap #2.
REGION="us-central1"

echo "project : ${PROJECT_ID}"
echo "bucket  : gs://${BUCKET}"
echo "region  : ${REGION}"
read -rp "proceed? [y/N] " ok && [[ "${ok}" == "y" ]] || exit 1

# ---- 1. project ------------------------------------------------------------
if ! gcloud projects describe "${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud projects create "${PROJECT_ID}" --name="NorthPeak Lakehouse"
fi
gcloud config set project "${PROJECT_ID}"

BILLING_ACCOUNT="$(gcloud billing accounts list --format='value(name)' --limit=1)"
[[ -n "${BILLING_ACCOUNT}" ]] || { echo "no billing account found — link one in the console first"; exit 1; }
gcloud billing projects link "${PROJECT_ID}" --billing-account="${BILLING_ACCOUNT}"

# ---- 2. budget alert FIRST -------------------------------------------------
# A $1 threshold is deliberately absurd. It is not a spending cap — it is a
# tripwire that emails within hours of any unexpected charge, while it is
# still one dollar.
echo "creating \$1 budget alert ..."
gcloud billing budgets create \
  --billing-account="${BILLING_ACCOUNT}" \
  --display-name="northpeak-tripwire" \
  --budget-amount=1USD \
  --threshold-rule=percent=0.5 \
  --threshold-rule=percent=0.9 \
  --threshold-rule=percent=1.0 \
  || echo "  budget creation failed (needs billing.budgets.create) — create it in the console instead, this is NOT optional"

# ---- 3. APIs ---------------------------------------------------------------
gcloud services enable \
  storage.googleapis.com \
  secretmanager.googleapis.com

# ---- 4. bucket -------------------------------------------------------------
if ! gcloud storage buckets describe "gs://${BUCKET}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${BUCKET}" \
    --location="${REGION}" \
    --default-storage-class=STANDARD \
    --uniform-bucket-level-access \
    --public-access-prevention
fi

# Versioning protects against an accidental overwrite; the lifecycle rule stops
# it becoming an archive. The landing zone is a transfer buffer, not storage —
# Bronze is the durable copy.
gcloud storage buckets update "gs://${BUCKET}" --versioning

cat > /tmp/np_lifecycle.json <<'JSON'
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {"age": 14, "isLive": true}
    },
    {
      "action": {"type": "Delete"},
      "condition": {"daysSinceNoncurrentTime": 7}
    }
  ]
}
JSON
gcloud storage buckets update "gs://${BUCKET}" --lifecycle-file=/tmp/np_lifecycle.json
rm -f /tmp/np_lifecycle.json

# ---- 5. secrets ------------------------------------------------------------
# Values are set interactively, never passed as arguments — a secret in a
# command line lands in your shell history.
for secret in northpeak-api-token northpeak-pg-password; do
  gcloud secrets describe "${secret}" >/dev/null 2>&1 \
    || gcloud secrets create "${secret}" --replication-policy=automatic
done
echo "secrets created (empty). Add versions with:"
echo "  printf 'VALUE' | gcloud secrets versions add northpeak-api-token --data-file=-"

# ---- 6. application default credentials -------------------------------------
# ADC on the host, mounted read-only into the cdc-sink container. Preferred
# over a downloaded service-account key: a key file on disk is the most common
# way a GCP credential ends up in a Git history.
gcloud auth application-default login

cat <<EOF

Done. Put these in docker/.env:

  GCP_PROJECT_ID=${PROJECT_ID}
  GCS_BUCKET=${BUCKET}

Verify:
  gcloud storage ls gs://${BUCKET}
  gcloud billing budgets list --billing-account=${BILLING_ACCOUNT}
EOF
