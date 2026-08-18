"""SPIKE 1 — can Databricks Free Edition reach a GCS bucket via Unity Catalog?

BLOCKING. This decides whether the "Databricks reads directly from GCS" story
holds, and it is the one assumption Phase 0 could not settle from documentation.

Run this as a notebook in the Free Edition workspace, cell by cell. It is
written as a plain script so it lives in Git and is reviewable; paste each
numbered block into a cell.

WHY IT IS UNCERTAIN
    Docs confirm Free Edition supports external locations via Unity Catalog.
    On GCP-hosted Databricks a GCS storage credential works by Databricks
    generating a Google service account you then grant on the bucket. But Free
    Edition signup does not clearly let you pick the hosting cloud, and an
    AWS-hosted workspace may only offer IAM roles.

OUTCOME
    PASS -> external location works. Landing zone is read directly from GCS.
    FAIL -> fall back to Option A in PHASE0-FEASIBILITY.md §7: push files into
            a Unity Catalog Volume via the Files API. Auto Loader reads the
            Volume path instead. Nothing else in the architecture changes.

    Record the result in docs/PHASE2-SETUP.md before continuing to Phase 3.
"""

# ---------------------------------------------------------------------------
# 1. What cloud is this workspace actually on?
#    Answers the core question before you spend time in the UI.
# ---------------------------------------------------------------------------
BLOCK_1 = """
SELECT current_metastore(), current_catalog();
"""
# Then: Catalog Explorer -> External Data -> Credentials -> Create credential.
# Inspect the "Credential Type" dropdown:
#   "GCP Service Account"  -> GCP-hosted. Spike very likely PASSES.
#   only "AWS IAM Role"    -> AWS-hosted. GCS external location NOT available.
#                             Stop here and take the Volumes fallback.


# ---------------------------------------------------------------------------
# 2. Create the storage credential (UI)
#    Catalog Explorer -> External Data -> Credentials -> Create credential
#      Type: GCP Service Account
#      Name: northpeak_gcs_cred
#    Databricks generates a service account and shows its email. Copy it.
#
# 3. Grant that service account on the bucket (run on your machine)
# ---------------------------------------------------------------------------
BLOCK_3 = """
# Substitute the generated service account email and your bucket.
SA="<databricks-generated>@<project>.iam.gserviceaccount.com"
BUCKET="northpeak-landing-CHANGEME"

# Two roles, both required:
#   legacyBucketReader -> read bucket metadata
#   objectAdmin        -> read and write objects
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \\
  --member="serviceAccount:${SA}" --role="roles/storage.legacyBucketReader"

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \\
  --member="serviceAccount:${SA}" --role="roles/storage.objectAdmin"

gcloud storage buckets get-iam-policy "gs://${BUCKET}"
"""


# ---------------------------------------------------------------------------
# 4. Create the external location and prove read + write
# ---------------------------------------------------------------------------
BLOCK_4 = """
CREATE EXTERNAL LOCATION IF NOT EXISTS northpeak_landing
  URL 'gs://northpeak-landing-CHANGEME/landing'
  WITH (STORAGE CREDENTIAL northpeak_gcs_cred)
  COMMENT 'Spike 1 — Free Edition GCS reachability';

-- Databricks' own validator. Checks list, read, write and delete.
-- If this passes, the spike passes.
DESCRIBE EXTERNAL LOCATION northpeak_landing;
"""

# ---------------------------------------------------------------------------
# 5. End-to-end proof: write a file, read it back with Auto Loader.
#    DESCRIBE passing is necessary but not sufficient — Auto Loader needs
#    directory listing, which is a different permission path.
# ---------------------------------------------------------------------------
LANDING = "gs://northpeak-landing-CHANGEME/landing/_spike1"

BLOCK_5 = f"""
from pyspark.sql import Row

landing = "{LANDING}"

# --- write ---
spark.createDataFrame([Row(id=1, note="spike1"), Row(id=2, note="spike1")]) \\
     .write.mode("overwrite").json(landing + "/probe")

# --- plain read ---
display(spark.read.json(landing + "/probe"))

# --- Auto Loader read (the one that actually matters) ---
df = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", landing + "/_schema")
        .load(landing + "/probe"))

(df.writeStream
   .option("checkpointLocation", landing + "/_checkpoint")
   .trigger(availableNow=True)
   .toTable("workspace.default.spike1_autoloader"))
"""

BLOCK_6 = """
SELECT count(*) AS rows_ingested FROM workspace.default.spike1_autoloader;
-- Expect 2. If this returns 2, SPIKE 1 PASSES.
"""

# ---------------------------------------------------------------------------
# 7. Clean up — leaving a spike's artefacts behind is how a workspace rots.
# ---------------------------------------------------------------------------
BLOCK_7 = """
DROP TABLE IF EXISTS workspace.default.spike1_autoloader;
-- Keep the external location and credential: Phase 4 uses them.
-- If the spike FAILED, drop them too:
--   DROP EXTERNAL LOCATION IF EXISTS northpeak_landing;
--   DROP STORAGE CREDENTIAL IF EXISTS northpeak_gcs_cred;
"""

# ---------------------------------------------------------------------------
# TROUBLESHOOTING
#
# "PERMISSION_DENIED on storage credential"
#     The IAM binding has not propagated. Wait 60s and retry before assuming
#     failure — GCP IAM is eventually consistent.
#
# "Credential type GCP Service Account not offered"
#     Workspace is not GCP-hosted. This is a genuine FAIL, not a config error.
#     Take the Volumes fallback; do not burn hours on it.
#
# "External location overlaps with an existing one"
#     Free Edition auto-creates a location for its default storage. Use a
#     distinct path prefix under your own bucket.
#
# "Auto Loader works but plain read fails" (or vice versa)
#     Almost always a missing legacyBucketReader — objectAdmin alone grants
#     object access but not bucket metadata, and listing needs metadata.
# ---------------------------------------------------------------------------
