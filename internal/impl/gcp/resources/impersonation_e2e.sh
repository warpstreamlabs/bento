#!/usr/bin/env bash
# Provisions the live GCP resources for TestIntegrationGCPImpersonationLive
# (see ../integration_impersonation_test.go). Usage stays within GCP free tiers.
#
# Usage: PROJECT_ID=my-project ./impersonation_e2e.sh <setup|adc|teardown>
set -euo pipefail

: "${PROJECT_ID:?set PROJECT_ID to your GCP project id}"
REGION="${REGION:-us-central1}"
BUCKET="${PROJECT_ID}-bento-e2e"
TOPIC="bento-e2e-topic"
SUB="bento-e2e-sub"
RUNTIME_SA="bento-e2e-runtime@${PROJECT_ID}.iam.gserviceaccount.com"
INPUT_SA="bento-e2e-input@${PROJECT_ID}.iam.gserviceaccount.com"
STORAGE_SA="bento-e2e-storage@${PROJECT_ID}.iam.gserviceaccount.com"
HOP_SA="bento-e2e-hop@${PROJECT_ID}.iam.gserviceaccount.com"

case "${1:-}" in

setup)
  gcloud services enable --project "$PROJECT_ID" \
    iamcredentials.googleapis.com storage.googleapis.com pubsub.googleapis.com

  # Creates tolerate "already exists" failures so setup can be rerun; read any
  # error above the "continuing" line to tell a rerun from a real failure.
  for sa in bento-e2e-runtime bento-e2e-input bento-e2e-storage bento-e2e-hop; do
    gcloud iam service-accounts create "$sa" --project "$PROJECT_ID" \
      || echo "continuing: SA $sa not created (see error above)"
  done
  gcloud storage buckets create "gs://${BUCKET}" --project "$PROJECT_ID" \
    --location "$REGION" --uniform-bucket-level-access \
    || echo "continuing: bucket not created (see error above)"
  gcloud pubsub topics create "$TOPIC" --project "$PROJECT_ID" \
    || echo "continuing: topic not created (see error above)"
  gcloud pubsub subscriptions create "$SUB" --topic "$TOPIC" --project "$PROJECT_ID" \
    || echo "continuing: subscription not created (see error above)"

  # Asymmetric permissions: storage SA -> bucket only, input SA -> pubsub only,
  # runtime SA -> impersonation only. This is what makes a passing test conclusive.
  gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
    --member "serviceAccount:${STORAGE_SA}" --role roles/storage.objectAdmin
  gcloud pubsub subscriptions add-iam-policy-binding "$SUB" --project "$PROJECT_ID" \
    --member "serviceAccount:${INPUT_SA}" --role roles/pubsub.subscriber
  gcloud pubsub topics add-iam-policy-binding "$TOPIC" --project "$PROJECT_ID" \
    --member "serviceAccount:${INPUT_SA}" --role roles/pubsub.publisher
  ACCOUNT="$(gcloud config get-value account 2>/dev/null)"
  : "${ACCOUNT:?no active gcloud account — run 'gcloud auth login' first}"
  gcloud iam service-accounts add-iam-policy-binding "$RUNTIME_SA" --project "$PROJECT_ID" \
    --member "user:${ACCOUNT}" \
    --role roles/iam.serviceAccountTokenCreator
  for target in "$INPUT_SA" "$STORAGE_SA" "$HOP_SA"; do
    gcloud iam service-accounts add-iam-policy-binding "$target" --project "$PROJECT_ID" \
      --member "serviceAccount:${RUNTIME_SA}" --role roles/iam.serviceAccountTokenCreator
  done
  # Delegation chain (runtime -> hop -> storage) for the delegated test.
  gcloud iam service-accounts add-iam-policy-binding "$STORAGE_SA" --project "$PROJECT_ID" \
    --member "serviceAccount:${HOP_SA}" --role roles/iam.serviceAccountTokenCreator

  cat <<EOF

Setup done (IAM grants take 1-7 min to propagate). Next: $0 adc,
then run from the repo root:

GCP_E2E_BUCKET=${BUCKET} \\
GCP_E2E_STORAGE_SA=${STORAGE_SA} \\
GCP_E2E_PROJECT=${PROJECT_ID} \\
GCP_E2E_TOPIC=${TOPIC} \\
GCP_E2E_SUBSCRIPTION=${SUB} \\
GCP_E2E_INPUT_SA=${INPUT_SA} \\
GCP_E2E_HOP_SA=${HOP_SA} \\
go test -v -timeout 10m -run 'TestIntegrationGCPImpersonationLive' ./internal/impl/gcp/
EOF
  ;;

adc)
  # Base ADC = the powerless runtime SA, so test success is attributable to
  # impersonation rather than to your own (likely privileged) identity.
  gcloud auth application-default login \
    --impersonate-service-account="$RUNTIME_SA" --billing-project="$PROJECT_ID"
  echo "ADC is now ${RUNTIME_SA}. Undo with: gcloud auth application-default revoke"
  ;;

teardown)
  gcloud pubsub subscriptions delete "$SUB" --project "$PROJECT_ID" --quiet || true
  gcloud pubsub topics delete "$TOPIC" --project "$PROJECT_ID" --quiet || true
  gcloud storage rm -r "gs://${BUCKET}" || true
  for sa in "$RUNTIME_SA" "$INPUT_SA" "$STORAGE_SA" "$HOP_SA"; do
    gcloud iam service-accounts delete "$sa" --project "$PROJECT_ID" --quiet || true
  done
  echo "Done. Reset ADC: gcloud auth application-default revoke"
  ;;

*)
  echo "Usage: PROJECT_ID=<id> $0 <setup|adc|teardown>"
  exit 1
  ;;
esac
