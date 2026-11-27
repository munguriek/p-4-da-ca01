#!/usr/bin/env bash
# One-time Google Cloud setup for deploying routewatch from GitHub Actions.
# Run it in Cloud Shell, which already has gcloud, curl and jq, signed in as
# the project's owner:
#   bash gcp-setup.sh PROJECT_ID
# It creates, keeping anything that already exists:
#  - a VPC whose only inbound rule is SSH from Identity-Aware Proxy (IAP);
#  - the VM: e2-standard-2, Ubuntu 26.04 LTS, OS Login, no service account;
#  - a deployer service account that may only reach and administer that VM;
#  - a Workload Identity pool and provider that accept GitHub's OIDC tokens
#    only from this repository's main branch in its production environment.
# No key is created: GitHub Actions exchanges a short-lived token instead.
# Override the defaults with environment variables, e.g. ZONE=europe-west2-b.

set -euo pipefail

PROJECT_ID="${1:?usage: gcp-setup.sh PROJECT_ID}"
REGION="${REGION:-europe-west1}"
ZONE="${ZONE:-europe-west1-b}"
VM_NAME="${VM_NAME:-routewatch-vm}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-2}"
DISK_SIZE_GB="${DISK_SIZE_GB:-200}" # about 1.5 days of the full rrc21 feed
IMAGE_FAMILY="${IMAGE_FAMILY:-ubuntu-2604-lts-amd64}"
REPOSITORY=munguriek/p-4-da-ca01
NETWORK=routewatch-net
SUBNET=routewatch-subnet
SUBNET_RANGE=10.20.0.0/24
TAG=routewatch
IAP_RANGE=35.235.240.0/20 # where IAP's TCP forwarding connects from
SA_NAME=routewatch-deployer
POOL=github
PROVIDER=p-4-da-ca01

export CLOUDSDK_CORE_PROJECT="$PROJECT_ID"
exists() { "$@" >/dev/null 2>&1; }
step() { printf '\n== %s\n' "$*"; }

step "APIs"
gcloud services enable compute.googleapis.com iap.googleapis.com \
  oslogin.googleapis.com iam.googleapis.com iamcredentials.googleapis.com \
  sts.googleapis.com cloudresourcemanager.googleapis.com
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"

step "Network: $NETWORK, SSH from IAP only"
exists gcloud compute networks describe "$NETWORK" ||
  gcloud compute networks create "$NETWORK" --subnet-mode=custom
exists gcloud compute networks subnets describe "$SUBNET" --region="$REGION" ||
  gcloud compute networks subnets create "$SUBNET" --network="$NETWORK" \
    --region="$REGION" --range="$SUBNET_RANGE"
exists gcloud compute firewall-rules describe routewatch-allow-iap-ssh ||
  gcloud compute firewall-rules create routewatch-allow-iap-ssh --network="$NETWORK" \
    --direction=INGRESS --action=ALLOW --rules=tcp:22 \
    --source-ranges="$IAP_RANGE" --target-tags="$TAG"

step "VM: $VM_NAME in $ZONE"
if ! exists gcloud compute images describe-from-family "$IMAGE_FAMILY" --project=ubuntu-os-cloud; then
  echo "Image family $IMAGE_FAMILY not found. Families available:" >&2
  gcloud compute images list --project=ubuntu-os-cloud --no-standard-images \
    --filter="family~2604" --format='value(family)' | sort -u >&2
  echo "Rerun with IMAGE_FAMILY=<one of them>." >&2
  exit 1
fi
exists gcloud compute instances describe "$VM_NAME" --zone="$ZONE" ||
  gcloud compute instances create "$VM_NAME" --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" --subnet="$SUBNET" \
    --image-family="$IMAGE_FAMILY" --image-project=ubuntu-os-cloud \
    --boot-disk-size="${DISK_SIZE_GB}GB" --boot-disk-type=pd-balanced \
    --no-service-account --no-scopes \
    --metadata=enable-oslogin=TRUE,enable-guest-attributes=TRUE \
    --shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring \
    --tags="$TAG" --labels=app=routewatch

step "Deployer service account"
SA_EMAIL="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
exists gcloud iam service-accounts describe "$SA_EMAIL" ||
  gcloud iam service-accounts create "$SA_NAME" \
    --display-name="routewatch deployer (GitHub Actions)"
for role in roles/compute.viewer roles/iap.tunnelResourceAccessor; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --condition=None \
    --member="serviceAccount:$SA_EMAIL" --role="$role" --format=none
done
# Administrator login (sudo) on this VM only.
gcloud compute instances add-iam-policy-binding "$VM_NAME" --zone="$ZONE" \
  --member="serviceAccount:$SA_EMAIL" --role=roles/compute.osAdminLogin --format=none

step "Workload Identity Federation for $REPOSITORY"
repo_json="$(curl -fsS "https://api.github.com/repos/$REPOSITORY")"
REPO_ID="$(jq -r .id <<<"$repo_json")"
OWNER_ID="$(jq -r .owner.id <<<"$repo_json")"
[[ $REPO_ID =~ ^[0-9]+$ && $OWNER_ID =~ ^[0-9]+$ ]] || {
  echo "Could not read the repository's IDs from GitHub" >&2
  exit 1
}
exists gcloud iam workload-identity-pools describe "$POOL" --location=global ||
  gcloud iam workload-identity-pools create "$POOL" --location=global \
    --display-name="GitHub Actions"
exists gcloud iam workload-identity-pools providers describe "$PROVIDER" \
  --location=global --workload-identity-pool="$POOL" ||
  gcloud iam workload-identity-pools providers create-oidc "$PROVIDER" \
    --location=global --workload-identity-pool="$POOL" \
    --display-name="$REPOSITORY" \
    --issuer-uri="https://token.actions.githubusercontent.com" \
    --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.repository_id=assertion.repository_id,attribute.ref=assertion.ref,attribute.environment=assertion.environment" \
    --attribute-condition="assertion.repository_id == '$REPO_ID' && assertion.repository_owner_id == '$OWNER_ID' && assertion.ref == 'refs/heads/main' && assertion.environment == 'production'"
# Only the production environment's jobs may act as the deployer.
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --role=roles/iam.workloadIdentityUser --format=none \
  --member="principal://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL/subject/repo:$REPOSITORY:environment:production"

cat <<EOF

Done. Add these as repository variables in GitHub (Settings > Secrets and
variables > Actions > Variables). They are identifiers, not secrets.
  GCP_PROJECT_ID                  $PROJECT_ID
  GCP_ZONE                        $ZONE
  GCP_VM_NAME                     $VM_NAME
  GCP_WORKLOAD_IDENTITY_PROVIDER  projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL/providers/$PROVIDER
  GCP_DEPLOY_SERVICE_ACCOUNT      $SA_EMAIL

Next, prepare the VM (from the repository's deploy/ folder):
  gcloud compute scp vm-bootstrap.sh new-env.sh $VM_NAME: --project=$PROJECT_ID --zone=$ZONE --tunnel-through-iap
  gcloud compute ssh $VM_NAME --project=$PROJECT_ID --zone=$ZONE --tunnel-through-iap --command='sudo bash vm-bootstrap.sh'
EOF
