# Deploying RouteWatch

RouteWatch runs on one Google Cloud VM as a Docker Compose project. Every change reaches it through the same pipeline: tests, an image checked end to end, your approval, then a deploy that checks live data and rolls itself back if the checks fail.

```text
push or pull request ──► lint and unit tests (3.12, 3.14) ──► integration tests
                     ──► image: build, check its contents, run the stack, smoke test
push to main, also   ──► publish ghcr.io/munguriek/routewatch@sha256:… ──► approval (production)
                     ──► deploy job: Workload Identity Federation, no keys ──► IAP tunnel to port 22
                     ──► on the VM: release.sh deploy ──► checks ──► keep, or roll back
```

On the VM (e2-standard-2, Ubuntu 26.04 LTS) the project `routewatch` runs Kafka and PostgreSQL from `compose.routewatch.yaml`, and from `compose.app.yaml` two one-off jobs (topic, then raw table and UI role) and four services: collector, raw consumer, cleaner and web UI. The UI listens on the VM's loopback only. Nothing listens on the internet: the only inbound rule is SSH from Identity-Aware Proxy (IAP).

## One-time setup

Do these in order; each step is safe to repeat.

1. **Google Cloud project.** Create a project with billing enabled, and set a budget alert under Billing → Budgets & alerts.
2. **Cloud resources.** Open Cloud Shell, upload `deploy/gcp-setup.sh` (or clone the repository), and run `bash gcp-setup.sh PROJECT_ID`. It enables the APIs and creates the network, the VM, the deployer service account and the Workload Identity provider, then prints five values and the next two commands. Defaults: region `europe-west1`, zone `europe-west1-b`, a 200 GB disk; override with, for example, `ZONE=europe-west2-b DISK_SIZE_GB=300 bash gcp-setup.sh PROJECT_ID`.
3. **The VM.** From Cloud Shell, in the repository's `deploy/` folder, run the two commands it printed. They copy `vm-bootstrap.sh` and `new-env.sh` to the VM and run the bootstrap, which installs Docker from Docker's apt repository, writes `/opt/routewatch/.env` with new secrets (mode 600, root only) and installs the disk guard.
4. **GitHub settings** (repository Settings):
   - Secrets and variables → Actions → Variables: add the five values as repository variables: `GCP_PROJECT_ID`, `GCP_ZONE`, `GCP_VM_NAME`, `GCP_WORKLOAD_IDENTITY_PROVIDER`, `GCP_DEPLOY_SERVICE_ACCOUNT`. None is a secret.
   - Environments → New environment `production`: under Deployment protection rules, add yourself as a required reviewer and leave "Prevent self-review" off; under Deployment branches and tags, choose Selected branches and add `main`.
   - Rules → Rulesets (recommended): a branch ruleset for `main` that requires a pull request, requires the status checks `Lint and unit tests (Python 3.12)`, `Lint and unit tests (Python 3.14)`, `Integration tests (Kafka and PostgreSQL)` and `Image, checked end to end`, and blocks force pushes.
5. **The first release.** Merge a pull request into `main`. The image job publishes the image as a new package, which GitHub makes private. Before approving the deployment, open the package (your profile → Packages → routewatch → Package settings) and change its visibility to Public, so the VM can pull it without a token. Then approve the waiting deployment in the run's summary.

## Releasing a change

Work on `develop`, open a pull request into `main`, and wait for the four checks. Merging starts the run again on `main`; it publishes the image (tags `sha-<commit>` and `main`; the VM uses the digest) and waits for your approval. You may reject the deployment of a change that need not reach the VM, such as documentation (the run then shows as failed): each deploy restarts the collector, which misses the few seconds of the stream while it reconnects.

On the VM, `release.sh deploy` starts the release with `docker compose up --wait`, then checks that every service stays up without restarting for 20 seconds, that the UI answers and can read PostgreSQL and reach Kafka, and that new raw messages are stored and cleaned within three minutes. If any check fails it starts the previous release again and the job fails. To return to an earlier release later, run `sudo routewatch rollback` on the VM, or re-run the deploy job of an earlier successful run.

## Operating the VM

Connect with `gcloud compute ssh routewatch-vm --zone=europe-west1-b --tunnel-through-iap` (add `--project=PROJECT_ID` if gcloud's default project is another one), then:

| Task | Command on the VM |
| --- | --- |
| Releases, containers, disk and database size | `sudo routewatch status` |
| Follow a service's log | `sudo routewatch compose logs -f --tail=100 routewatch-collector` |
| Return to the previous release | `sudo routewatch rollback` |
| Pause or resume collecting | `sudo routewatch compose stop routewatch-collector`, `sudo routewatch compose up -d` |
| Deployment history | `less /opt/routewatch/deploy.log` |

To open the web UI from your laptop, keep this running and browse to http://localhost:8765/ (use local port 8765: the UI accepts only the Host names it expects):

```sh
gcloud compute ssh routewatch-vm --zone=europe-west1-b --tunnel-through-iap -- -N -L 8765:127.0.0.1:8765
```

## Disk

The raw and clean tables grow for as long as the collector runs. The local stack measured about 454 messages a second from rrc21, about 2.8 kB each once stored and cleaned: about 4.6 GB an hour, so the 200 GB disk holds about a day and a half of the feed. RIS traffic varies, so check the rate in the first hours: run `sudo routewatch status` twice, an hour apart. Every five minutes a systemd timer runs `routewatch disk-guard`, which stops the collector once the disk is 85% full (the consumer and cleaner carry on and drain Kafka) and writes a line to `deploy.log`. A deploy starts the collector again, and the guard stops it again within five minutes.

Kafka keeps at most 2 GiB of the topic, which at this rate is a matter of hours, so do not leave the consumer stopped for long: messages it has not read by then are lost. Docker keeps at most 50 MB of logs per container.

Once a table passes a million rows, the data page shows PostgreSQL's estimates (≈) instead of exact counts, and searches on the clean tab for rare values, which read the whole table, may exceed the five-second limit.

To make room, grow the disk; this keeps the data. Run `gcloud compute disks resize routewatch-vm --zone=europe-west1-b --size=300GB`, then reboot the VM or grow the partition and file system in place (check the device with `lsblk`; usually `sudo growpart /dev/sda 1 && sudo resize2fs /dev/sda1`), and resume the collector.

## Security

- No key is stored anywhere. The deploy job exchanges GitHub's OIDC token for a short-lived token; the provider accepts only this repository (by ID), on `main`, in the `production` environment, and only that environment's jobs may act as the deployer.
- The deployer may view the project's VMs, open IAP tunnels, and log in with sudo to this VM only. SSH keys it registers expire after 30 minutes.
- Port 22 accepts IAP's range only (35.235.240.0/20); OS Login controls who may log in. The VM has no service account, so its containers can obtain no Google credentials.
- The image is public. Its build context is an allow-list (`.dockerignore`), and CI fails if `/app` holds anything but the code, or a `.env`, `data` or notebook file. The secrets exist only in `/opt/routewatch/.env` on the VM, and in a throwaway `.env` inside each CI run.
- The services run as an unprivileged user with a read-only root file system and no Linux capabilities. The UI connects as the read-only role and never receives the owner's password.

## Troubleshooting

- **The deploy job fails at authentication.** Check the five variables, that the job ran on `main` in `production`, and allow a few minutes after `gcp-setup.sh` for IAM changes to apply.
- **SSH is refused.** Run `gcloud compute ssh routewatch-vm --zone=europe-west1-b --tunnel-through-iap --troubleshoot`.
- **The VM cannot pull the image.** The package is still private; see step 5.
- **No new rows after a deploy.** Read the collector's log; RIS Live may be unreachable or limiting connections.

## Costs and removal

The VM, its disk and its external address are billed while they exist; see Google Cloud's pricing calculator for current prices. `gcloud compute instances stop routewatch-vm --zone=europe-west1-b` stops the VM's charges but not the disk's. After the assessment, `gcloud compute instances delete routewatch-vm --zone=europe-west1-b` deletes the VM and its disk, and all the data in it, permanently; deleting the project removes everything.
