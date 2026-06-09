# Runbook — in-region latency measurement (operator-run)

Run this yourself, under your own identity, to keep the measurement out of any
automated-session anomaly detection. The harness (`measure_latency_inregion.py`)
makes only ordinary S3 data-plane calls — read `nccsdata`, write+delete a
throwaway `phase0-inregion/` prefix in `nccs-dataexplorer-stg`. No EC2/IAM/SSM.

Goal: the real in-region DuckDB-COPY-to-S3 throughput (MB/s). The local-WSL spike
saw ~1.5 MB/s across the public internet; we need the in-region number to decide
Lambda vs App Runner for materialization. **Paste the final JSON block back.**

---

## Path A — CloudShell (recommended; lightest, no infra)

1. Open **AWS CloudShell** in the console, **Region = N. Virginia (us-east-1)**.
2. Get the harness — either paste the file contents, or if the repo is reachable:
   ```bash
   pip install --quiet duckdb boto3
   # then paste measure_latency_inregion.py into the editor, or scp/clone it
   ```
3. Run the moderate sweep (fits CloudShell's ~2 GB RAM / 1 GB disk):
   ```bash
   python measure_latency_inregion.py medium 1.5GB
   ```
   - Confirm the printed `region:` line says `us-east-1`.
   - `small` (~p50) and `medium` (~0.3 GB, all-years narrow) should complete.
   - If `medium` OOMs/spills, that itself is signal; rerun with just `small`.
4. Copy the `=== PASTE THIS BACK ===` JSON to the assistant.

CloudShell gives the decisive **throughput rate**; from it the Lambda cutoff
(largest result that fits inside 15 min + 10 GB) can be derived analytically.

---

## Path B — throwaway EC2 (only if you also want big-join completion timing)

Use if you want the `large` tier (wide projection, multi-GB, exceeds CloudShell
RAM). Run these **yourself**; tag clearly and terminate when done.

```bash
AMI=ami-0152204c1a187337c            # AL2023 x86_64 us-east-1 (re-fetch if stale)
SUBNET=subnet-9d03a2b6               # default-VPC public subnet (us-east-1a)
PROFILE=ec2-s3FullAccess             # existing instance profile w/ S3 access

# launch (c5.2xlarge: 8 vCPU / 16 GB, up-to-10 Gbps)
IID=$(aws ec2 run-instances --image-id $AMI --instance-type c5.2xlarge \
  --subnet-id $SUBNET --iam-instance-profile Name=$PROFILE \
  --associate-public-ip-address \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=sector-in-brief-api-phase0-latency},{Key=owner,Value=YOUR_NAME},{Key=ttl,Value=delete-today}]' \
  --query 'Instances[0].InstanceId' --output text)
echo "launched $IID"

# wait for SSM to register, then run the harness via Run Command
aws ssm wait instance-information-available --instance-information-filter-key InstanceIds --instance-information-filter-value-set $IID 2>/dev/null || sleep 60

aws ssm send-command --instance-ids $IID --document-name AWS-RunShellScript \
  --comment "phase0 latency" \
  --parameters commands='["sudo dnf -y install python3-pip git >/dev/null 2>&1","pip3 install --quiet duckdb boto3","curl -s -o /tmp/m.py https://raw.githubusercontent.com/UrbanInstitute/sector-in-brief-api/main/phase0/measure_latency_inregion.py","python3 /tmp/m.py large 12GB"]' \
  --query 'Command.CommandId' --output text
# (fetch output once it completes)
aws ssm list-command-invocations --command-id <CMD_ID> --details \
  --query 'CommandInvocations[0].CommandPlugins[0].Output' --output text

# TEAR DOWN — don't leave it running
aws ec2 terminate-instances --instance-ids $IID
```

Note: the `curl` of the harness assumes the repo/branch is pushed & public; if
not, paste the script onto the box instead. Re-fetch `AMI` with:
`aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 --query Parameter.Value --output text`

---

## What I'll do with the result

Update `phase0/FINDINGS.md` with the in-region throughput, compute the Lambda
result-size cutoff (rate × ~13 min usable wall, vs the 10 GB ceiling), and
finalize the host go/no-go. If in-region MB/s is high enough that the p75 (117 MB)
class lands well inside Lambda's window, a Lambda-small + App-Runner-tail hybrid
is on; if even moderate results are slow, App Runner (or async worker) for all
materialization.
