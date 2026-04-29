# CISC 886 — Cloud Computing Project Group 6

**Institution:** Queen's University, School of Computing  
**Region:** us-east-1 (N. Virginia)
---
## Resource Naming
All AWS resources are prefixed with `25qgkp` per the course naming policy.  
Examples: `25qgkp-vpc`, `25qgkp-emr`, `25qgkp-all-data`, `25qgkp-ec2`


## Section 4 — Data Preprocessing with Apache Spark on EMR
### Download dataset from HuggingFace 
### Upload raw dataset to S3
```bash
# Create S3 bucket: 25qgkp-all-data
upload all.jsonl to the raw directory likn: 3://25qgkp-all-data --region us-east-1

# Download dataset from HuggingFace and upload to S3
aws s3 cp all.jsonl s3://25qgkp-all-data/raw/all.jsonl
```

###  Upload scripts to S3
```bash
s3://25qgkp-all-data/scripts/25qgkp_emr_preprocessing.py
s3://25qgkp-all-data/scripts/bootstrap.sh
```
### create Bootstrap Script
```bash
cat > /tmp/bootstrap.sh << 'EOF'
#!/bin/bash
sudo pip3 install --upgrade pip
sudo pip3 install matplotlib numpy pandas pyarrow fsspec s3fs boto3
sudo pip3 install datasets --ignore-installed --no-deps
sudo pip3 install huggingface-hub tqdm requests filelock --ignore-installed
EOF
aws s3 cp /tmp/bootstrap.sh s3://25qgkp-all-data/scripts/bootstrap.sh
```
A bootstrap script (bootstrap.sh) was attached to the cluster to install the required Python libraries, including matplotlib, numpy, pandas, pyarrow, fsspec, s3fs, boto3, datasets, huggingface-hub, tqdm, requests, and filelock before the Spark job began.

###  Create EMR cluster
```bash
aws emr create-cluster \
  --name "25qgkp-emr" \
  --release-label emr-7.1.0 \
  --applications Name=Spark \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge \
    InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.xlarge \
  --bootstrap-actions Name="Install Python libs",Path=s3://25qgkp-all-data/scripts/bootstrap.sh \
  --log-uri s3://25qgkp-all-data/logs/ \
  --region us-east-1 \
  --ec2-attributes KeyName=25qgkp-keypair \
  --use-default-roles
```
### ssh connection EMR

chmod 400 ~/25qgkp-keypair.pem
ssh -i ~/25qgkp-keypair.pem hadoop@$(aws emr describe-cluster \
  --cluster-id <your-cluster-id> \
  --region us-east-1 \
  --query "Cluster.MasterPublicDnsName" \
  --output text)

### Step 4 — Submit PySpark job
```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  s3://25qgkp-all-data/scripts/25qgkp_emr_preprocessing.py
```

### Step 5 — Terminate cluster after job completes
```bash
aws emr terminate-clusters \
  --cluster-ids <your-cluster-id> \
  --region us-east-1
```

### Expected outputs in S3
| Path | Description |
|---|---|
| `s3://25qgkp-all-data/raw/all.jsonl` | Raw input (1,600,000 rows) |
| `s3://25qgkp-all-data/processed/clean_data/` | Clean data (360,032 rows) |
| `s3://25qgkp-all-data/eda/eda_report.png` | EDA report (3 plots) |
| `s3://25qgkp-all-data/splits/train/` | Train split (287,928 rows) |
| `s3://25qgkp-all-data/splits/validation/` | Validation split (36,052 rows) |
| `s3://25qgkp-all-data/splits/test/` | Test split (36,052 rows) |

---
## Cost Summary

| AWS Service | Usage | Approx. Cost |
|---|---|---|
| EMR (m5.xlarge × 3 nodes) | ~1.5 hours | ~$0.90 |
| S3 (25qgkp-all-data) | ~5 GB storage | ~$0.12 |
