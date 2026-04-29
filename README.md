# Cloud_project_Group6
---
## Resource Naming
All AWS resources are prefixed with `25qgkp` per the course naming policy.  
Examples: `25qgkp-vpc`, `25qgkp-emr`, `25qgkp-all-data`, `25qgkp-ec2`


## Section 4 — Data Preprocessing with Apache Spark on EMR

### Step 1 — Upload raw dataset to S3
```bash
# Create S3 bucket
aws s3 mb s3://25qgkp-all-data --region us-east-1

# Download dataset from HuggingFace and upload to S3
aws s3 cp all.jsonl s3://25qgkp-all-data/raw/all.jsonl
```

### Step 2 — Upload scripts to S3
```bash
s3://25qgkp-all-data/scripts/25qgkp_emr_preprocessing.py
s3://25qgkp-all-data/scripts/bootstrap.sh
```

### Step 3 — Create EMR cluster
```bash
aws emr create-cluster \
  --name "25qgkp-emr" \
  --release-label emr-7.1.0 \
  --applications Name=Spark \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge \
    InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.xlarge \
  --bootstrap-actions \
    Path=s3://25qgkp-all-data/scripts/bootstrap.sh \
  --log-uri s3://25qgkp-all-data/logs/ \
  --region us-east-1 \
  --ec2-attributes KeyName=25qgkp-keypair
```

### Step 4 — Submit PySpark job
```bash
aws emr add-steps \
  --cluster-id <your-cluster-id> \
  --steps Type=Spark,Name="25qgkp-preprocessing",\
ActionOnFailure=CONTINUE,\
Args=[s3://25qgkp-all-data/scripts/25qgkp_emr_preprocessing.py] \
  --region us-east-1
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
