# CISC 886 — Cloud Computing Project Group 6

**Institution:** Queen's University, School of Computing  
**Course:** CISC 886 — Cloud Computing  
**Region:** `us-east-1` / N. Virginia  

---

## Resource Naming

All AWS resources are prefixed with `25qgkp` according to the course naming policy.

Examples:

```text
25qgkp-vpc
25qgkp-emr
25qgkp-all-data
25qgkp-ec2
```

---

# Section 4 — Data Preprocessing with Apache Spark on EMR

This section documents the EMR-based preprocessing pipeline used for the StackMathQA dataset. The pipeline loads the raw JSONL dataset from Amazon S3, cleans and filters the records using PySpark, produces EDA figures, creates train/validation/test splits, and writes the processed outputs back to S3.

---

## 1. Prerequisites

Before running this section, make sure the following are available:

- AWS CLI configured with access to the project AWS account
- S3 access in `us-east-1`
- EMR permissions
- EC2 key pair: `25qgkp-keypair`
- Local dataset file exported as `all.jsonl`
- PySpark preprocessing script: `25qgkp_emr_preprocessing.py`
- Bootstrap script: `bootstrap.sh`

---

## 2. Download Dataset from Hugging Face and Upload Raw Dataset to S3

The StackMathQA dataset was downloaded from Hugging Face and exported locally as `all.jsonl`. The JSONL file was then uploaded to S3 as the raw input for the EMR PySpark job.

```bash
# Create S3 bucket
aws s3 mb s3://25qgkp-all-data --region us-east-1

# Upload raw dataset to S3
aws s3 cp all.jsonl s3://25qgkp-all-data/raw/all.jsonl
```

Expected raw input path:

```text
s3://25qgkp-all-data/raw/all.jsonl
```

---

## 3. Upload Preprocessing Scripts to S3

Upload the PySpark preprocessing script and bootstrap script to the project S3 bucket.

```bash
aws s3 cp 25qgkp_emr_preprocessing.py s3://25qgkp-all-data/scripts/25qgkp_emr_preprocessing.py
aws s3 cp bootstrap.sh s3://25qgkp-all-data/scripts/bootstrap.sh
```

Expected script paths:

```text
s3://25qgkp-all-data/scripts/25qgkp_emr_preprocessing.py
s3://25qgkp-all-data/scripts/bootstrap.sh
```

---

## 4. Bootstrap Script

The bootstrap script installs the Python libraries required by the preprocessing and EDA pipeline.

Create `bootstrap.sh`:

```bash
cat > /tmp/bootstrap.sh << 'EOF'
#!/bin/bash
sudo pip3 install --upgrade pip
sudo pip3 install matplotlib numpy pandas pyarrow fsspec s3fs boto3
sudo pip3 install datasets --ignore-installed --no-deps
sudo pip3 install huggingface-hub tqdm requests filelock --ignore-installed
EOF
```

Upload it to S3:

```bash
aws s3 cp /tmp/bootstrap.sh s3://25qgkp-all-data/scripts/bootstrap.sh
```

The bootstrap script installs:

```text
matplotlib
numpy
pandas
pyarrow
fsspec
s3fs
boto3
datasets
huggingface-hub
tqdm
requests
filelock
```

---

## 5. Create EMR Cluster

The preprocessing pipeline was run on an AWS EMR cluster named `25qgkp-emr`.

Cluster configuration:

| Parameter | Value |
|---|---|
| Cluster name | `25qgkp-emr` |
| Region | `us-east-1` |
| EMR release | `emr-7.1.0` |
| Application | Spark |
| Master node | 1 × `m5.xlarge` |
| Core nodes | 2 × `m5.xlarge` |
| Task nodes | 0 |
| Log path | `s3://25qgkp-all-data/logs/` |
| Key pair | `25qgkp-keypair` |

Create the EMR cluster:

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

After running the command, save the returned EMR cluster ID. It will look like:

```text
j-XXXXXXXXXXXXX
```

---

## 6. SSH Connection to EMR Master Node

Set permissions on the key pair:

```bash
chmod 400 ~/25qgkp-keypair.pem
```

Connect to the EMR master node:

```bash
ssh -i ~/25qgkp-keypair.pem hadoop@$(aws emr describe-cluster \
  --cluster-id <your-cluster-id> \
  --region us-east-1 \
  --query "Cluster.MasterPublicDnsName" \
  --output text)
```

Replace:

```text
<your-cluster-id>
```

with the actual EMR cluster ID.

---

## 7. Optional Library Verification

After connecting to the EMR master node, verify the required libraries:

```bash
python3 -c "import matplotlib, numpy, pandas, pyarrow, boto3, datasets; print('All libraries OK')"
```

Expected output:

```text
All libraries OK
```

---

## 8. Submit PySpark Preprocessing Job

Run the PySpark preprocessing script on EMR:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  s3://25qgkp-all-data/scripts/25qgkp_emr_preprocessing.py
```

The script performs:

1. Loading raw JSONL data from S3
2. Removing empty Q/A records
3. Cleaning text and normalizing LaTeX
4. Filtering records by question and answer length
5. Filtering by question score
6. Deduplicating records
7. Formatting into Alpaca schema
8. Creating EDA plots
9. Creating train/validation/test splits
10. Writing outputs back to S3

---

## 9. Preprocessing Configuration

The preprocessing script used the following key configuration:

| Setting | Value |
|---|---:|
| Minimum question length | 40 characters |
| Maximum question length | 1,500 characters |
| Minimum answer length | 100 characters |
| Maximum answer length | 4,000 characters |
| Minimum question score | 5 |
| Train split | 80% |
| Validation split | 10% |
| Test split | 10% |
| Random seed | 42 |

The quality filter keeps records with:

```text
question_score >= 5
```

This removes low-quality Stack Exchange records while preserving useful math Q&A examples.

---

## 10. Expected Outputs in S3

| Path | Description |
|---|---|
| `s3://25qgkp-all-data/raw/all.jsonl` | Raw input dataset, 1,600,000 rows |
| `s3://25qgkp-all-data/processed/clean_data/` | Cleaned Alpaca-format dataset, 360,032 rows |
| `s3://25qgkp-all-data/eda/eda_report.png` | Combined EDA report with 3 plots |
| `s3://25qgkp-all-data/splits/train/` | Training split, 287,928 rows |
| `s3://25qgkp-all-data/splits/validation/` | Validation split, 36,052 rows |
| `s3://25qgkp-all-data/splits/test/` | Test split, 36,052 rows |
| `s3://25qgkp-all-data/logs/` | EMR logs |

---

## 11. Final Dataset Counts

| Stage | Rows |
|---|---:|
| Raw dataset | 1,600,000 |
| Final clean dataset | 360,032 |
| Train split | 287,928 |
| Validation split | 36,052 |
| Test split | 36,052 |

Retention rate:

```text
360,032 / 1,600,000 = 22.5%
```

---

## 12. EDA Outputs

The EDA output was saved as:

```text
s3://25qgkp-all-data/eda/eda_report.png
```

The combined EDA report contains three subfigures:

| Subfigure | Description |
|---|---|
| Fig. 4.8a | Text length distribution for questions and answers |
| Fig. 4.8b | Question score vs data quality proxy |
| Fig. 4.8c | LaTeX density / tokenizer readiness check |

---

## 13. Data Leakage Control

Data leakage risk was reduced by applying cleaning and deduplication before splitting.

The pipeline removes:

- duplicate URL and answer ID pairs
- duplicate content hashes based on the first 200 characters of question and answer text

After deduplication, the clean dataset was split into train, validation, and test sets using a fixed random seed of `42`.

The split-count validation confirmed that the three output splits accounted for the full cleaned dataset:

```text
287,928 + 36,052 + 36,052 = 360,032
```

---

## 14. Terminate EMR Cluster

After the Spark job finished successfully, the EMR cluster was terminated to avoid extra cost.

```bash
aws emr terminate-clusters \
  --cluster-ids <your-cluster-id> \
  --region us-east-1
```

The final report includes a screenshot showing the EMR cluster status as:

```text
Terminated (User request)
```

---

## 15. Screenshots / Figures for Report

The final report should include the following Section 4 screenshots and figures:

| Figure | Description |
|---|---|
| Fig. 4.1 | EMR cluster `25qgkp-emr` in Waiting status |
| Fig. 4.2 | EMR instance groups showing Primary and Core nodes |
| Fig. 4.3 | `m5.xlarge` instance details |
| Fig. 4.4 | EMR cluster terminated status |
| Fig. 4.5 | S3 scripts folder containing preprocessing script and bootstrap script |
| Fig. 4.6 | S3 raw folder containing `all.jsonl` |
| Fig. 4.7 | CloudShell output showing preprocessing summary and split confirmation |
| Fig. 4.8 | EDA report with three subfigures |
| Fig. 4.9 | Top-level S3 bucket showing output folders |
| Fig. 4.10 | S3 `splits/train/` folder showing PySpark-generated output files |

---

## 16. Cost Summary

Approximate cost summary for Section 4:

| AWS Service | Usage | Approximate Cost |
|---|---|---:|
| EMR | `m5.xlarge × 3 nodes` for about 1.5 hours | ~$0.90 |
| S3 | `25qgkp-all-data`, about 5 GB storage | ~$0.12 |

The EMR cluster was terminated after preprocessing to avoid unnecessary cost.

---

## 17. Cleanup Confirmation

The following resources were cleaned up after completion:

| Resource | Cleanup status |
|---|---|
| EMR cluster `25qgkp-emr` | Terminated |
| Temporary EMR compute nodes | Terminated |
| S3 data bucket | Kept only if needed for final project artifacts |
| Local temporary bootstrap file | Can be removed |

---

## 18. Files to Commit to GitHub

Commit these files for Section 4:

```text
data_preprocessing/25qgkp_emr_preprocessing.py
data_preprocessing/bootstrap.sh
README.md
```

Also include the EDA image and screenshots used in the final report:

```text
figures/eda_report.png
screenshots/section4/
```
