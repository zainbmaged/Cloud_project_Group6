# Architecture Notes

This file summarizes the final architecture used in the report and README. It is written to match the final architecture diagram and the latest AWS screenshots.

---

## 1. System overview

The system was organized into three zones:

1. **Zone 1 — Data Pipeline**
   - The StackMathQA dataset was taken from Hugging Face.
   - Raw data was stored in Amazon S3.
   - AWS EMR with PySpark was used for preprocessing and EDA.
   - Cleaned train/validation/test splits were written back to S3.
   - The model was fine-tuned using Unsloth and QLoRA.
   - The final model was exported as a GGUF artifact and uploaded to Hugging Face.

2. **Zone 2 — AWS Deployment VPC**
   - A custom VPC hosted the EC2 deployment.
   - The EC2 instance ran Ollama and OpenWebUI.
   - Ollama listened locally on `localhost:11434`.
   - OpenWebUI ran in Docker and was accessed through HTTP port `8080`.

3. **Zone 3 — User Access**
   - Users accessed the chat UI through `http://<EC2_PUBLIC_IP>:8080`.
   - Administrators used SSH on port `22`.
   - The Ollama API port `11434` was not exposed to the public internet.

---

## 2. Final AWS infrastructure values

These values must match the final report text and screenshots.

| Component | Final value |
|---|---|
| VPC | `25qgkp-vpc` |
| VPC CIDR | `10.0.0.0/16` |
| Region | `us-east-1` / US East (N. Virginia) |
| Public subnet | `25qgkp-subnet-public1-us-east-1a` |
| Subnet CIDR | `10.0.0.0/20` |
| Availability Zone | `us-east-1a` |
| Route table | `25qgkp-rtb-public` |
| Internet Gateway | `25qgkp-igw` |
| Security Group | `25qgkp-sg` |
| EC2 instance | `25qgkp-ec2` |
| EC2 type | `t3.xlarge` |
| EC2 OS | Ubuntu 24.04 |
| Private IP shown | `10.0.6.215` |
| Public IP shown in screenshot | `54.236.55.121` |

The README uses `<EC2_PUBLIC_IP>` instead of hardcoding the public IP because EC2 public IPs can change when no Elastic IP is attached.

---

## 3. Route table design

The public route table `25qgkp-rtb-public` contained:

| Destination | Target | Purpose |
|---|---|---|
| `10.0.0.0/16` | local | Internal VPC communication |
| `0.0.0.0/0` | `25qgkp-igw` | Internet access through the Internet Gateway |

This was required because the EC2 instance needed to download packages/model artifacts and serve OpenWebUI through the browser.

---

## 4. Security Group design

The Security Group `25qgkp-sg` allowed only:

| Port | Protocol | Purpose |
|---:|---|---|
| 22 | TCP | SSH administration |
| 8080 | TCP | OpenWebUI browser access |

Port `11434` was intentionally not opened. Ollama was local-only:

```text
localhost:11434
```

This design reduced the public attack surface. External users accessed the system through OpenWebUI on port `8080`, while OpenWebUI connected internally to Ollama.

---

## 5. EC2, Ollama, and OpenWebUI relationship

The EC2 instance `25qgkp-ec2` hosted both services:

```text
EC2 host
├── Ollama
│   └── localhost:11434
└── OpenWebUI Docker container
    └── browser interface on port 8080
```

OpenWebUI was launched using Docker host networking:

```bash
sudo docker run -d \
  --name open-webui \
  --network host \
  -v open-webui:/app/backend/data \
  -e OLLAMA_BASE_URL=http://127.0.0.1:11434 \
  --restart always \
  ghcr.io/open-webui/open-webui:main
```

Because host networking was used, the container could reach Ollama through `127.0.0.1:11434`, and users could reach OpenWebUI through:

```text
http://<EC2_PUBLIC_IP>:8080
```

---

## 6. Why `t3.xlarge` was used

The selected EC2 instance was `t3.xlarge` with 4 vCPUs and 16 GB RAM. The GGUF model was about 4.9 GB, and Ollama needs extra memory during inference. A smaller instance could be cheaper, but it would be more likely to run out of memory or respond very slowly during the demo.

---

## 7. Data pipeline notes

The preprocessing pipeline used:

| Stage | Tool |
|---|---|
| Raw data storage | Amazon S3 |
| Distributed preprocessing | AWS EMR + PySpark |
| Cleaned output storage | Amazon S3 |
| Fine-tuning | Unsloth + QLoRA |
| Model artifact hosting | Hugging Face |
| Local deployment | Ollama + GGUF |
| Browser UI | OpenWebUI |

Final dataset counts:

| Dataset stage | Rows |
|---|---:|
| Raw dataset | 1,600,000 |
| Clean dataset | 360,032 |
| Train split | 287,928 |
| Validation split | 36,052 |
| Test split | 36,052 |
| Retention | 22.5% |

The quality filter used:

```text
question_score >= 5
```

The S3 `splits/train/` folder contained 27 Spark `part-*.json` output files and a `_SUCCESS` marker. The `_SUCCESS` marker confirms that the Spark write job completed successfully.

---

## 8. Fine-tuning notes

Person C's fine-tuning used:

- LLaMA 3 8B as the base model;
- StackMathQA cleaned data;
- QLoRA for memory-efficient fine-tuning;
- Unsloth for faster training;
- Google Colab / Lightning AI with NVIDIA L4;
- GGUF export for Ollama deployment.

The required warmup value was:

```text
warmup_steps = 200
```

The fine-tuned model was expected to produce more step-by-step mathematical explanations than the base model.

---

## 9. Cost notes

The project used two billing views:

1. **Cost Explorer** filtered to the project period, grouped by service, and limited to US East (N. Virginia).
2. **Amazon Q billing summary** showing daily usage charges, credits applied, and net paid.

Cost values from the Amazon Q billing summary:

| Date | Usage charges | Credits applied | Net paid |
|---|---:|---:|---:|
| April 28, 2026 | `$0.27` | `-$0.27` | `$0.00` |
| April 29, 2026 | `$2.50` | `-$2.50` | `$0.00` |
| Total | `$2.77` | `-$2.77` | `$0.00` |

The system did generate AWS usage, but it was offset by AWS credits. EMR and `t3.xlarge` are normally billable resources; they were not treated as permanently free services.

---

## 10. Cleanup

After screenshots and testing, the temporary compute resources were terminated to avoid further charges. This included the EC2 deployment instance and the EMR cluster. S3 artifacts were kept only as needed for submission evidence.
