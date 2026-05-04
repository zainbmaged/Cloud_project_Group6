# Cloud-Based Math Q&A Chatbot using AWS, EMR, LLaMA 3, Ollama, and OpenWebUI

## 1. Project Overview

This repository contains the final CISC 886 Cloud Computing project for Group 6. The project builds an end-to-end cloud-based mathematical question-answering chatbot. The system starts from the StackMathQA dataset, preprocesses it on AWS EMR using PySpark, fine-tunes a LLaMA 3 8B model with Unsloth + QLoRA, exports the model as a GGUF artifact, and deploys it on AWS EC2 using Ollama and OpenWebUI.

The final user experience is a browser-based chat interface. The user opens OpenWebUI on port `8080`, selects the fine-tuned model, and sends math questions. OpenWebUI talks to Ollama locally on the EC2 instance through `localhost:11434`. Ollama serves the fine-tuned GGUF model.

GitHub repository:

```text
https://github.com/zainbmaged/Cloud_project_Group6.git
```

---

## 2. Final System Summary

| Component | Final value |
|---|---|
| AWS region | `us-east-1` / US East (N. Virginia) |
| Resource prefix | `25qgkp` |
| VPC | `25qgkp-vpc` |
| VPC CIDR | `10.0.0.0/16` |
| Public subnet | `25qgkp-subnet-public1-us-east-1a` |
| Public subnet CIDR | `10.0.0.0/20` |
| Availability Zone | `us-east-1a` |
| Internet Gateway | `25qgkp-igw` |
| Route table | `25qgkp-rtb-public` |
| Security group | `25qgkp-sg` |
| EC2 instance | `25qgkp-ec2` |
| EC2 instance type | `t3.xlarge` |
| EC2 operating system | Ubuntu 24.04 |
| EC2 private IP shown in screenshots | `10.0.6.215` |
| EC2 public IP | Dynamic public IP from EC2 Console; use `<EC2_PUBLIC_IP>` in commands because no Elastic IP was attached |
| Ollama local API | `localhost:11434` |
| OpenWebUI browser port | `8080` |
| Final Ollama model | `25qgkp-mathqa-merged4:latest` |
| GGUF file | `llama3-math-merged4-q4_k_m.gguf` |
| GGUF source | `https://huggingface.co/Zainb114/llama3-math-merged4-Q4_K_M-GGUF` |

Important deployment note: Ollama port `11434` was kept local to the EC2 instance. The security group exposed only SSH `22` and OpenWebUI `8080` in the final screenshots. OpenWebUI connects to Ollama internally using `http://127.0.0.1:11434`.

---

## 3. Repository Structure

The repository should contain the following structure. File names may differ slightly, but the submitted repository should keep the same logical organization. The command snippets below are the exact command patterns used during the project; replace placeholders such as `<cluster-id>` and `<EC2_PUBLIC_IP>` with the values from your AWS Console.

```text
Cloud_project_Group6/
├── README.md
├── data_preprocessing/
│   ├── 25qgkp_emr_preprocessing.py
│   └── bootstrap.sh
├── fine_tuning/
│   └── Llama3_8B_final_finetuning_v3.ipynb
├── deployment/
│   ├── ec2_ollama_openwebui_commands.md
│   └── architecture_notes.md
├── screenshots/
│   ├── 01-architecture-diagram.png
│   ├── 02-vpc.png
│   ├── 03-subnet.png
│   ├── 04-route-table.png
│   ├── 05-igw.png
│   ├── 06-security-group.png
│   ├── 07-ec2-running.png
│   ├── 08-ollama-model.png
│   ├── 09-curl-response.png
│   ├── 10-openwebui-model.png
│   ├── 11-openwebui-chat.png
│   ├── 12-amazon-q-billing.png
│   ├── 13-cost-explorer.png
│   └── section4-emr-spark/
└── report/
    └── CISC886_Group6_Final_Submission_Report.pdf
```

---

## 4. System Architecture

![description](<img width="1602" height="982" alt="01-architecture-diagram" src="https://github.com/user-attachments/assets/e93f84f8-aff8-410a-96f5-465000d05451" />)

The system is divided into three zones.

### Zone 1: Data Pipeline

```text
StackMathQA dataset
→ Amazon S3 raw data
→ AWS EMR cluster with PySpark preprocessing
→ Amazon S3 cleaned train/validation/test splits
→ Lightning AI / Google Colab fine-tuning using Unsloth + QLoRA
→ Hugging Face GGUF model artifact
```

### Zone 2: AWS Deployment VPC

```text
Custom VPC 25qgkp-vpc
→ Public subnet 25qgkp-subnet-public1-us-east-1a
→ Security group 25qgkp-sg
→ EC2 instance 25qgkp-ec2
→ Ollama local model server on localhost:11434
→ OpenWebUI Docker container on port 8080
```

### Zone 3: User Access

```text
User browser
→ http://<EC2_PUBLIC_IP>:8080
→ OpenWebUI
→ localhost:11434
→ Ollama
→ 25qgkp-mathqa-merged4:latest
```

Administrative access was done through SSH:

```text
User terminal
→ SSH port 22
→ EC2 instance
```

The architecture was intentionally kept simple because this was a temporary course deployment. EMR was used for preprocessing and then terminated. EC2 was used for final model serving and then terminated after screenshots and testing. The final architecture screenshot used in the report is `01-architecture-diagram.png` and shows Ollama as local-only on `localhost:11434` with OpenWebUI exposed on port `8080`.

---

## 5. Prerequisites

### Local prerequisites

- AWS Console access
- SSH client
- Web browser
- EC2 key pair file, for example `25qgkp-keypair.pem`
- Git

### AWS permissions needed

The AWS account must allow creation and management of:

- VPC
- Subnet
- Route table
- Internet Gateway
- Security Group
- EC2 instance
- S3 bucket and objects
- EMR cluster
- IAM roles needed for EMR

### Fine-tuning prerequisites

For the fine-tuning notebook:

- Python 3.10 or later
- CUDA GPU environment, preferably NVIDIA L4 or equivalent
- Hugging Face account and token
- Preprocessed StackMathQA data from the EMR/S3 output

---

## 6. VPC and Networking Setup

The infrastructure was created through the AWS Console, not Terraform. This was chosen because the project was temporary, screenshots were required for grading, and the deliverable allows console provisioning if the design is justified.

### 6.1 Create a new VPC

Create a VPC with:

| Setting | Value |
|---|---|
| Name | `25qgkp-vpc` |
| CIDR | `10.0.0.0/16` |
| DNS resolution | Enabled |
| DNS hostnames | Enabled |

Reason: the default VPC was not used because the project required a new custom VPC. The `/16` address range gives enough private IP space for future expansion even though the final demo used one public subnet.

### 6.2 Create the public subnet

Create a subnet with:

| Setting | Value |
|---|---|
| Name | `25qgkp-subnet-public1-us-east-1a` |
| CIDR | `10.0.0.0/20` |
| Availability Zone | `us-east-1a` |

Reason: the EC2 instance needed to be reachable from the browser for OpenWebUI and from SSH for administration. A public subnet was enough for this project. A NAT Gateway was not used because it would add cost and was not needed for the demo.

### 6.3 Create and attach the Internet Gateway

Create an Internet Gateway:

| Setting | Value |
|---|---|
| Name | `25qgkp-igw` |
| Attached VPC | `25qgkp-vpc` |

Reason: the EC2 instance needed internet access to install packages, download the GGUF model, pull the OpenWebUI Docker image, and receive browser traffic on port `8080`.

### 6.4 Create the public route table

Create route table:

| Setting | Value |
|---|---|
| Name | `25qgkp-rtb-public` |
| VPC | `25qgkp-vpc` |

Routes:

| Destination | Target | Purpose |
|---|---|---|
| `10.0.0.0/16` | local | Internal VPC traffic |
| `0.0.0.0/0` | `25qgkp-igw` | Internet access |

Associate this route table with the public subnet.

### 6.5 Create the security group

Security group:

| Setting | Value |
|---|---|
| Name | `25qgkp-sg` |
| VPC | `25qgkp-vpc` |

Inbound rules:

| Type | Protocol | Port | Source | Purpose |
|---|---|---:|---|---|
| SSH | TCP | `22` | `0.0.0.0/0` | Temporary server administration |
| Custom TCP | TCP | `8080` | `0.0.0.0/0` | Browser access to OpenWebUI |

Ollama port `11434` was not publicly exposed in the final security group. It was kept local to the EC2 instance. In production, SSH should be restricted to a trusted IP address instead of `0.0.0.0/0`.

---

## 7. Data Preprocessing with EMR and PySpark

### 7.1 Dataset

| Item | Value |
|---|---|
| Dataset | StackMathQA, 1600K subset |
| Source | `https://huggingface.co/datasets/math-ai/StackMathQA` |
| License | CC BY-SA 4.0 |
| Raw records used in preprocessing | 1,600,000 |
| Final clean records | 360,032 |
| Train split | 287,928 |
| Validation split | 36,052 |
| Test split | 36,052 |

The data is suitable for this chatbot because it contains real mathematical questions and accepted answers from Stack Exchange-style communities. Many answers contain LaTeX notation and multi-step explanations.

### 7.2 S3 layout

The S3 bucket used for the data pipeline was:

```text
s3://25qgkp-all-data/
```

Recommended layout:

```text
s3://25qgkp-all-data/raw/all.jsonl
s3://25qgkp-all-data/scripts/bootstrap.sh
s3://25qgkp-all-data/scripts/25qgkp_emr_preprocessing.py
s3://25qgkp-all-data/processed/clean_data/
s3://25qgkp-all-data/splits/train/
s3://25qgkp-all-data/splits/validation/
s3://25qgkp-all-data/splits/test/
s3://25qgkp-all-data/eda/
s3://25qgkp-all-data/logs/
```

### 7.3 EMR cluster configuration

| Parameter | Value |
|---|---|
| Cluster name | `25qgkp-emr` |
| Region | `us-east-1` |
| EMR release | `emr-7.1.0` |
| Application | Spark |
| Master node | 1 x `m5.xlarge` |
| Core nodes | 2 x `m5.xlarge` |
| Task nodes | 0 |
| Log path | `s3://25qgkp-all-data/logs/` |
| Bootstrap script | `s3://25qgkp-all-data/scripts/bootstrap.sh` |
| PySpark script | `s3://25qgkp-all-data/scripts/25qgkp_emr_preprocessing.py` |

### 7.4 Bootstrap script

```bash
#!/bin/bash
sudo pip3 install --upgrade pip
sudo pip3 install matplotlib numpy pandas pyarrow fsspec s3fs boto3
sudo pip3 install datasets --ignore-installed --no-deps
sudo pip3 install huggingface-hub tqdm requests filelock --ignore-installed
```

### 7.5 PySpark preprocessing logic

The preprocessing script performs the following steps:

1. Reads the raw JSONL dataset from S3.
2. Drops records with missing or empty question/answer fields.
3. Normalizes text while preserving mathematical notation.
4. Applies minimum and maximum length filters.
5. Keeps records with question score at least `5`.
6. Removes duplicates before splitting to reduce leakage.
7. Converts cleaned rows into instruction-following format.
8. Creates EDA statistics and plots.
9. Writes `processed/clean_data` and `splits/train`, `splits/validation`, `splits/test` back to S3.

Preprocessing settings:

| Setting | Value |
|---|---|
| Minimum question length | 40 characters |
| Maximum question length | 1,500 characters |
| Minimum answer length | 100 characters |
| Maximum answer length | 4,000 characters |
| Minimum question score | 5 |
| Split ratio | 80/10/10 |
| Random seed | 42 |

### 7.6 Run the Spark job

Example command on the EMR primary node:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  s3://25qgkp-all-data/scripts/25qgkp_emr_preprocessing.py
```

### 7.7 EDA interpretation

Three EDA checks were used.

**Plot 1: Text Length Distribution (Q vs A)**

This histogram compares question and answer character lengths across the 360,032 clean records. Questions are mostly between 200 and 700 characters, with mean 624 and median 563. Answers are longer and more spread out, with mean 764, median 624, and maximum 4,000. The 4,000-character truncation risk zone helped verify that extremely long records were controlled before tokenization.

**Plot 2: Question Score vs Data Quality Proxy**

This chart groups records by question score bucket: 5-9, 10-49, 50-199, and 200+. It shows both the row count and average answer length. Higher-scored questions tend to have longer answers, supporting the choice of score `>= 5` as a quality filter. Most retained records are in the 5-9 bucket, while higher-score buckets are smaller but generally more detailed.

**Plot 3: LaTeX Density / Tokenizer Readiness Check**

This chart measures LaTeX expression density in each question-answer pair. About 17.9% of records contain no LaTeX, while 42.2% contain 30 or more LaTeX expressions. About 31% use display-math block equations. This confirms that the dataset is strongly mathematical and that the model/tokenizer must handle LaTeX notation.

### 7.8 S3 output proof

The `splits/train/` S3 folder contained 27 Spark `part-*.json` output files. The screenshot shows files ranging from about 4.7 MB to 16.1 MB and written on April 29, 2026. The `_SUCCESS` marker file confirms that the Spark write job completed successfully.

This screenshot is important because it proves the preprocessed data was not only represented by top-level folders, but actually written as Spark output files inside the train split folder.

### 7.9 Teardown

The EMR cluster was terminated after preprocessing. This is required because EMR clusters can continue generating charges if left running.

---

## 8. Model Fine-Tuning

### 8.1 Model selection

| Item | Value |
|---|---|
| Base model | `unsloth/llama-3-8b-bnb-4bit` |
| Parameters | 8 billion |
| Trainable parameters after LoRA | 83,886,080, about 1.03% |
| License | Meta LLaMA 3 Community License |
| Fine-tuning approach | LoRA + QLoRA |
| Deployment format | GGUF, Q4_K_M |
| Final deployed model | `25qgkp-mathqa-merged4:latest` |
| Final GGUF artifact | `https://huggingface.co/Zainb114/llama3-math-merged4-Q4_K_M-GGUF` |

LLaMA 3 8B was chosen because it is below 10B parameters, has strong general reasoning ability, and fits the available GPU when loaded in 4-bit NF4 quantization. QLoRA avoids full fine-tuning and trains only adapter weights, which is more realistic for a course project.

### 8.2 Training environment

| Component | Value |
|---|---|
| Platform | Lightning AI Studios cloud GPU environment |
| GPU | NVIDIA L4 |
| VRAM | About 20 GB |
| Precision | 4-bit NF4 quantization with automatic bfloat16/float16 detection |
| Training time | About 76.7 minutes for 600 steps |

Libraries used:

| Library | Version / note | Purpose |
|---|---|---|
| Unsloth | latest | Faster LoRA training and optimized kernels |
| Transformers | 4.56.2 | Model and tokenizer loading/generation |
| TRL | 0.22.2 | `SFTTrainer` supervised fine-tuning |
| PEFT | latest | LoRA adapter injection |
| BitsAndBytes | latest | 4-bit NF4 quantization |
| Datasets | 4.3.0 | Dataset loading and preprocessing |
| ROUGE-Score | latest | ROUGE-L evaluation |

### 8.3 Hyperparameters

| Parameter | Value | Reason |
|---|---:|---|
| LoRA rank `r` | 32 | Balances quality and memory |
| LoRA alpha | 64 | Standard 2x rank scaling |
| LoRA dropout | 0 | No regularization used |
| Target modules | `q_proj`, `k_proj`, `v_proj`, `o_proj`, `gate_proj`, `up_proj`, `down_proj` | Covers attention and MLP projections |
| Learning rate | `2e-4` | Standard LoRA SFT learning rate |
| Batch size per device | 4 | Fits L4 memory |
| Gradient accumulation steps | 4 | Effective batch size 16 |
| Warmup steps | 200 | Stabilizes training and prevents early divergence |
| Max steps | 600 | Used due to time/resource constraints |
| Number of epochs | about 0.19 | 50,000 samples / effective batch 16 = about 3,125 steps per epoch; 600 steps is about 0.19 epoch |
| Optimizer | `adamw_8bit` | Saves memory |
| LR scheduler | cosine | Stable decay |
| Max sequence length | 512 | Balances context length and VRAM |
| Quantization | 4-bit QLoRA | Reduces memory usage |
| Sequence packing | Enabled | Improves efficiency for shorter samples |
| Trainable parameters | 83,886,080 | About 1.03% of 8B model |

### 8.4 Loss curve

The loss started around 1.8 and decreased to around 1.3. It mostly plateaued after about step 300, with normal small spikes. This suggests the model learned the main formatting and domain style patterns from the selected training subset within the 600-step run.

### 8.5 Base model vs fine-tuned model examples

#### Example 1: Linear equation

| Model | Response |
|---|---|
| Base model | `### Instruction:`<br>`Solve for x: 2x + 5 = 13`<br>`### Response:`<br>`x = 4` |
| Fine-tuned model | `### Instruction:\n Solve for x: 2x + 5 = 13\n\n`<br>`### Response:`<br>`$$2x+5=13$$`<br>`$$2x=13-5$$`<br>`$$2x=8$$`<br>`$$x=8/2$$`<br>`$$x=4$$` |

The base model gives the correct answer directly. The fine-tuned model gives a more StackMathQA-style answer by showing algebra steps and using LaTeX display math.

#### Example 2: Derivative

| Model | Response |
|---|---|
| Base model | `### Instruction:`<br>`What is the derivative of x^2 + 3x?`<br>`### Response:`<br>`The derivative of x^2 + 3x is 2x + 3` |
| Fine-tuned model | `### Instruction:`<br>`What is the derivative of x^2 + 3x?`<br>`### Response:`<br>`Another way to see it is to use the following rule: If $f(x)$ is a polynomial function, then the derivative of $f(x)$ is also a polynomial function whose degree is 1 less than the degree of $f(x)$.`<br>`Thus, the derivative of $f(x) = x^2 + 3x$ is $f'(x) = 2x + 3$. <|end_of_text|>` |

The fine-tuned response is not perfect because it includes the end token in the captured text. However, it demonstrates the intended style shift: the model gives a more explanatory mathematical answer and uses LaTeX notation.

### 8.6 Evaluation

Evaluation was done on 200 held-out samples.

| Metric | Base model | Fine-tuned model | Interpretation |
|---|---:|---:|---|
| Token F1 | 0.0877 | 0.0940 | Improved token overlap with references |
| ROUGE-L | 0.1644 | 0.1744 | Improved sequence similarity |
| Step Proxy | 0.7400 | 0.6800 | Lower, likely due to inline explanation style rather than numbered steps |

These metrics are useful proxies but do not fully prove mathematical correctness. A stronger future evaluation would use more samples and an LLM-as-a-judge rubric for correctness and clarity.

---

## 9. EC2, Ollama, and Model Deployment

### 9.1 EC2 instance

| Parameter | Value |
|---|---|
| Name | `25qgkp-ec2` |
| Instance ID shown in screenshots | `i-089bdfd81e32c9908` |
| Instance type | `t3.xlarge` |
| OS | Ubuntu 24.04 |
| Private IP shown | `10.0.6.215` |
| Public IP | Dynamic public IP from EC2 Console; no Elastic IP was attached |
| Elastic IP | Not used |

`t3.xlarge` was selected because the Q4_K_M GGUF model is about 4.9 GB and Ollama needs additional memory overhead. A smaller instance may have been too tight on RAM. A larger instance would be more expensive and unnecessary for a temporary demo.

### 9.2 SSH into EC2

From Windows PowerShell:

```powershell
icacls "25qgkp-keypair.pem" /reset
icacls "25qgkp-keypair.pem" /grant:r "$($env:USERNAME):(R)"
icacls "25qgkp-keypair.pem" /inheritance:r
ssh -i "25qgkp-keypair.pem" ubuntu@<EC2_PUBLIC_IP>
```

If the instance is stopped and restarted, the public IP may change because no Elastic IP was attached.

### 9.3 Install Ollama

```bash
curl -fsSL https://ollama.com/install.sh | sh
ollama --version
curl http://localhost:11434/api/tags
```

Expected API response before loading models:

```json
{"models":[]}
```

### 9.4 Download the GGUF model

```bash
wget https://huggingface.co/Zainb114/llama3-math-merged4-Q4_K_M-GGUF/resolve/main/llama3-math-merged4-q4_k_m.gguf
ls -lh llama3-math-merged4-q4_k_m.gguf
```

### 9.5 Create the Ollama model

```bash
echo 'FROM ./llama3-math-merged4-q4_k_m.gguf' > Modelfile-merged4
ollama create 25qgkp-mathqa-merged4 -f Modelfile-merged4
ollama list
```

Expected final model:

```text
25qgkp-mathqa-merged4:latest    ...    4.9 GB
```

Note: one terminal screenshot shows the earlier `25qgkp-mathqa:latest` tag created during model-loading verification. The final OpenWebUI browser demo uses `25qgkp-mathqa-merged4:latest`, which is the tag selected in the browser screenshots.

### 9.6 Test the model through the Ollama API

```bash
curl -s http://localhost:11434/api/generate -d '{
  "model": "25qgkp-mathqa-merged4",
  "prompt": "### Question:\nWhat is the derivative of x^2? Answer in one sentence.\n\n### Response:\n",
  "stream": false,
  "options": {
    "num_predict": 40,
    "temperature": 0,
    "stop": ["This can be shown", "Let "]
  }
}' | python3 -c 'import sys,json; d=json.load(sys.stdin); print("Question: What is the derivative of x^2?\n"); print("Response:"); print(d["response"].strip())'
```

Observed output:

```text
Question: What is the derivative of x^2?

Response:
The derivative of $x^2$ is $2x$.
```

---

## 10. OpenWebUI Deployment

### 10.1 Install Docker

```bash
sudo apt update
sudo apt install -y docker.io
sudo systemctl enable docker
sudo systemctl start docker
sudo docker --version
```

### 10.2 Run OpenWebUI

```bash
sudo docker run -d \
  --name open-webui \
  --network host \
  -v open-webui:/app/backend/data \
  -e OLLAMA_BASE_URL=http://127.0.0.1:11434 \
  --restart always \
  ghcr.io/open-webui/open-webui:main
```

Verify container health:

```bash
sudo docker ps
curl http://localhost:8080
```

The `--restart always` flag satisfies the auto-start requirement for OpenWebUI. Docker was also enabled using `sudo systemctl enable docker`.

### 10.3 Browser access

Open in a browser:

```text
http://<EC2_PUBLIC_IP>:8080
```

Select model:

```text
25qgkp-mathqa-merged4:latest
```

Sample prompt used for the screenshot:

```text
Question:
What is the derivative of x^2?

Response:
```

---

## 11. Screenshot Evidence

The final report uses the following final screenshots for the infrastructure and deployment part.

| Screenshot | What it proves |
|---|---|
| `01-architecture-diagram.png` | Final three-zone architecture |
| `02-vpc.png` | Custom VPC `25qgkp-vpc` and CIDR `10.0.0.0/16` |
| `03-subnet.png` | Public subnet and association with the custom VPC |
| `04-route-table.png` | Route table with local and Internet Gateway routes |
| `05-igw.png` | Internet Gateway attached to the custom VPC |
| `06-security-group.png` | Security group with inbound SSH 22 and HTTP 8080 only |
| `07-ec2-running.png` | EC2 instance `25qgkp-ec2` running with status checks passed |
| `08-ollama-model.png` | Ollama confirms a 4.9 GB GGUF model was registered successfully on EC2 |
| `09-curl-response.png` | Ollama API response through curl |
| `10-openwebui-model.png` | OpenWebUI browser interface with final model visible |
| `11-openwebui-chat.png` | Sample browser conversation |
| `12-amazon-q-billing.png` | Daily usage charges, credits applied, and net paid amount |
| `13-cost-explorer.png` | Cost Explorer filtered by service, date range, and region |
| `sec4-s3-train-split.jpg` | Spark-generated train split part files and `_SUCCESS` marker |

---

## 12. Cost Summary

### 12.1 Billing interpretation

Two screenshots were used for cost evidence:

1. **Amazon Q billing summary** showed the daily usage charges and credits applied.
2. **AWS Cost Explorer** showed the service-level billing view filtered to the project window and US East (N. Virginia).

The important point is that the project did create usage, but the account credits covered it. Therefore, the net paid amount was `$0.00`.

### 12.2 Daily usage and credits

| Date | Usage charges | Credits applied | Net paid |
|---|---:|---:|---:|
| April 28, 2026 | $0.27 | -$0.27 | $0.00 |
| April 29, 2026 | $2.50 | -$2.50 | $0.00 |
| **Total** | **$2.77** | **-$2.77** | **$0.00** |

### 12.3 Cost Explorer service view

Cost Explorer was filtered to:

```text
Date range: 2026-04-28 to 2026-04-30
Granularity: Daily
Group by: Service
Region: US East (N. Virginia)
Cost type: Unblended costs
```

The service-level visible amounts appeared as zero after credits:

| Service shown in Cost Explorer | Project use | Visible net amount |
|---|---|---:|
| EC2-Instances | EC2 deployment host for Ollama and OpenWebUI | $0.00 |
| EC2-Other | EC2 support costs such as storage/data path items | $0.00 |
| Elastic MapReduce | EMR preprocessing cluster | $0.00 |
| S3 | Raw data, processed data, scripts, and output folders | $0.00 |
| VPC | Networking support | $0.00 |
| CloudWatch | Logs and metrics | $0.00 |
| Glue | Supporting service visibility in billing | $0.00 |
| Data Transfer | Small network transfer adjustments | -$0.00 |
| **Total visible estimate** | Selected project period after credits | **$0.00** |

This table should be read together with the daily usage table. Cost Explorer shows the visible net service-level total after credits, while the Amazon Q summary explains that usage was fully offset by credits.

### 12.4 Free-tier and normally billable components

| Architecture component | Cost category | Explanation |
|---|---|---|
| VPC, subnet, route table, security group | No direct hourly charge | These networking objects do not create compute cost by themselves |
| Internet Gateway | No direct hourly charge | The gateway has no hourly charge, but data transfer can still matter |
| EC2 `t3.xlarge` | Normally billable | Not a micro free-tier instance; used temporarily and terminated |
| EBS root volume | Potentially billable or credit/free-tier-covered | Used by the EC2 operating system, Docker, and GGUF file |
| S3 | Potentially billable or credit/free-tier-covered | Used for raw data, processed outputs, scripts, logs, and EDA |
| EMR `m5.xlarge` cluster | Normally billable | EMR service and underlying EC2 nodes are not always-free resources |
| Ollama, Docker, OpenWebUI | No AWS software charge | Open-source software; compute cost comes from EC2 |

### 12.5 Cleanup

All temporary compute resources were terminated after the required screenshots and tests.

| Resource | Final status |
|---|---|
| EMR cluster `25qgkp-emr` | Terminated |
| EC2 instance `25qgkp-ec2` | Terminated after screenshots and testing |
| OpenWebUI container | Removed with EC2 termination |
| Ollama deployment files | Removed with EC2 termination |
| S3 project bucket | Kept only as needed for final artifacts and grading |

---

## 13. Cleanup Commands

Terminate EMR from AWS CLI if needed:

```bash
aws emr terminate-clusters --cluster-ids <cluster-id> --region us-east-1
```

Stop OpenWebUI manually if the EC2 instance is still running:

```bash
sudo docker stop open-webui
sudo docker rm open-webui
```

Remove the Ollama model manually if needed:

```bash
ollama rm 25qgkp-mathqa-merged4
```

Terminate EC2 from the AWS Console:

```text
EC2 → Instances → Select 25qgkp-ec2 → Instance state → Terminate instance
```

---

## 14. Final Verification Summary

| Requirement | Final status |
|---|---|
| Custom VPC used instead of default VPC | Completed |
| VPC, subnet, route table, IGW, and security group screenshots included | Completed |
| EMR preprocessing completed with PySpark | Completed |
| S3 output files shown, including train split part files and `_SUCCESS` marker | Completed |
| EMR terminated-state evidence included | Completed |
| LLaMA 3 8B fine-tuned with QLoRA/Unsloth | Completed |
| Hyperparameter table includes learning rate, batch size, warmup steps, epochs, and LoRA values | Completed |
| Two base-vs-fine-tuned examples included | Completed |
| GGUF model deployed to EC2 with Ollama | Completed |
| curl API response shown | Completed |
| OpenWebUI browser interface shown with model name visible | Completed |
| OpenWebUI configured with `--restart always` | Completed |
| Cost Explorer and Amazon Q billing evidence included | Completed |
| EC2 and EMR cleanup documented | Completed |

---

## 15. Known Limitations

- The EC2 deployment was CPU-only, so response speed is slower than a GPU-backed deployment.
- SSH was temporarily open to `0.0.0.0/0` for class demonstration and should be restricted in production.
- No Elastic IP was used, so the public IP can change if the instance is stopped and restarted.
- The evaluation set for fine-tuning contained only 200 held-out samples, so the metrics are useful but limited.
- Current-period AWS billing data can update after the usage day, so the billing numbers are treated as best available evidence at submission time.

