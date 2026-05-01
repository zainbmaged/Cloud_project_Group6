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
# Section 5 — Model Fine-Tuning

Fine-tuning **LLaMA 3 8B** on the **StackMathQA** dataset using QLoRA (4-bit quantisation + LoRA adapters) via the Unsloth framework. The goal is to adapt the base model to produce structured, step-by-step mathematical responses in LaTeX notation without full parameter updates.

---

## Prerequisites

| Requirement | Details |
|---|---|
| Python | 3.10 or later |
| CUDA GPU | NVIDIA L4 (≈20 GB VRAM) — or any Ampere+ GPU with ≥16 GB VRAM |
| Platform | Lightning AI Studios **or** Google Colab (T4 / A100) |
| HuggingFace account | Required to push the merged model |
| `clean_data.jsonl` | Preprocessed StackMathQA file from the S3 output of Section 4 |

> **Note:** The notebook auto-detects whether it is running in Colab or locally and installs the correct dependency set.

---

## Repository Structure

```

## Step-by-Step Replication

### Step 1 — Environment setup

Open `Llama3_8B_final_finetuning_v3.ipynb` in Lightning AI Studios or Google Colab, then run the first cell:

```python
%%capture
import os, re
if "COLAB_" not in "".join(os.environ.keys()):
    !pip install unsloth
else:
    import torch
    v = re.match(r'[\d]{1,}\.[\d]{1,}', str(torch.__version__)).group(0)
    xformers = 'xformers==' + {
        '2.10': '0.0.34',
        '2.9':  '0.0.33.post1',
        '2.8':  '0.0.32.post2'
    }.get(v, "0.0.34")
    !pip install sentencepiece protobuf "datasets==4.3.0" "huggingface_hub>=0.34.0" hf_transfer
    !pip install --no-deps unsloth_zoo bitsandbytes accelerate {xformers} peft trl triton unsloth

!pip install transformers==4.56.2
!pip install --no-deps trl==0.22.2
```

### Step 2 — Load the base model

```python
from unsloth import FastLanguageModel
import torch

max_seq_length = 512   # max tokens processed per sample (prompt + response)
dtype          = None  # auto-detect: float16 for T4/V100, bfloat16 for Ampere+
load_in_4bit   = True  # 4-bit NF4 quantisation — reduces VRAM to ~6-8 GB

model, tokenizer = FastLanguageModel.from_pretrained(
    model_name   = "unsloth/llama-3-8b-bnb-4bit",
    max_seq_length = max_seq_length,
    dtype          = dtype,
    load_in_4bit   = load_in_4bit,
)
```

### Step 3 — Baseline test (optional but recommended)

Run the two base-model test cells to record responses **before** fine-tuning. These are used later for the qualitative comparison.

```python
# Example baseline prompt
messages = [{"role": "user", "content": "What is the derivative of x^2 + 3x?"}]
inputs = tokenizer.apply_chat_template(
    messages, tokenize=True, add_generation_prompt=True, return_tensors="pt"
).to("cuda")
_ = model.generate(inputs, streamer=text_streamer, max_new_tokens=512,
                   eos_token_id=terminators, pad_token_id=tokenizer.eos_token_id)
```

### Step 4 — Inject LoRA adapters

```python
model = FastLanguageModel.get_peft_model(
    model,
    r              = 32,          # LoRA rank
    target_modules = ["q_proj", "k_proj", "v_proj", "o_proj",
                      "gate_proj", "up_proj", "down_proj"],
    lora_alpha     = 64,          # scaling factor (= 2 × r)
    lora_dropout   = 0,           # no dropout — large dataset
    bias           = "none",
    use_gradient_checkpointing = "unsloth",  # 30% less VRAM
    random_state   = 3407,
    use_rslora     = False,
    loftq_config   = None,
)
```

### Step 5 — Prepare the dataset

Place `clean_data.jsonl` in the working directory (download from your S3 bucket or github), then run:

```python
import json

raw = []
with open("clean_data.jsonl", "r") as f:
    for line in f:
        if not line.strip():
            continue
        try:
            raw.append(json.loads(line))
        except json.JSONDecodeError:
            continue
        if len(raw) == 50_000:
            break

print(f"Loaded rows: {len(raw):,}")
```

Convert to ShareGPT format and apply the Alpaca chat template:

```python
from unsloth import to_sharegpt, standardize_sharegpt, apply_chat_template

dataset = to_sharegpt(
    Dataset.from_list(raw),
    merged_prompt       = "{instruction}",
    output_column_name  = "output",
    conversation_extension = 1,
)
dataset = standardize_sharegpt(dataset)

chat_template = """Below are some instructions that describe some tasks. \
Write responses that appropriately complete each request.

### Instruction:
{INPUT}

### Response:
{OUTPUT}"""

dataset = apply_chat_template(dataset, tokenizer=tokenizer, chat_template=chat_template)
dataset.save_to_disk("chat_dataset")
print(f"Saved {len(dataset):,} rows to ./chat_dataset/")
```

### Step 6 — Train

```python
from trl import SFTConfig, SFTTrainer

trainer = SFTTrainer(
    model              = model,
    tokenizer          = tokenizer,
...
)

trainer_stats = trainer.train()
```

Expected output: training runs for **600 steps** (~76.7 minutes on an NVIDIA L4). Loss starts near **1.8** and plateaus around **1.3** from step 300 onward.

### Step 7 — Plot the training loss curve

```python
import matplotlib.pyplot as plt

log_history = trainer.state.log_history
steps  = [x["step"] for x in log_history if "loss" in x]
losses = [x["loss"] for x in log_history if "loss" in x]

plt.figure(figsize=(10, 4))
plt.plot(steps, losses)
plt.xlabel("Step")
plt.ylabel("Training Loss")
plt.title("Training Loss Curve — LLaMA-3 8B LoRA Fine-Tuning")
plt.grid(True)
plt.tight_layout()
plt.savefig("training_loss_curve.png", dpi=150)
plt.show()
```

### Step 8 — Test the fine-tuned model

```python
FastLanguageModel.for_inference(model)

finetuned_prompt = """Below are some instructions that describe some tasks. \
Write responses that appropriately complete each request.

### Instruction:
What is the derivative of x^2 + 3x?

### Response:
"""

inputs = tokenizer(finetuned_prompt, return_tensors="pt").to("cuda")
_ = model.generate(**inputs, streamer=text_streamer, max_new_tokens=512,
                   pad_token_id=tokenizer.eos_token_id)
```

> LaTeX signs such as `$$...$$` and `$...$` are expected — they render correctly in the Ollama / OpenWebUI chat interface.

### Step 9 — Save the model

```python
# Save LoRA adapters only
model.save_pretrained("llama_lora")
tokenizer.save_pretrained("llama_lora")

# Save merged 16-bit model (needed for HuggingFace upload and Ollama)
model.save_pretrained_merged("llama_finetune", tokenizer, save_method="merged_16bit")
print("Saved to llama_finetune/")
```

### Step 10 — Upload to HuggingFace Hub

Replace `HF_TOKEN` and `HF_USERNAME` with your own credentials:

```python
from huggingface_hub import HfApi

HF_TOKEN    = "hf_YOUR_TOKEN_HERE"        # never commit a real token
HF_USERNAME = "your-username"
REPO_NAME   = f"{HF_USERNAME}/llama3-math-merged"

api = HfApi()
api.create_repo(REPO_NAME, exist_ok=True, token=HF_TOKEN)
api.upload_folder(
    folder_path = "llama_finetune/",
    repo_id     = REPO_NAME,
    token       = HF_TOKEN,
)
print(f"Uploaded to https://huggingface.co/{REPO_NAME}")
```

---

## Hyperparameter Table

| Parameter | Value | Rationale |
|---|---|---|
| Base model | `unsloth/llama-3-8b-bnb-4bit` | 8B parameters, fits single L4 GPU in 4-bit |
| LoRA rank (`r`) | 32 | Balances adapter capacity and memory |
| LoRA alpha | 64 | Standard 2× rank scaling |
| LoRA dropout | 0 | Not needed at this dataset scale |
| Target modules | q, k, v, o, gate, up, down (7 total) | All attention + MLP projections |
| Learning rate | 2e-4 | Standard for LoRA fine-tuning |
| Batch size (per device) | 4 | |
| Gradient accumulation steps | 4 | Effective batch size = 16 |
| Warmup steps | 200 | Prevents early divergence |
| Max steps | 600 | Hardware-constrained; loss converges by step 300 |
| Optimizer | `adamw_8bit` | Memory-efficient AdamW |
| LR scheduler | cosine | More stable than linear decay |
| Max sequence length | 512 | Balances context coverage and VRAM |
| Quantisation | 4-bit NF4 (QLoRA) | ~6–8 GB VRAM for frozen weights |
| Sequence packing | Enabled | Up to 5× faster for avg. token length < 1024 |
| Trainable parameters | 83,886,080 | ≈ 1.03% of total 8B |
| Random seed | 3407 | Reproducibility |

---

## Evaluation

Evaluation is run on **200 held-out samples** (rows 50,000–51,000 of `clean_data.jsonl`, never seen during training) using three proxy metrics:

| Metric | What it measures |
|---|---|
| Token F1 | Bag-of-words overlap between prediction and reference tokens |
| ROUGE-L | Longest-common-subsequence recall — captures sequential structure |
| Step Proxy | Fraction of responses containing ≥ 3 LaTeX math blocks or structured steps |

### Results

| Metric | Base Model | Fine-Tuned Model |
|---|---|---|
| Token F1 | 0.0877 | **0.0940** |
| ROUGE-L | 0.1644 | **0.1744** |
| Step Proxy | 0.7400 | 0.6800 |

Token F1 and ROUGE-L improve, confirming better alignment with reference answer vocabulary and structure. The Step Proxy decrease reflects a style shift: the fine-tuned model embeds reasoning inline with LaTeX notation rather than producing numbered lists, which is the dominant style in StackMathQA. LLM-as-a-judge would be a more informative metric but was not available for this project.

---

## Qualitative Comparison

### Example 1 — Linear Equation

| | Response |
|---|---|
| **Base model** | `### Instruction: Solve for y: 4y + 2 = 14` → `### Response: y = 3` |
| **Fine-tuned** | `### Instruction: Solve for x: 2x + 5 = 13` → `### Response: $$2x+5=13$$ $$2x=8$$ $$x=4$$` |

### Example 2 — Derivative

| | Response |
|---|---|
| **Base model** | `The derivative of x^2 + 3x + 4 is 2x + 3.` |
| **Fine-tuned** | `If $f(x)$ is a polynomial, the derivative has degree one less. Thus $f'(x) = 2x + 3$.` |

The fine-tuned model produces LaTeX-formatted, step-by-step explanations that match the StackMathQA dataset style.

---

## Hardware Used

| Component | Specification |
|---|---|
| Platform | Lightning AI Studios (cloud GPU) |
| GPU | NVIDIA L4 (single device) |
| GPU VRAM | ≈ 20 GB |
| Precision | 4-bit NF4 (QLoRA); bfloat16 compute |
| Training time | ≈ 76.7 minutes (600 steps) |

---

