# Architecture Notes

This file summarizes the final system architecture and the reasoning behind the main design decisions.

## 1. High-level architecture

The system is organized into three zones:

1. **Zone 1 — Data Pipeline**
   - Starts with the StackMathQA dataset from Hugging Face.
   - Stores raw data in Amazon S3.
   - Uses AWS EMR with PySpark for preprocessing and filtering.
   - Writes cleaned train/validation/test splits back to S3.
   - Fine-tunes LLaMA 3 8B using Unsloth and QLoRA on Google Colab / Lightning AI.
   - Exports the final model as a GGUF artifact to Hugging Face.

2. **Zone 2 — AWS Deployment VPC**
   - Uses a custom VPC named `25qgkp-vpc` in `us-east-1`.
   - Deploys a public subnet for the EC2 instance.
   - Runs Ollama and OpenWebUI on the EC2 instance.
   - Ollama serves the GGUF model locally on `localhost:11434`.
   - OpenWebUI runs in Docker and exposes the browser interface on port `8080`.

3. **Zone 3 — User Access**
   - Users access the web interface through `http://<EC2_PUBLIC_IP>:8080`.
   - Administrators access the server through SSH on port `22`.
   - The Ollama API port `11434` is not exposed to the public internet.

## 2. AWS networking configuration

The final AWS networking setup used the following resources:

| Component | Value |
|---|---|
| VPC name | `25qgkp-vpc` |
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

## 3. Route table design

The public route table contained two important routes:

| Destination | Target | Purpose |
|---|---|---|
| `10.0.0.0/16` | local | Allows communication inside the VPC |
| `0.0.0.0/0` | `25qgkp-igw` | Allows internet access through the Internet Gateway |

This route design was required because the EC2 instance needed to:

- download the GGUF model artifact from Hugging Face;
- install packages and Docker dependencies;
- expose OpenWebUI to the browser through HTTP port `8080`;
- allow SSH administration through port `22`.

## 4. Security Group design

The Security Group only allowed the two ports required for the demo:

| Port | Protocol | Purpose |
|---:|---|---|
| 22 | TCP | SSH administration and terminal testing |
| 8080 | TCP | Browser access to OpenWebUI |

Port `11434` was intentionally not opened. Ollama listens on `localhost:11434`, and OpenWebUI communicates with it internally from the EC2 host/container environment. This reduces public attack surface and matches the architecture diagram.

## 5. EC2 deployment design

The EC2 instance used:

```text
Name: 25qgkp-ec2
Instance type: t3.xlarge
OS: Ubuntu 24.04
```

The `t3.xlarge` instance was chosen because the quantized GGUF model was approximately 4.9 GB, and the instance needed enough RAM to run Ollama and OpenWebUI together. A smaller instance would be cheaper, but could be tight on memory for local LLM inference.

## 6. Ollama design

Ollama was used because it can run local GGUF models with a simple HTTP API. The model was imported using a `Modelfile`:

```bash
echo 'FROM ./llama3-math-merged5-q4_k_m.gguf' > Modelfile
ollama create 25qgkp-mathqa -f Modelfile
```

The project also tested a merged4 tag:

```bash
echo 'FROM ./llama3-math-merged4-q4_k_m.gguf' > Modelfile
ollama create 25qgkp-mathqa-merged4 -f Modelfile
```

Ollama listened locally at:

```text
http://localhost:11434
```

This local-only design means the model API was not directly public.

## 7. OpenWebUI design

OpenWebUI was deployed with Docker and exposed on port `8080`:

```text
http://<EC2_PUBLIC_IP>:8080
```

OpenWebUI gave the project a simple browser-based chat interface for testing the fine-tuned math model. It connected to Ollama internally rather than requiring users to call the Ollama API directly.

## 8. Data pipeline design

The data pipeline used S3 and EMR because the StackMathQA dataset was large enough to justify distributed preprocessing. Person B's preprocessing stage:

- read the raw StackMathQA dataset;
- filtered low-quality records;
- used `question_score >= 5` as a quality threshold;
- removed duplicates;
- formatted records in an instruction/response style;
- generated EDA plots;
- produced train/validation/test splits;
- wrote outputs back to S3.

Final dataset counts:

| Split | Rows |
|---|---:|
| Raw dataset | 1,600,000 |
| Clean dataset | 360,032 |
| Train | 287,928 |
| Validation | 36,052 |
| Test | 36,052 |
| Retention | 22.5% |

The S3 `splits/train/` folder contained 27 Spark `part-*.json` files and a `_SUCCESS` marker. The `_SUCCESS` marker confirms that the Spark write job completed successfully.

## 9. Fine-tuning design

The fine-tuning work used:

- LLaMA 3 8B as the base model;
- StackMathQA cleaned data;
- QLoRA for memory-efficient fine-tuning;
- Unsloth for faster training;
- Google Colab / Lightning AI with NVIDIA L4;
- GGUF export for Ollama deployment.

The training configuration included:

```text
warmup_steps = 200
```

The fine-tuned model was expected to provide more step-by-step mathematical explanations than the base model, especially for algebra and calculus prompts.

## 10. Cost interpretation

Two cost screenshots were used:

1. **Cost Explorer** grouped by service and filtered to US East (N. Virginia).
2. **Amazon Q billing summary** showing daily usage, credits, and net paid amount.

The Amazon Q summary showed:

| Date | Usage charges | Credits applied | Net paid |
|---|---:|---:|---:|
| April 28, 2026 | `$0.27` | `-$0.27` | `$0.00` |
| April 29, 2026 | `$2.50` | `-$2.50` | `$0.00` |
| Total | `$2.77` | `-$2.77` | `$0.00` |

This means the project did generate AWS usage, but the cost was offset by AWS credits. EMR and `t3.xlarge` are normally billable services; they were not treated as permanently free resources. The reported net cost was zero because credits covered the usage during the project window.

## 11. Cleanup

After the deployment screenshots and tests were completed, temporary resources were terminated to avoid further charges. This included terminating the EC2 deployment instance and EMR cluster after their evidence screenshots were captured.
