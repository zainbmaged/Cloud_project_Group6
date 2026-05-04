# EC2, Ollama, and OpenWebUI Commands

This file matches the deployment commands documented in the final report. It focuses on the EC2/Ollama/OpenWebUI part of the project and uses placeholders only where the value is dynamic, such as the EC2 public IP or a temporary Hugging Face download URL.

Project deployment target:

| Item | Value |
|---|---|
| EC2 instance | `25qgkp-ec2` |
| Instance type | `t3.xlarge` |
| Operating system | Ubuntu 24.04 |
| Public access ports | SSH `22`, HTTP `8080` |
| Ollama API | `localhost:11434` only |
| OpenWebUI | Docker container, browser access on `8080` |

> Important: port `11434` was not opened in the AWS Security Group. Ollama was used locally from inside EC2. OpenWebUI connected to Ollama internally.

---

## 1. SSH into the EC2 instance

Use the current EC2 public IP from the AWS Console. The public IP may change if no Elastic IP is attached.

```bash
ssh -i "25qgkp-keypair.pem" ubuntu@<EC2_PUBLIC_IP>
```

The final screenshots show the EC2 instance `25qgkp-ec2` running in `us-east-1a` with private IP `10.0.6.215`. The public IP shown in the screenshot was `54.236.55.121`, but the README keeps `<EC2_PUBLIC_IP>` because public IPs are dynamic.

---

## 2. Install and verify Ollama

```bash
curl -fsSL https://ollama.com/install.sh | sh
ollama --version
curl http://localhost:11434/api/tags
```

Observed verification during the project:

```text
ollama version is 0.22.0
```

Before importing a local model, the local API returned an empty model list:

```json
{"models":[]}
```

---

## 3. Download the GGUF model artifact

The project model artifacts were stored on Hugging Face. During the live EC2 work, the actual `wget` command followed a temporary Hugging Face/Xet signed URL, which expires. For a reproducible README command, use the stable Hugging Face resolve URL.

Final merged4 model used in the OpenWebUI demo:

```bash
wget -O llama3-math-merged4-q4_k_m.gguf \
  https://huggingface.co/Zainb114/llama3-math-merged4-Q4_K_M-GGUF/resolve/main/llama3-math-merged4-q4_k_m.gguf
```

Earlier merged5 artifact tested during deployment:

```bash
wget -O llama3-math-merged5-q4_k_m.gguf \
  https://huggingface.co/Zainb114/llama3-math-merged5-Q4_K_M-GGUF/resolve/main/llama3-math-merged5-q4_k_m.gguf
```

The screenshot evidence shows the GGUF file size was about `4.58G` downloaded and `4.9 GB` after Ollama registration.

---

## 4. Register the GGUF model in Ollama

### 4.1 Initial model registration shown in the terminal screenshot

This command sequence appears in the Ollama terminal evidence. It proves that the downloaded GGUF file was successfully imported into Ollama.

```bash
echo 'FROM ./llama3-math-merged5-q4_k_m.gguf' > Modelfile
ollama create 25qgkp-mathqa -f Modelfile
ollama list
```

Observed output evidence:

```text
NAME                    ID              SIZE      MODIFIED
25qgkp-mathqa:latest    59733ca3c37b    4.9 GB    Less than a second ago
```

### 4.2 Final merged4 model used in OpenWebUI

The final browser demo used the improved merged4 model tag visible in OpenWebUI.

```bash
cat > Modelfile << 'EOF'
FROM ./llama3-math-merged4-q4_k_m.gguf
EOF

ollama create 25qgkp-mathqa-merged4 -f Modelfile
ollama list
```

Expected model tag:

```text
25qgkp-mathqa-merged4:latest
```

---

## 5. Test the model locally through the Ollama API

This is the exact curl test documented in the final report. It calls Ollama through `localhost:11434`, then formats the JSON response using Python.

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

Observed clean output:

```text
Question: What is the derivative of x^2?

Response:
The derivative of $x^2$ is $2x$.
```

---

## 6. Install Docker

The final report documents the lightweight Docker installation path used for the EC2 demo.

```bash
sudo apt update
sudo apt install -y docker.io
sudo systemctl enable docker
sudo systemctl start docker
```

Verify Docker:

```bash
sudo docker --version
sudo docker ps
```

---

## 7. Run OpenWebUI with Docker

This is the Docker command used in the final report. It uses host networking so the OpenWebUI container can reach Ollama at `127.0.0.1:11434` on the EC2 host.

```bash
sudo docker run -d \
  --name open-webui \
  --network host \
  -v open-webui:/app/backend/data \
  -e OLLAMA_BASE_URL=http://127.0.0.1:11434 \
  --restart always \
  ghcr.io/open-webui/open-webui:main
```

Check the container:

```bash
sudo docker ps
```

Expected evidence:

```text
CONTAINER ID   IMAGE                                COMMAND           STATUS                    NAMES
...            ghcr.io/open-webui/open-webui:main   "bash start.sh"   Up ... (healthy)          open-webui
```

Because `--network host` was used, Docker may not show a `0.0.0.0:8080->8080/tcp` mapping in the `PORTS` column. This matches the project screenshot where the OpenWebUI container was healthy but the `PORTS` column was empty.

---

## 8. Verify OpenWebUI locally and from the browser

Local EC2 check:

```bash
curl http://localhost:8080
```

Browser URL:

```text
http://<EC2_PUBLIC_IP>:8080
```

The browser evidence showed the model:

```text
25qgkp-mathqa-merged4:latest
```

---

## 9. Security Group rules used

Only these inbound rules were needed:

```text
SSH 22        -> terminal administration and curl testing
HTTP 8080     -> browser access to OpenWebUI
```

No inbound rule was created for `11434` because Ollama was only used locally by OpenWebUI and curl tests inside EC2.

---

## 10. Cleanup

After screenshots and tests were completed, temporary compute resources were terminated to avoid extra charges.

Terminate EC2 from the AWS Console, or use:

```bash
aws ec2 terminate-instances --instance-ids <INSTANCE_ID> --region us-east-1
```

Optional local cleanup before termination:

```bash
sudo docker stop open-webui
sudo docker rm open-webui
ollama list
ollama rm 25qgkp-mathqa
ollama rm 25qgkp-mathqa-merged4
```
