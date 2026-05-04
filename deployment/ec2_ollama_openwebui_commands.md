# EC2, Ollama, and OpenWebUI Commands

This file documents the deployment commands used for the AWS EC2 model-serving part of the project. Replace placeholder values such as `<EC2_PUBLIC_IP>`, `<KEY_FILE.pem>`, and `<HUGGING_FACE_GGUF_URL>` with the values from your own AWS/Hugging Face environment.

## 1. SSH into the EC2 instance

The EC2 instance used for deployment was named `25qgkp-ec2` and ran Ubuntu 24.04. The public IP can change if no Elastic IP is attached, so use the current EC2 public IP from the AWS console.

```bash
ssh -i <KEY_FILE.pem> ubuntu@<EC2_PUBLIC_IP>
```

Example from the project screenshots:

```bash
ssh -i <KEY_FILE.pem> ubuntu@54.236.55.121
```

## 2. Verify Ollama installation

Ollama was installed directly on the EC2 instance and listened locally on port `11434`.

```bash
ollama --version
curl http://localhost:11434/api/tags
```

Expected result before importing the model:

```json
{"models":[]}
```

## 3. Download the GGUF model artifact

The final fine-tuned/merged GGUF artifact was hosted on Hugging Face. The EC2 instance downloaded it using `wget`.

```bash
wget -O llama3-math-merged5-q4_k_m.gguf "<HUGGING_FACE_GGUF_URL>"
```

The project also tested a merged4 artifact:

```bash
wget -O llama3-math-merged4-q4_k_m.gguf "<HUGGING_FACE_MERGED4_GGUF_URL>"
```

## 4. Register the GGUF model in Ollama

Create an Ollama `Modelfile` that points to the local GGUF file.

```bash
echo 'FROM ./llama3-math-merged5-q4_k_m.gguf' > Modelfile
ollama create 25qgkp-mathqa -f Modelfile
ollama list
```

For the merged4 test model:

```bash
echo 'FROM ./llama3-math-merged4-q4_k_m.gguf' > Modelfile
ollama create 25qgkp-mathqa-merged4 -f Modelfile
ollama list
```

Expected evidence from `ollama list`:

```text
NAME                            ID              SIZE      MODIFIED
25qgkp-mathqa-merged4:latest    ...             4.9 GB    ...
25qgkp-mathqa:latest            ...             4.9 GB    ...
```

## 5. Test the model locally using the Ollama API

Ollama was tested locally through `localhost:11434`. This confirms that port `11434` only needs to be reachable inside the EC2 instance and does not need to be exposed in the AWS Security Group.

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

Expected clean output:

```text
Question: What is the derivative of x^2?

Response:
The derivative of $x^2$ is $2x$.
```

## 6. Install Docker on EC2

Docker was required to run OpenWebUI.

```bash
sudo apt update
sudo apt install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo $VERSION_CODENAME) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo docker --version
```

## 7. Run OpenWebUI with Docker

OpenWebUI was exposed to the browser on port `8080`. It connected to Ollama using the EC2 host network path to `localhost:11434`.

```bash
sudo docker run -d \
  --name open-webui \
  --restart always \
  -p 8080:8080 \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  --add-host=host.docker.internal:host-gateway \
  -v open-webui:/app/backend/data \
  ghcr.io/open-webui/open-webui:main
```

Check container status:

```bash
sudo docker ps
```

Expected result:

```text
CONTAINER ID   IMAGE                                STATUS                    NAMES
...            ghcr.io/open-webui/open-webui:main   Up ... (healthy)          open-webui
```

## 8. Access OpenWebUI from the browser

Open the following URL in a browser:

```text
http://<EC2_PUBLIC_IP>:8080
```

Example from the project screenshots:

```text
http://54.236.55.121:8080
```

The OpenWebUI interface should show the deployed Ollama model, for example:

```text
25qgkp-mathqa-merged4:latest
```

## 9. Security Group ports required

Only these inbound rules were needed:

```text
SSH 22        -> for administration and testing through terminal
HTTP 8080     -> for browser access to OpenWebUI
```

Port `11434` was not opened publicly because Ollama was accessed locally by OpenWebUI inside the EC2 instance.

## 10. Cleanup commands

After screenshots and tests were completed, temporary compute resources were terminated/stopped to avoid extra charges.

Stop and remove OpenWebUI container if needed:

```bash
sudo docker stop open-webui
sudo docker rm open-webui
```

List local Ollama models:

```bash
ollama list
```

Remove a local Ollama model if needed:

```bash
ollama rm 25qgkp-mathqa
ollama rm 25qgkp-mathqa-merged4
```

Terminate EC2 from the AWS console or with AWS CLI:

```bash
aws ec2 terminate-instances --instance-ids <INSTANCE_ID> --region us-east-1
```
