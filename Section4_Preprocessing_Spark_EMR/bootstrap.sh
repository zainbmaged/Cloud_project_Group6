#!/bin/bash
sudo pip3 install --upgrade pip
sudo pip3 install matplotlib numpy pandas pyarrow fsspec s3fs boto3
sudo pip3 install datasets --ignore-installed --no-deps
sudo pip3 install huggingface-hub tqdm requests filelock --ignore-installed
