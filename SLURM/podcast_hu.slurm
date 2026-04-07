#!/bin/bash
#SBATCH --job-name=podcast_hu_beast_uploader
#SBATCH --partition=med
#SBATCH --output=/home/szabol/beastAuto/logs/podcast_hu_%j.out
#SBATCH --error=/home/szabol/beastAuto/logs/podcast_hu_%j.err
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1
#SBATCH --time=INFINITE
#SBATCH --chdir=.

# logs mappa
mkdir -p /home/szabol/beastAuto/logs

# venv aktiválás
source /home/szabol/beastAuto/.venv/bin/activate

# unbuffered output
export PYTHONUNBUFFERED=1

# futtatás (DEFAULT workers › 2)
python main.py --workers 1