#!/bin/bash
#SBATCH --job-name=atalon_beast_uploader
#SBATCH --partition=med
#SBATCH --output=/home/szabol/beastAuto/logs/%x_%j.out
#SBATCH --error=/home/szabol/beastAuto/logs/%x_%j.err
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1
#SBATCH --time=INFINITE
#SBATCH --chdir=.

# logs mappa létrehozása
mkdir -p /home/szabol/beastAuto/logs

# venv aktiválása
source /home/szabol/beastAuto/.venv/bin/activate

# unbuffered python output
export PYTHONUNBUFFERED=1

# futtatás
python main.py