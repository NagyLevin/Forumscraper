#!/bin/bash
#SBATCH --job-name=gepigenyforumscraper
#SBATCH --partition=med
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1
#SBATCH --time=INFINITE
#SBATCH --chdir=.

mkdir -p logs
source .venv/bin/activate
export PYTHONUNBUFFERED=1

python gepigeny_forum_scraper_final.py --output ./Gepigeny --delay 3