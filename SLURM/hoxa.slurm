#!/bin/bash
#SBATCH --job-name=hoxaforumscraper
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

python hoxa_scraper.py --output ./hoxa --start-page 1 --end-page 5000 --delay 3