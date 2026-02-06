#!/bin/bash
#SBATCH -J dug_home
#SBATCH -p singhlab
#SBATCH -N 1
#SBATCH -t 02:00:00
#SBATCH --mem=32G
#SBATCH -o /hpc/home/%u/dug_scan_home.%j.out
#SBATCH -e /hpc/home/%u/dug_scan_home.%j.err

set -euo pipefail

ROOT="/hpc/home/${USER}"
FINAL_OUT="/hpc/home/${USER}/dug-home"
SCRATCH_BASE="/scratch"
JOB_DIR="${SCRATCH_BASE}/${USER}/dug_${SLURM_JOB_ID}"

echo "Scratch base: ${SCRATCH_BASE}"
echo "Local out: ${JOB_DIR}"
echo "Final out: ${FINAL_OUT}"
echo "Scanning ${ROOT}..."

mkdir -p "${JOB_DIR}"
mkdir -p "${FINAL_OUT}"

"/hpc/home/${USER}/dug" scan \
  --root "${ROOT}" \
  --out "${JOB_DIR}" \
  --workers 16 \
  --index-mode disk \
  --sqlite-tmp-dir "${JOB_DIR}" \
  --progress-interval 30s

DB_PATH="$(ls -t "${JOB_DIR}"/*.db | head -n 1)"

cp -f "${DB_PATH}" "${FINAL_OUT}/"
ln -sfn "$(basename "${DB_PATH}")" "${FINAL_OUT}/latest.db"

echo "Copied DB to ${FINAL_OUT}/$(basename "${DB_PATH}")"
