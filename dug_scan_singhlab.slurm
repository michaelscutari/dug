#!/bin/bash
#SBATCH -J dug_scan
#SBATCH -p singhlab
#SBATCH -N 1
#SBATCH -t 2-00:00:00
#SBATCH --mem=32G
#SBATCH -o /hpc/home/%u/dug_scan.%j.out
#SBATCH -e /hpc/home/%u/dug_scan.%j.err

set -euo pipefail

ROOT="/hpc/group/singhlab"
FINAL_OUT="/hpc/group/singhlab/dug"
SCRATCH_BASE="/scratch"
TMP_DIR="${SCRATCH_BASE}/${USER}/dug_${SLURM_JOB_ID}"

echo "Scratch base: ${SCRATCH_BASE}"
echo "Local out: ${FINAL_OUT}"
echo "Final out: ${FINAL_OUT}"
echo "SQLite tmp: ${TMP_DIR}"
echo "Scanning ${ROOT}..."

mkdir -p "${TMP_DIR}"
mkdir -p "${FINAL_OUT}"

"/hpc/home/${USER}/dug" scan \
  --root "${ROOT}" \
  --out "${FINAL_OUT}" \
  --workers 16 \
  --index-mode disk \
  --sqlite-tmp-dir "${TMP_DIR}" \
  --progress-interval 30s

DB_PATH="$(ls -t "${FINAL_OUT}"/dug-*.db | head -n 1)"
ln -sfn "$(basename "${DB_PATH}")" "${FINAL_OUT}/latest.db"
echo "Latest DB: ${FINAL_OUT}/$(basename "${DB_PATH}")"
