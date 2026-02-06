#!/bin/bash
#SBATCH -p singhlab
#SBATCH -J dug_scan
#SBATCH -o /hpc/home/mas296/dug_scan.%j.out
#SBATCH -e /hpc/home/mas296/dug_scan.%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --time=48:00:00

set -u
umask 002

SCRATCH_BASE="/scratch"
LOCAL_OUT="$SCRATCH_BASE/mas296/dug_$SLURM_JOB_ID"
FINAL_OUT="/hpc/group/singhlab/dug"

mkdir -p "$LOCAL_OUT" "$FINAL_OUT"

echo "Scratch base: $SCRATCH_BASE"
echo "Local out: $LOCAL_OUT"
echo "Final out: $FINAL_OUT"

time ~/dug scan --root /hpc/group/singhlab --out "$LOCAL_OUT" --workers 16 --verbose

LATEST_DB=$(ls -t "$LOCAL_OUT"/dug-*.db 2>/dev/null | head -n1 || true)
if [[ -z "$LATEST_DB" ]]; then
  echo "No database produced; skipping copy."
  exit 1
fi

cp -av "$LOCAL_OUT"/dug-*.db "$FINAL_OUT"/
ln -sfn "$(basename "$LATEST_DB")" "$FINAL_OUT/latest.db"
