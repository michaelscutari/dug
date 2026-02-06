#!/bin/bash
#SBATCH -p singhlab
#SBATCH -J dug_scan_test
#SBATCH -o /hpc/home/mas296/dug_scan_test.%j.out
#SBATCH -e /hpc/home/mas296/dug_scan_test.%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --time=01:00:00

SCRATCH_BASE="${SLURM_TMPDIR:-${TMPDIR:-/tmp}}"
LOCAL_OUT="$SCRATCH_BASE/dug_test_$SLURM_JOB_ID"
FINAL_OUT="/hpc/home/mas296/dug-test"

mkdir -p "$LOCAL_OUT" "$FINAL_OUT"

~/dug scan --root /hpc/home/mas296 --out "$LOCAL_OUT" --workers 8 --verbose

LATEST_DB=$(ls -t "$LOCAL_OUT"/dug-*.db 2>/dev/null | head -n1 || true)
if [[ -z "$LATEST_DB" ]]; then
  echo "No database produced; skipping copy."
  exit 1
fi

cp -av "$LOCAL_OUT"/dug-*.db "$FINAL_OUT"/
ln -sfn "$(basename "$LATEST_DB")" "$FINAL_OUT/latest.db"
