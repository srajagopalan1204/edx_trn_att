#!/usr/bin/env bash
# Back up Scripts/ then move selected "one-off" helpers into Scripts/OneOff/.
# New York–time timestamped backup; manifest logged.

set -euo pipefail

SCRIPTS_DIR="${SCRIPTS_DIR:-Scripts}"

# ---- One-off files to move (EDIT THIS LIST as you like) ----
ONEOFF_LIST=(
  "build_module_risk_flat.py"
  "make_module_translation.py"
  "name_translate.py"
  "name_translate_fuzzy.py"
  "name_translate_No_fuzzy.py"
  "train_report.py"
)

# Optional glob patterns (edit/leave empty as needed)
ONEOFF_GLOBS=(
  # "scratch_*.py"
  # "*_experimental.py"
  # "*_oneoff.py"
)

# ------------------------------------------------------------
DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

[[ -d "$SCRIPTS_DIR" ]] || { echo "ERROR: '$SCRIPTS_DIR' not found"; exit 1; }

TZ_NY="America/New_York"
TS="$(TZ=$TZ_NY date +%Y%m%d_%H%M)"
PREV_DIR="$SCRIPTS_DIR/Prev_${TS}"
ONEOFF_DIR="$SCRIPTS_DIR/OneOff"
MANIFEST="$PREV_DIR/housekeep_manifest_${TS}.txt"

echo "==> Backup to: $PREV_DIR (NY time $TS)"
[[ $DRY_RUN -eq 1 ]] || mkdir -p "$PREV_DIR" "$ONEOFF_DIR"

# Backup .py/.ps1 with structure; ignore previous backups/OneOff
CMD=(rsync -a --prune-empty-dirs
  --include '*/' --include '*.py' --include '*.ps1'
  --exclude 'Prev_*/' --exclude 'Prev/' --exclude 'OneOff/' --exclude '*'
  "$SCRIPTS_DIR/" "$PREV_DIR/")

if [[ $DRY_RUN -eq 1 ]]; then
  echo "[DRY] ${CMD[*]}"
else
  "${CMD[@]}"
fi

# Manifest
if [[ $DRY_RUN -eq 0 ]]; then
  {
    echo "Housekeep @ $TS America/New_York"
    echo "Backed up: $SCRIPTS_DIR  ->  $PREV_DIR"
    echo
    echo "Files backed up:"
    (cd "$PREV_DIR" && find . -type f \( -name '*.py' -o -name '*.ps1' \) | sort)
    echo
  } > "$MANIFEST"
fi

# Build list of files to move
to_move=()
for f in "${ONEOFF_LIST[@]}"; do
  [[ -e "$SCRIPTS_DIR/$f" ]] && to_move+=("$f")
done

shopt -s nullglob
for pat in "${ONEOFF_GLOBS[@]}"; do
  for hit in "$SCRIPTS_DIR"/$pat; do
    to_move+=("$(basename "$hit")")
  done
done
shopt -u nullglob

# Deduplicate
if ((${#to_move[@]})); then
  mapfile -t to_move < <(printf "%s\n" "${to_move[@]}" | awk '!seen[$0]++' | sort)
fi

echo "==> Will move ${#to_move[@]} file(s) to Scripts/OneOff/:"
printf '    %s\n' "${to_move[@]:-<none>}"

# Move
if ((${#to_move[@]})); then
  [[ $DRY_RUN -eq 1 ]] || mkdir -p "$ONEOFF_DIR"
  for rel in "${to_move[@]}"; do
    src="$SCRIPTS_DIR/$rel"
    if [[ -e "$src" ]]; then
      if [[ $DRY_RUN -eq 1 ]]; then
        echo "[DRY] mv '$src' '$ONEOFF_DIR/'"
      else
        mv "$src" "$ONEOFF_DIR/"
        echo "moved: $rel -> OneOff/" | tee -a "$MANIFEST"
      fi
    fi
  done
fi

if [[ $DRY_RUN -eq 0 ]]; then
  {
    echo
    echo "Moved to Scripts/OneOff:"
    printf '%s\n' "${to_move[@]:-<none>}"
  } >> "$MANIFEST"
fi

echo "==> Done."
echo "Backup:  $PREV_DIR"
[[ $DRY_RUN -eq 0 ]] && echo "Manifest: $MANIFEST"
echo "OneOff:  $ONEOFF_DIR"
