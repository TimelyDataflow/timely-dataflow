#!/usr/bin/env bash
set -eou pipefail

BRANCH="$(git symbolic-ref --short HEAD)"
REVISION="$(git rev-parse --short HEAD)"
if [ -z ${SUFFIX+x} ]; then
  SUFFIX=""
else
  SUFFIX="_$SUFFIX"
fi
GP="exchange_${BRANCH}_${REVISION}$SUFFIX.gp"
PLOT="exchange_${BRANCH}_${REVISION}$SUFFIX.svg"
cargo build --release --example exchange

cat > "$GP" <<EOF
set datafile separator "\t"
set output "$PLOT"
set terminal svg
set logscale y 2
set grid

plot \\
EOF

NPROC="$(nproc)"
i=1
while ((i <= NPROC)); do
  CAPTURE="exchange_${BRANCH}_${REVISION}_$i$SUFFIX.txt"
  cargo run --release --example exchange 16500 1024 -w "$i" | tee "$CAPTURE"
  echo -n " '$CAPTURE' with lines title '$i workers'" >> "$GP"
  i=$((i * 2))
  if ((i <= NPROC)); then
    echo ", \\" >> "$GP"
  fi
done

echo "# Now, run:"
echo "  gnuplot '$GP'"
