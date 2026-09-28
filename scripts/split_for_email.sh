#!/bin/sh
# Cut the bundle into pieces small enough for e-mail: 7,000,000 bytes each, which stays under
# 10 MB once the mail encodes it (base64 adds a third). Pieces are numbered .001, .002 ... (join
# them with cat, copy /b or 7-Zip); files already small enough (the PDFs) are copied as they are.
#
#   sh scripts/split_for_email.sh [out_dir]
set -e
cd "$(dirname "$0")/.."
PIECE=7000000
OUT=${1:-dist/email}
V=$(sed -n 's/^version = "\(.*\)"/\1/p' pyproject.toml)
VP=$(sed -n 's/^version = "\(.*\)"/\1/p' "${PROMAGG:-../promagg}/pyproject.toml")
B=osagg-$V-promagg-$VP-bundle-py311
rm -rf "$OUT"
mkdir -p "$OUT"
for part in part1-core part2-mcp; do
  split -b "$PIECE" -d -a 3 --numeric-suffixes=1 "dist/$B-$part.tar.gz" "$OUT/$B-$part.tar.gz."
done
for f in dist/docs/*.pdf; do
  [ -f "$f" ] && cp "$f" "$OUT/"
done
(cd dist && sha256sum "$B-part1-core.tar.gz" "$B-part2-mcp.tar.gz") > "$OUT/SHA256SUMS"
(cd "$OUT" && sha256sum "$B"-*.tar.gz.0* ./*.pdf 2>/dev/null | sed 's# \./# #') >> "$OUT/SHA256SUMS"
n1=$(ls "$OUT/$B-part1-core.tar.gz."* | wc -l)
n2=$(ls "$OUT/$B-part2-mcp.tar.gz."* | wc -l)
last1=$(printf "%03d" "$n1")
last2=$(printf "%03d" "$n2")
p1=$(printf "%s+" $(seq -f "$B-part1-core.tar.gz.%03g" 1 "$n1")); p1=${p1%+}
p2=$(printf "%s+" $(seq -f "$B-part2-mcp.tar.gz.%03g" 1 "$n2")); p2=${p2%+}
cat > "$OUT/README-JOIN.txt" <<EOF
osagg $V + promagg $VP bundle, cut in pieces of 7 MB for e-mail (under 10 MB each once sent)

1. Put all the pieces in one folder and join them:

   $B-part1-core.tar.gz.001 ... .$last1   ->  $B-part1-core.tar.gz   (required)
   $B-part2-mcp.tar.gz.001 ... .$last2    ->  $B-part2-mcp.tar.gz    (only for Superset's MCP service / the AI agent)

   Linux / macOS:
     cat $B-part1-core.tar.gz.0* > $B-part1-core.tar.gz
     cat $B-part2-mcp.tar.gz.0* > $B-part2-mcp.tar.gz
     sha256sum -c SHA256SUMS --ignore-missing

   Windows (cmd):
     copy /b $p1 $B-part1-core.tar.gz
     copy /b $p2 $B-part2-mcp.tar.gz
     certutil -hashfile $B-part1-core.tar.gz SHA256
   or 7-Zip: right-click the .001 piece > "Combine files".

2. Check the SHA-256 of the joined files (also in SHA256SUMS):
$(head -2 "$OUT/SHA256SUMS" | sed 's/^/     /')

3. Extract both archives into the same folder and follow INSTALL.txt inside:
     tar xzf $B-part1-core.tar.gz
     tar xzf $B-part2-mcp.tar.gz

The report and the runbook PDFs need no joining.
EOF
ls -la "$OUT"
