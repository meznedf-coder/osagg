#!/bin/sh
# Build dist/osagg-<v>-promagg-<v>-bundle-py311-*: osagg + promagg wheelhouse (CPython 3.11),
# MCP service packages, AI agent and tools service, config snippets, systemd units, docs:
# one zip, one tar.gz, and the tar.gz in two parts under 30 MB.
# Needs dist/wheelhouse (osagg + deps), dist/wheelhouse-mcp (see docs/DEPLOY.md) and the promagg
# wheel ($PROMAGG, default ../promagg/dist).
set -e
cd "$(dirname "$0")/.."
V=$(sed -n 's/^version = "\(.*\)"/\1/p' pyproject.toml)
PROMAGG=${PROMAGG:-../promagg}
VP=$(sed -n 's/^version = "\(.*\)"/\1/p' "$PROMAGG/pyproject.toml")
NAME=osagg-$V-promagg-$VP-bundle
B=$(mktemp -d)/$NAME
mkdir -p "$B/wheelhouse" "$B/wheelhouse-mcp" "$B/ai" "$B/config" "$B/systemd"
cp dist/osagg-$V-py3-none-any.whl dist/wheelhouse/Events-*.whl dist/wheelhouse/opensearch_py-*.whl \
   dist/wheelhouse/duckdb-*-cp311-cp311-*.whl "$B/wheelhouse/"
cp "$PROMAGG/dist/promagg-$VP-py3-none-any.whl" "$B/wheelhouse/"
cp "$PROMAGG/README.md" "$B/promagg-README.md"
cp dist/wheelhouse-mcp/*.whl "$B/wheelhouse-mcp/"
cp tools/superset_agent.py tools/superset_tools_mcp.py deploy/bundle/ai/agent.env deploy/bundle/ai/tools.env \
   deploy/bundle/ai/catalog.yaml deploy/bundle/ai/add_saved_metrics.py "$B/ai/"
chmod 600 "$B/ai/agent.env" "$B/ai/tools.env"
cp deploy/bundle/config/* "$B/config/"
cp deploy/bundle/systemd/* "$B/systemd/"
cp deploy/bundle/install.sh deploy/bundle/INSTALL.txt CHANGES.md "$B/"
cp docs/DEPLOY.md "$B/DEPLOY.md"
tar czf "dist/$NAME-py311-linux-x86_64.tar.gz" -C "$(dirname "$B")" "$NAME"
DIST=$(pwd)/dist
(cd "$(dirname "$B")" && python3 -c "import shutil, sys; shutil.make_archive(sys.argv[1], 'zip', '.', sys.argv[2])" \
   "$DIST/$NAME-py311-linux-x86_64" "$NAME")
# the same in two parts under 30 MB (both extract into the same folder)
tar czf "dist/$NAME-py311-part1-core.tar.gz" -C "$(dirname "$B")" --exclude wheelhouse-mcp "$NAME"
tar czf "dist/$NAME-py311-part2-mcp.tar.gz" -C "$(dirname "$B")" "$NAME/wheelhouse-mcp"
rm -rf "$(dirname "$B")"
ls -la dist/$NAME-py311-*
