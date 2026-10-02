#!/bin/sh
# Install or upgrade osagg (OpenSearch) and promagg (Prometheus / Mimir) - and, with --mcp,
# the packages of Superset's MCP service - into the Python that runs Superset. pip only,
# offline, from this bundle.
#
#   ./install.sh /opt/superset/venv/bin/python          # osagg + promagg
#   ./install.sh /opt/superset/venv/bin/python --mcp    # osagg + promagg + MCP service packages
set -e
PY="$1"
if [ -z "$PY" ] || [ ! -x "$PY" ]; then
    echo "usage: $0 /path/to/superset/venv/bin/python [--mcp]"
    echo "the path is what this prints, without '#!':  head -1 \"\$(command -v superset)\""
    exit 1
fi
DIR=$(cd "$(dirname "$0")" && pwd)
"$PY" -m pip --version >/dev/null 2>&1 || "$PY" -m ensurepip --upgrade
"$PY" -m pip install --disable-pip-version-check --upgrade --no-index \
    --find-links "$DIR/wheelhouse" osagg promagg
if [ "$2" = "--mcp" ]; then
    if [ ! -d "$DIR/wheelhouse-mcp" ]; then
        echo "wheelhouse-mcp/ is missing: extract the part2-mcp archive into the same folder"; exit 1
    fi
    "$PY" -m pip install --disable-pip-version-check --no-index \
        --find-links "$DIR/wheelhouse-mcp" "fastmcp>=3.1.0,<4.0"
fi
"$PY" -m pip check --disable-pip-version-check || echo "(pip check: see above; osagg adds only osagg, duckdb, opensearch-py, Events; promagg adds only promagg)"
# a fresh pip install of Superset 6.1.0 today: Flask-Caching 2.5+ breaks its metastore cache; Flask-Limiter 4 no
# longer brings "rich" (Superset's command line imports it); "cachetools" (imported by Superset) no longer comes
"$PY" - <<'PYEOF' || true
from importlib.metadata import PackageNotFoundError, version
try:
    v = tuple(int(x) for x in version("flask-caching").split(".")[:2])
except (PackageNotFoundError, ValueError):
    v = (0, 0)
if v >= (2, 5):
    print("WARNING: Flask-Caching", version("flask-caching"), "- Superset 6.1.0 needs an older one: "
          "pip install 'flask-caching==2.3.1' (DEPLOY.md, section 2)")
for module, fix in (("rich", "'flask-limiter<4'"), ("cachetools", "cachetools")):
    try:
        __import__(module)
    except ImportError:
        print(f"WARNING: no {module!r} - Superset 6.1.0 needs it: pip install {fix} (DEPLOY.md, section 2)")
PYEOF
"$PY" -c "import osagg, promagg; print('osagg', osagg.__version__, 'and promagg', promagg.__version__, 'installed')"
echo "Next: restart the Superset web server, Celery workers and beat (INSTALL.txt, step A3)."
