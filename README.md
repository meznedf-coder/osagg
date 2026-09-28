# osagg 0.2.6: binaries

Built from tag `v0.2.6`. The code is on `main`; this branch only holds the files to install.
Each version has its own branch `binaries-<version>`; the branches of older versions are kept.

| file | what |
|---|---|
| `osagg-0.2.6/osagg-0.2.6-promagg-0.2.1-bundle-py311-linux-x86_64.zip` | offline install for Superset on Python 3.11, Linux x86_64: osagg and promagg wheels with their dependencies, the MCP dependencies, the MCP tools service and agent, `install.sh`, `INSTALL.txt`, `DEPLOY.md`, config snippets, systemd units |
| `osagg-0.2.6/osagg-0.2.6-py3-none-any.whl` | the osagg wheel alone (pure Python), for an online install |
| `osagg-0.2.6/SHA256SUMS` | checksums |

Download: open the file on GitHub, then **Download raw file**; check it with `sha256sum -c SHA256SUMS`.
Install: unzip, then follow `INSTALL.txt` (pip only, into Superset's virtualenv).
