"""TLS options reach the OpenSearch client (offline: no request is sent)."""

from __future__ import annotations

from sqlalchemy.engine import make_url

import osagg
from osagg.sqla import OpenSearchAggHttpsDialect

BASE = "osagg+https://svc:p%40ss@opensearch.example:9200/default?timezone=Europe/Paris"


def pool_of(conn):
    return conn.transport.client.transport.connection_pool.connections[0].pool


def connect_uri(uri: str):
    args, kw = OpenSearchAggHttpsDialect().create_connect_args(make_url(uri))
    return osagg.connect(*args, **kw)


def test_certificates_are_verified_by_default():
    pool = pool_of(connect_uri(BASE))
    assert pool.scheme == "https" and pool.cert_reqs == "CERT_REQUIRED"


def test_verify_certs_false_in_the_uri_disables_verification():
    pool = pool_of(connect_uri(BASE + "&verify_certs=false"))
    assert pool.scheme == "https" and pool.cert_reqs == "CERT_NONE" and pool.ca_certs is None


def test_verify_certs_false_in_engine_parameters():
    # Advanced > Other > Engine parameters: {"connect_args": {"verify_certs": false}}
    conn = osagg.connect(host="opensearch.example", port=9200, scheme="https", verify_certs=False)
    assert pool_of(conn).cert_reqs == "CERT_NONE"


def test_ca_certs_is_used_for_verification(tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("placeholder")
    pool = pool_of(connect_uri(BASE + f"&ca_certs={ca}"))
    assert pool.cert_reqs == "CERT_REQUIRED" and pool.ca_certs == str(ca)
