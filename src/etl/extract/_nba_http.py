"""Shared HTTP config for nba_api stats endpoints.

`stats.nba.com` silently drops requests coming from datacenter IP ranges
(GitHub Actions, cloud VMs), which shows up as a `ReadTimeout`. Routing the
requests through a residential proxy fixes it.

Set these in the environment (or `.env`) to enable it:

    NBA_PROXY=http://user:pass@host:port   # residential proxy endpoint
    NBA_TIMEOUT=60                          # per-request read timeout, seconds

When `NBA_PROXY` is unset the calls go out directly, exactly as before.
"""
import os

from dotenv import load_dotenv

load_dotenv()


def nba_proxy_kwargs() -> dict:
    """`{"proxy": ...}` when NBA_PROXY is set, otherwise `{}`."""
    proxy = os.getenv("NBA_PROXY")
    return {"proxy": proxy} if proxy else {}


def nba_request_kwargs() -> dict:
    """Common kwargs (`proxy` + `timeout`) for an nba_api stats endpoint call."""
    return {"timeout": int(os.getenv("NBA_TIMEOUT", "60")), **nba_proxy_kwargs()}
