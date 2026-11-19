"""Run the fake HubSpot server: ``python -m fake_hubspot --port 8090``."""

from __future__ import annotations

import argparse
import os


def main() -> None:
    import uvicorn

    from fake_hubspot.app import create_app
    from fake_hubspot.hub import FakeHub, FaultConfig

    p = argparse.ArgumentParser(description="Fake HubSpot CRM server")
    p.add_argument("--host", default=os.environ.get("FAKE_HUBSPOT_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("FAKE_HUBSPOT_PORT", "8090")))
    p.add_argument("--rate-limit", type=int, default=None, help="requests per 10s (default: off)")
    p.add_argument("--search-delay-ms", type=int, default=0)
    p.add_argument("--p-429", type=float, default=0.0)
    p.add_argument("--p-5xx", type=float, default=0.0)
    a = p.parse_args()
    faults = FaultConfig(
        requests_per_10s=a.rate_limit,
        search_delay_ms=a.search_delay_ms,
        p_429=a.p_429,
        p_5xx=a.p_5xx,
    )
    uvicorn.run(create_app(FakeHub(faults=faults)), host=a.host, port=a.port, log_level="warning")


if __name__ == "__main__":
    main()
