"""A fake HubSpot CRM server for integration tests and benchmarks."""

from fake_hubspot.app import create_app
from fake_hubspot.hub import FakeHub, FaultConfig

__all__ = ["FakeHub", "FaultConfig", "create_app"]
