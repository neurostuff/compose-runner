import os
import pytest


@pytest.fixture(scope="session")
def vcr_config():
    """Control VCR record mode via VCR_RECORD_MODE env var (default: none).

    To re-record cassettes:
        VCR_RECORD_MODE=new_episodes pytest compose_runner/tests/
    """
    return {
        "decode_compressed_response": True,
        "record_mode": os.environ.get("VCR_RECORD_MODE", "none"),
    }
