import pytest


@pytest.fixture(scope="session")
def vcr_config():
    """Replay cassettes without live network dependency during routine test runs."""
    return {
        "decode_compressed_response": True,
        "record_mode": "none",
    }
