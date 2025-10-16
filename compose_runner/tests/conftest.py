import pytest


@pytest.fixture(scope="session")
def vcr_config():
    """Configure VCR to decode compressed HTTP responses when recording."""
    return {"decode_compressed_response": True}
