import os
import pytest

@pytest.fixture(autouse=True, scope="session")
def env_setup():
    os.environ["origURL"] = "test-url"
    os.environ["basePath"] = "/super"
    os.environ["sapBucket"] = "s3-test-bucket"