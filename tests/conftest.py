import shutil

import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    if shutil.which("java") is None:
        pytest.skip("Spark needs Java, which is not installed here")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        # macOS often cannot bind to its own hostname; stay on loopback
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
