import pytest

from pyspark_partitions.utils.pyspark import get_spark_session


@pytest.fixture(scope="session", autouse=True)
def spark():
    spark = get_spark_session()
    return spark
