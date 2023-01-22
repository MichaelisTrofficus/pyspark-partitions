import logging

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType


def count_number_of_partitions(iterator):
    """
    Simply returns de number of nonempty partitions in a DataFrame.
    :param iterator: An iterator containing each partition
    :return:
    """
    n = 0
    for _ in iterator:
        n += 1
        break
    yield n


def df_size_in_bytes_exact(df: DataFrame):
    """
    Calculates the exact size in memory of a DataFrame by caching it and accessing the optimized plan

    NOTE: BE CAREFUL WITH THIS FUNCTION BECAUSE IT WILL CACHE ALL THE DATAFRAME!!! IF YOUR DATAFRAME IS
    TOO BIG USE `estimate_df_size_in_bytes`!!

    :param df: A pyspark DataFrame
    :return: The exact size in bytes
    """
    df = df.cache().select(
        df.columns
    )  # Just force the Spark planner to add the Cache op to the plan
    logging.info(f"Number of rows in the input DataFrame: {df.count()}")
    size_in_bytes = df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
    df.unpersist(blocking=True)
    return size_in_bytes


def df_size_in_bytes_approximate(df: DataFrame, sample_perc: float = 0.05):
    """
    This method takes a sample of the input DataFrame (`sample_perc`) and applies `df_size_in_bytes_exact`
    method to it. After it calculates the exact size of the sample, it extrapolates the total size.

    :param df: A pyspark DataFrame
    :param sample_perc: The percentage of the DataFrame to sample. By default, a 5 %
    :return: The estimated size in bytes
    """
    sample_size_in_bytes = df_size_in_bytes_exact(df.sample(sample_perc))
    return sample_size_in_bytes / sample_perc


def add_partition_id_column(df: DataFrame):
    return df.withColumn("partition_id", sf.spark_partition_id())


def get_partition_count(df: DataFrame) -> DataFrame:
    """
    Gets the number of registers per partition. This method is useful if we are trying to determine if some
    partition is skewed.

    :return: A DataFrame containing `partition_id` and `count` columns
    """
    return add_partition_id_column(df).groupBy("partition_id").count()


def add_salt_column(df: DataFrame, skew_factor: int):
    """
    Adds a salt column to a DataFrame. We will be using this salt column when we are trying to perform
    join, groupBy, etc. operations into a skewed DataFrame. The idea is to add a random column and use
    the original keys + this salted key to perform the operations, so that we can avoid data skewness and
    possibly, OOM errors.

    :param df: A Pyspark DataFrame
    :param skew_factor: The skew factor. For example, if we set this value to 3, then the salted column will
        be populated by the elements 0, 1 and 2, extracted from a uniform probability distribution.
    :return: The original DataFrame with a `salt_id` column.
    """
    return df.withColumn("salt_id", (sf.rand() * skew_factor).cast(IntegerType()))
