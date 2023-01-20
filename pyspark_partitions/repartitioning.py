from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql._typing import ColumnOrName

from pyspark_partitions.helpers import count_number_of_partitions


def repartition(
    df: DataFrame,
    numPartitions: Union[int, "ColumnOrName"],
):
    """
    Acts as a safe repartition function. If we are trying to repartition by a number of partitions `numPartitions` less
    than the number of current partitions, it will automatically make a coalesce operation.

    Note: This method will only be useful if we DO NOT WANT TO REPARTITION BY ANY OTHER COLUMN. If we want to
    reduce the number of partitions but still maintain partitions by some columns, we will need to use the
    usual `df.repartition(...)`

    :param df: A Pyspark DataFrame
    :param numPartitions: Number of partitions
    :return: A partitioned Dataframe
    """
    if df.rdd.getNumPartitions() < numPartitions:
        df = df.repartition(numPartitions)
    elif df.rdd.getNumPartitions() > numPartitions:
        df = df.coalesce(numPartitions)
    return df


def remove_empty_partitions(df: DataFrame):
    """
    This method will remove empty partitions from a DataFrame. It is useful after a filter, for
    example, when a great number of partitions may contain zero registers.

    Note: This functionality may be useless if you are using Adaptive Query Execution from Spark 3.0

    :param df: A pyspark DataFrame
    :return: A DataFrame with all empty partitions removed
    """
    non_empty_partitions = sum(
        df.rdd.mapPartitions(count_number_of_partitions).collect()
    )
    return df.coalesce(non_empty_partitions)
