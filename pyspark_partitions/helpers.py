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
