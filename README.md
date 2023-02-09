<h1 align="center">
  pyspark-partitions
</h1>

<h4 align="center">A library to help PySpark developers with partition tuning</h4>

<p align="center">
  <a href="#description">Description</a> •
  <a href="#how-to-use">How To Use</a> •
  <a href="#next-steps">Useful Resources</a> •
</p>


## Description

One of the main problems that any PySpark developer has to face is
[partition tuning](https://luminousmen.com/post/spark-tips-partition-tuning).
If the number of partitions is too high, this will directly impact the performance of our job. If the number is small,
the job may not finish because the executors run out of memory.

**So, how can we know the optimal number of partitions
when partitioning a DataFrame? And what happens if the data is heavily skewed?**

Well, this library aims to solve these problems, although it is true that, as always in PySpark,
it will also require some intuition and trial-and-error experiments to get our job to work optimally.


## How To Use

```bash
# Install the latest version of the package
$ pip install -U pyspark-partitions
```

COMPLETE THIS WHEN FIRST VERSION OF LIBRARY IS AVAILABLE


## Useful Resources

A list of articles and blogs I used to delve deeper into partition tuning and Spark in general.

* [Spark Tips. Partition Tuning](https://luminousmen.com/post/spark-tips-partition-tuning)
* [PySpark — The Famous Salting Technique](https://subhamkharwal.medium.com/pyspark-the-famous-salting-technique-da8f34c28211)
* [Spark’s Salting — A Step Towards Mitigating Skew Problem](https://medium.com/curious-data-catalog/sparks-salting-a-step-towards-mitigating-skew-problem-5b2e66791620)
