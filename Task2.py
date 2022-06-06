import numpy as np
from concurrent.futures import as_completed, ProcessPoolExecutor
from time import time
import os
import ray
from pyspark import SparkContext

os.environ["PYSPARK_PYTHON"] = "python"
os.environ['HADOOP_HOME'] = r"C:\Program Files\Spark\spark-3.2.1-bin-hadoop2.7\hadoop-2.7.1"


def factorization_count(n):
    count = 0
    while n % 2 == 0:
        count += 1
        n >>= 1
    for i in range(3, int(np.sqrt(n)) + 1):
        while n % i == 0:
            n //= i
            count += 1
        i += 2
    if n > 2:
        count += 1
    return count


def use_pure_python(numbers):
    factorization_sum = 0
    for n in numbers:
        factorization_sum += factorization_count(n)
    return factorization_sum


def use_multiprocessing(numbers):
    factorization_sum = 0
    with ProcessPoolExecutor(max_workers=12) as pool:
        futures = [pool.submit(factorization_count, n) for n in numbers]
        for res in as_completed(futures):
            factorization_sum += res.result()
    return factorization_sum


def use_rdd(numbers):
    sc = SparkContext(master='local[*]')
    rdd = sc.parallelize(numbers)
    return sum(rdd.map(factorization_count).collect())


@ray.remote(num_cpus=4)
def ray_func(n):
    return factorization_count(n)


def use_ray(numbers):
    refs = [ray_func.remote(n) for n in numbers]
    return sum([ray.get(ref) for ref in refs])


if __name__ == "__main__":
    with open("numbers.txt", 'r') as file:
        numbers = list(map(int, file.read().split('\n')))

    ray.init()

    for func, name in zip(
            [use_pure_python, use_multiprocessing, use_rdd, use_ray],
            ["Pure python", "Multiprocessing", "Pyspark RDD", "Ray"]
    ):
        times = []
        factorization_sum = 0
        for i in range(10):
            start = time()
            factorization_sum = func(numbers)
            times.append(time() - start)
        print(name)
        print(f"Time {np.mean(times):.4}s mean, {np.std(times):.4}s std")
        print("Result :", factorization_sum, end="\n\n")
