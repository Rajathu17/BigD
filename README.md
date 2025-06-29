# BigD
Big Data Lab

# PySpark Programs for Big Data Processing

This repository contains four PySpark programs performing common data transformations using RDDs. Each script reads a text file, performs map-reduce operations, and saves the result to an output directory.

## Requirements

* Python 3.x
* Apache Spark with PySpark installed
* Hadoop-compatible input files (CSV or tab-delimited)

---

## Program 1: Max Temperature by Year

**Description**: Extracts the year and temperature from each record and finds the maximum temperature per year.

```python
import sys
from pyspark import SparkContext

sc = SparkContext()
f = sc.textFile(sys.argv[1])

# Extract year (position 15-19) and temperature (position 87-92)
data = f.map(lambda x: (int(x[15:19]), int(x[87:92])))

# Reduce to maximum temperature per year
ans = data.reduceByKey(lambda a, b: a if a > b else b)

ans.saveAsTextFile(sys.argv[2])
```

---

## Program 2: Count of Items by Column 15 (CSV)

**Description**: Counts the frequency of values in the 16th column (index 15) of a CSV file.

```python
import sys
from pyspark import SparkContext

sc = SparkContext()
f = sc.textFile(sys.argv[1])

# Split CSV and map column 15 to (value, 1)
data = f.map(lambda x: (x.split(",")[15], 1))

# Reduce by key to count occurrences
ans = data.reduceByKey(lambda a, b: a + b)

ans.saveAsTextFile(sys.argv[2])
```

---

## Program 3: Count of Items by Column 7 (CSV)

**Description**: Counts the frequency of values in the 8th column (index 7) of a CSV file.

```python
import sys
from pyspark import SparkContext

sc = SparkContext()
f = sc.textFile(sys.argv[1])

# Split CSV and map column 7 to (value, 1)
data = f.map(lambda x: (x.split(",")[7], 1))

# Reduce by key to count occurrences
ans = data.reduceByKey(lambda a, b: a + b)

ans.saveAsTextFile(sys.argv[2])
```

---

## Program 4: Sum of Values by Key (Tab-separated)

**Description**: Computes the sum of values (9th column) grouped by keys in the 4th column (tab-separated input).

```python
import sys
from pyspark import SparkContext

sc = SparkContext()
f = sc.textFile(sys.argv[1])

# Map from 4th column to 9th column as float
data = f.map(lambda x: (x.split('\t')[3], float(x.split('\t')[8])))

# Reduce by key to compute sum
ans = data.reduceByKey(lambda a, b: a + b)

ans.saveAsTextFile(sys.argv[2])
```

---

## Running the Scripts

```bash
spark-submit programX.py input_file_path output_directory_path
```

Replace `programX.py` with the desired script file name.

---

## Notes

* Ensure SparkContext is not already running before executing.
* Output directory must not already exist.
* Adjust indexing logic as per your actual data format.

