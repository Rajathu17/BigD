# BigD
Big Data Lab
# PySpark Programs

This repository contains four PySpark programs that perform different data processing tasks.

## Program 1: Maximum Value by Year

### Description
This program reads a text file, extracts the year and a specific value from each line, and finds the maximum value for each year.

### Code

import sys
from pyspark import SparkContext

sc = SparkContext()
f = sc.textFile(sys.argv[1])
data = f.map(lambda x: (int(x[15:19]), int(x[87:92])))
ans = data.reduceByKey(lambda a, b: a if a > b else b)
ans.saveAsTextFile(sys.argv[2])


### Usage
- Run the program using spark-submit program1.py input.txt output
- input.txt is the input file path
- output is the output directory path

## Program 2: Count by Column 15

### Description
This program reads a text file, splits each line by comma, and counts the occurrences of each value in the 15th column.

### Code

import sys
from pyspark import SparkContext

sc = SparkContext()
f = sc.textFile(sys.argv[1])
data = f.map(lambda x: (x.split(",")[15], 1))
ans = data.reduceByKey(lambda a, b: a + b)
ans.saveAsTextFile(sys.argv[2])


### Usage
- Run the program using spark-submit program2.py input.txt output
- input.txt is the input file path
- output is the output directory path

## Program 3: Count by Column 7

### Description
This program reads a text file, splits each line by comma, and counts the occurrences of each value in the 7th column.

### Code

import sys
from pyspark import SparkContext

sc = SparkContext()
f = sc.textFile(sys.argv[1])
data = f.map(lambda x: (x.split(",")[7], 1))
ans = data.reduceByKey(lambda a, b: a + b)
ans.saveAsTextFile(sys.argv[2])


### Usage
- Run the program using spark-submit program3.py input.txt output
- input.txt is the input file path
- output is the output directory path

## Program 4: Sum by Column 3

### Description
This program reads a text file, splits each line by tab, and sums up the values in the 8th column for each value in the 3rd column.

### Code

import sys
from pyspark import SparkContext

sc = SparkContext()
f = sc.textFile(sys.argv[1])
data = f.map(lambda x: (x.split('\t')[3], float(x.split('\t')[8])))
ans = data.reduceByKey(lambda a, b: a + b)
ans.saveAsTextFile(sys.argv[2])


### Usage
- Run the program using spark-submit program4.py input.txt output
- input.txt is the input file path
- output is the output directory path

## Requirements

- PySpark
- Spark

## Contributing

Feel free to contribute to this repository by submitting pull requests or issues.
