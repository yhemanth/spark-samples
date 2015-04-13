# spark-samples
This project contains small, self-contained, and mostly toy examples that demonstrate the [Apache Spark API](https://spark.apache.org/).
I created these samples while reading the Spark documentation to practise the API. Each program independently exercises a key API on a very toy dataset.

## Pre-requisites
I have tested these samples with
* Java 7
* Spark 1.3.0
* Apache Hadoop 2.4.1 (as the master)

## Running the samples
* Build with `mvn clean package`. This creates an uber-jar in the target directory under spark-samples home.
* Run `spark-submit target/spark-samples-<version>-jar-with-dependencies.jar`. This lists all available samples in the project, each with a name and a short description of what the sample is about.
* Run `spark-submit target/spark-samples-<version>-jar-with-dependencies.jar <sample-name>` to get sample specific help, mainly the number and nature of arguments.
* Run `spark-submit target/spark-samples-<version>-jar-with-dependencies.jar <sample-name> <sample-args>` to execute each sample.

Note that each sample is run on sample data. The data is available under src/main/resources/<sub-directory>. Each sample's help points to the specific sub-directory to use as input.
I copy these files onto HDFS and run the program on YARN.

An example command is as follows:
```
spark-submit --master yarn --deploy-mode client target/spark-samples-1.0-SNAPSHOT-jar-with-dependencies.jar accumulators spark-samples/common-text-data spark-samples/broadcast-vars/accumulators-out
```
## Legal stuff
Copyright (c) 2015 Hemanth Yamijala

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
