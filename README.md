# HBase+Hive for RDF

### Background
This is an implementation of Hive over HBase to store and query RDF using Hadoop.

This system was part of two academic efforts:
1. P. Cudré-Mauroux, I. Enchev, S. Fundatureanu, P. Groth, A. Haque, A. Harth, F. Keppmann, D. Miranker, J. Sequeda, and M. Wylot. "NoSQL Databases for RDF: An Empirical Evaluation." Proceedings of the 12th International Semantic Web Conference (ISWC). LNCS, vol. 8219, pp. 310-325. Springer, 2013. DOI: 10.1007/978-3-642-41338-4_20. Project Website: http://ribs.csres.utexas.edu/nosqlrdf/index.php

2. A. Haque. "A MapReduce Approach to NoSQL RDF Databases." The University of Texas at Austin, Department of Computer Science. Report# HR-13-1X (honors theses). Dec 2013. 79 pages.

### Files
* src/DataSetProcessor.java - MapReduce program parses the dataset file and gets the unique subjects.
* src/Transformer.java - Assists in parsing RDF triples.
* src/CreateHBaseTable.java - Creates the HBase table and specifies HBase parameters.
* src/MRLoad.java - MapReduce program that loads data into the HBase table.

## How-To Guide to Setting Up the Experiment
### Table of Contents
1. Section 1: Cluster Setup (Amazon EC2/EMR)
2. Section 2: Loading Data into HBase

### Section 1: Cluster Setup (Amazon EC2/EMR)

This guide was created using the AWS Management Console interface in December 2013.

1. Navigate to the AWS Management Console and go to the *Elastic MapReduce* service.
2. Click *Create Cluster*.
3. Under *Software Configuration*, select the appropriate version of Hadoop.
4. If HBase is not listed under *Applications to be installed*, add it and ignore the backup steps.
5. If Hive is not listed under *Applications to be installed*, add it and select the appropriate Hive version.
6. All instance types should be Large (m1.large) by default since we are running HBase.
7. Under *Hardware Configuration*, in the *Core* instance type, enter the number of slave nodes for the cluster (1, 2, 4, 8, or 16 used in this experiment).
8. *Task Instance Group* should be zero.
9. Select your EC2 key pair and leave all other options to default.
10. Under *Bootstrap Actions*, add a new, *Custom action*.
11. For the *S3 Location* enter:
`s3://us-east-1.elasticmapreduce/bootstrap-actions/configure-hbase`.
12. For ‘Optional Arguments’ enter:
`-s hbase.hregion.max.filesize=10737418240`.
13. Add the bootstrap action.
14. Review your settings and click *Create Cluster*.
15. The cluster will take 3-5 minutes to fully initialize.

### Section 2: Loading Data into HBase
1: Move the dataset file to a location on HDFS.

2: Create a list of all unique subjects that appear in the dataset. Depending on the dataset you are running (BSBM or DBPedia), you may have to recreate the KeyProcessor.jar file.

```
hadoop jar KeyProcessor.jar <INPUT_DATASET_FILE> <OUTPUT_FOLDER>
hadoop jar KeyProcessor.jar /data/bsbm_10M.nt /user/hadoop/bsbm-keys
```

3: Determine the keys that will be used to divide the data evenly among the cluster.
```
hadoop jar hadoop-core-1.0.4.jar org.apache.hadoop.mapreduce.lib.partition.InputSampler -r <CLUSTER_SIZE> -inFormat org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat -keyClass org.apache.hadoop.io.Text -splitRandom <PROBABILITY> <NUMBER_OF_SAMPLES> <NUMBER_OF_SPLITS_EXAMINED> <PATH_TO_KEYS_ON_HDFS> <OUTPUT_LOCATION>
hadoop jar hadoop-core-1.0.4.jar org.apache.hadoop.mapreduce.lib.partition.InputSampler -r 16 -inFormat org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat -keyClass org.apache.hadoop.io.Text -splitRandom 0.1 2000000 200 /dbp-keys /dbp-keyresult
```

4: Look at the output from the InputSampler. Take these keys and insert them into the CreateHBaseTable.java file. Generate the jar file.

5: Create the HBase table by executing the CreateHBaseTable java/jar file.

6: Create the HBase StoreFiles.
```
hadoop jar MRLoad.jar <TABLE_NAME> <ZOOKEEPER_QUORUM> <DATASET_FILE> <OUTPUT_DIRECTORY_FOR_STOREFILES>
hadoop jar MRLoad.jar rdf1 ec2-23-20-000-00.compute-1.amazonaws.com /MRLoad/input/dataset.nt /MRLoad/output
```

7: Load the StoreFiles into HBase.
```
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles <PATH_TO_STOREFILES> <TABLE_NAME>
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs:///MRLoad/output rdf1
```
	
8: The dataset has now been loaded into HBase.