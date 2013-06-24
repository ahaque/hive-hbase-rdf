hive-hbase-rdf
====================

An implementation of Hive over HBase to store and query RDF.

Files:
    src/DataSetProcessor.java - MapReduce program parses the dataset file and gets the unique subjects.
    src/Transformer.java - Assists in parsing RDF triples.
    src/CreateHBaseTable.java - Creates the HBase table and specifies HBase parameters.
    src/MRLoad.java - MapReduce program that loads data into the HBase table.`


How-To Guide to Setting Up the Experiment
---------------------
### HBase+Hive Cluster Setup (Amazon EC2/EMR)

1. Navigate to ‘Your Elastic MapReduce Job Flows’.
2. Click ‘Create New Job Flow’.
3. Make sure ‘Run your own application’ is checked.
4. In the ‘Choose Job Type’, select ‘HBase’.
5. Click ‘Continue’.
6. Under ‘Install Additional Packages’, check the ‘Hive’ box.
7. Click ‘Continue’.
8. All instance types should be ‘Large (m1.large)’ by default.
9. Under ‘Core Instance Group’, enter the number of slave nodes for the cluster (1, 2, 4, 8, or 16 used in this experiment).
10.	‘Task Instance Group’ should be zero.
11.	Click ‘Continue’.
12.	Select your EC2 key pair and leave all other options to default.
13.	Click ‘Continue’.
14.	Check the box ‘Configure your Bootstrap Actions’.
15. Under ‘Action Type’ select ‘Custom Action’.
16. For the ‘Amazon S3 Location’ enter: `s3://us-east-1.elasticmapreduce/bootstrap-actions/configure-hbase`.
17. For ‘Optional Arguments’ enter: `-s hbase.hregion.max.filesize=10737418240`.
18. Click ‘Continue’.
19. Review your settings and click ‘Create Job Flow’.
20. The cluster will take 3-5 minutes to fully initialize. 

Determine Split Keys & Create HBase Table
--------------
1. Move the dataset file to a location on HDFS.
2. Create a list of all unique subjects that appear in the dataset. Depending on the dataset you are running (BSBM or DBPedia), you may have to recreate the KeyProcessor.jar file.

> Run: `hadoop jar KeyProcessor.jar <INPUT_DATASET_FILE> <OUTPUT_FOLDER>`
> Example: `hadoop jar KeyProcessor.jar /data/bsbm_10M.nt /user/hadoop/bsbm-keys`

3. Determine the keys that will be used to divide the data evenly among the cluster.

> Run: `hadoop jar hadoop-core-1.0.4.jar org.apache.hadoop.mapreduce.lib.partition.InputSampler -r <CLUSTER_SIZE> -inFormat org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat -keyClass org.apache.hadoop.io.Text -splitRandom <PROBABILITY> <NUMBER_OF_SAMPLES> <NUMBER_OF_SPLITS_EXAMINED> <PATH_TO_KEYS_ON_HDFS> <OUTPUT_LOCATION>`
> Example: `hadoop jar hadoop-core-1.0.4.jar org.apache.hadoop.mapreduce.lib.partition.InputSampler -r 16 -inFormat org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat -keyClass org.apache.hadoop.io.Text -splitRandom 0.1 2000000 200 /dbp-keys /dbp-keyresult`

4. Look at the output from the InputSampler. Take these keys and insert them into the CreateHBaseTable.java file. Generate the jar file.
5. Create the HBase table by executing the CreateHBaseTable java/jar file.
6. Create the HBase StoreFiles.
    Run: `hadoop jar MRLoad.jar <TABLE_NAME> <ZOOKEEPER_QUORUM> <DATASET_FILE> <OUTPUT_DIRECTORY_FOR_STOREFILES>`
    Example: `hadoop jar MRLoad.jar rdf1 ec2-23-20-000-00.compute-1.amazonaws.com /MRLoad/input/dataset.nt /MRLoad/output`
7. Load the StoreFiles into HBase.
    Run: `hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles <PATH_TO_STOREFILES> <TABLE_NAME>`
    Example: `hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs:///MRLoad/output rdf1`
8. The dataset has now been loaded into HBase.
