hive-hbase-rdf
==============

An implementation of Hive over HBase to store RDF.

Currently, this repository only contains our bulk loader. We are cleaning up our query layer and will commit that soon.

Files:

src/MRLoad.java - MapReduce bulk loader

src/CreateHBaseTable.java - Creates the HBase table