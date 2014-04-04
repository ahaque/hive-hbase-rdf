package edu.utexas.cs.ultrawrap.server;

import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;

public class DBpediaDriver {

	/**
	 * @param args
	 */
	private static String[] hiveDDL = new String[21];
	private static String[] hiveQL = new String[21];
	
	private static double[] arithmetic = new double[21];
	private static double[] geometric = new double[21];
		
	public static void main(String[] args) throws IOException, InterruptedException {

		/*
		String[] temp = new String[2];
		temp[0] = "localhost";
		temp[1] = "5";
		args = temp;
		*/
		
		if (args == null || args.length != 2) {
			System.out.println("  Arguments: <zk quorum> <query runs>");
			System.exit(0);
		}
		int queryRuns = -1;
		try {
			queryRuns = Integer.parseInt(args[1]);
		} catch (NumberFormatException e) {
			System.err.println(e);
		}
		DBpediaDriver dbp = new DBpediaDriver();
		SparqlHiveLink link = new SparqlHiveLink();
		HiveDDL ddl = new HiveDDL();
		link.master = args[0];
		
		for (int n = 1; n <= 20; n++) {
			if (n == 8 || n == 17) {
				continue;
			}
			System.out.println("\n--------------------------------------------");
			System.out.println("CURRENTLY RUNNING QUERY " + n + "\n");
			System.out.print("Dropping previous tables...");
			ddl.dropAllHiveTables();
			System.out.print(" Done!\n");
			long[] runtimes = new long[queryRuns];
			if (n == 4) {
				createQ4Tables(link);
			} else {
			System.out.print("Creating table for query " + n + "...");
			//System.out.println(hiveDDL[n]);
			link.execute(hiveDDL[n]);
			System.out.print(" Done!\n");
			}
			for (int i = 0; i < queryRuns; i++) {
				System.out.println("\nQuery: " + n + ". Run: " + (i+1) + " of " + queryRuns + "...");
				long startTime = System.nanoTime();
				System.out.println(hiveQL[n]);
				System.out.println(link.execute(hiveQL[n]));
				runtimes[i] = System.nanoTime() - startTime;
			}
			dbp.calculateAverages(n, runtimes);
			System.out.print("All Runtimes for Query " + n + " (sec): ");
			for (long t : runtimes) {
				System.out.print(t/1e9 + " ");
			}
			System.out.println();
			System.out.println("Q" + n + ": Arithmetic: " + arithmetic[n] + " | Geometric: " + geometric[n]);
			System.out.println();
		}
		
		System.out.println("\n\n---------------------------------------------------");
		System.out.println("DBpedia Benchmark With " + queryRuns + " Query Runs");
		System.out.println("Query\tAri\tGeo");
		for (int n = 1; n <= 20; n++) {
			if (n==8 || n == 17) {
				continue;
			}
			System.out.println("Q" + n + "\t" + arithmetic[n] + "\t" + geometric[n]);
		}
	}
	
	private static void createQ4Tables(SparqlHiveLink link) throws IOException, InterruptedException {
		String q4a = "CREATE EXTERNAL TABLE rdf2(key string, w3_2004_02_skos_core_subject string) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:w3_2004_02_skos_core_subject\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		String q4b = "CREATE EXTERNAL TABLE rdf3(key string, w3_2004_02_skos_core_broader string) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:w3_2004_02_skos_core_broader\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		System.out.println(link.execute(q4a));
		System.out.println(link.execute(q4b));
	}
	
	private static void calculateAverages(int queryNum, long[] times) {
		// Convert everything to seconds
		double[] seconds = new double[times.length];
		for (int i = 0; i < times.length; i++) {
			seconds[i] = (double) (times[i] / 1e9);
		}
		
		double sum = 0;
		double product = 1;
		for (double s : seconds) {
			sum += s;
			product *= s;
		}
		arithmetic[queryNum] = (double) ((double) sum)/((double)times.length);
		geometric[queryNum] = (double) Math.pow(product, 1.0/((double)times.length));
	}
	
	public DBpediaDriver() {
		hiveDDL[1] = "CREATE EXTERNAL TABLE rdf1(key string, subject string) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:w3_2004_02_skos_core_subject\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[2] = "CREATE EXTERNAL TABLE rdf1(key string, prop_redirect string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:prop_redirect\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[3] = "CREATE EXTERNAL TABLE rdf1(key string, ont_thumbnail string, foaf_page string, foaf_homepage string, w3_2000_01_rdf_schema_label string, w3_1999_02_22_rdf_syntax_ns_type string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:ont_thumbnail,p:foaf_page,p:foaf_homepage,p:w3_2000_01_rdf_schema_label,p:w3_1999_02_22_rdf_syntax_ns_type\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[5] = "CREATE EXTERNAL TABLE rdf1(key string, foaf_name string, ont_birthDate string, ont_deathDate string, prop_birthPlace string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:foaf_name,p:ont_birthDate,p:ont_deathDate,p:prop_birthPlace\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[6] = "CREATE EXTERNAL TABLE rdf1(key string, prop_writer string, prop_executiveProducer string, prop_creator string, prop_starring string, prop_guest string, prop_director string, prop_producer string, prop_series string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:prop_writer,p:prop_executiveProducer,p:prop_creator,p:prop_starring,p:prop_guest,p:prop_director,p:prop_producer,p:prop_series\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[7] = "CREATE EXTERNAL TABLE rdf1(key string, ont_abstract string) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES (\'hbase.columns.mapping\' = \':key,p:ont_abstract\', \'hbase.table.default.storage.type\' = \'binary\') TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[9] = "CREATE EXTERNAL TABLE rdf1(key string, foaf_givenName string, w3_1999_02_22_rdf_syntax_ns_type string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:foaf_givenName,p:w3_1999_02_22_rdf_syntax_ns_type\', \'hbase.table.default.storage.type\' = \'binary\') TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[10] = "CREATE EXTERNAL TABLE rdf1(key string, w3_1999_02_22_rdf_syntax_ns_type string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:w3_1999_02_22_rdf_syntax_ns_type\', \'hbase.table.default.storage.type\' = \'binary\' )TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[11] = "CREATE EXTERNAL TABLE rdf1(key string, foaf_name string, w3_2000_01_rdf_schema_comment string, w3_2004_02_skos_core_subject string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:foaf_name,p:w3_2000_01_rdf_schema_comment,p:w3_2004_02_skos_core_subject\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[12] = "CREATE EXTERNAL TABLE rdf1(key string, prop_redirect string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:prop_redirect\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[13] = "CREATE EXTERNAL TABLE rdf1(key string, foaf_page string, w3_2000_01_rdf_schema_label string, ont_influenced string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:foaf_page,p:w3_2000_01_rdf_schema_label,p:ont_influenced\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[14] = "CREATE EXTERNAL TABLE rdf1(key string, prop_instrument string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:prop_instrument\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[15] = "CREATE EXTERNAL TABLE rdf1(key string, w3_2000_01_rdf_schema_comment string, foaf_depiction string, foaf_homepage string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:w3_2000_01_rdf_schema_comment,p:foaf_depiction,p:foaf_homepage\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[16] = "CREATE EXTERNAL TABLE rdf1(key string, w3_2000_01_rdf_schema_label string, w3_1999_02_22_rdf_syntax_ns_type string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:w3_2000_01_rdf_schema_label,p:w3_1999_02_22_rdf_syntax_ns_type\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[18] = "CREATE EXTERNAL TABLE rdf1(key string, w3_2000_01_rdf_schema_label string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:w3_2000_01_rdf_schema_label\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[19] = "CREATE EXTERNAL TABLE rdf1(key string, w3_2000_01_rdf_schema_label string, w3_1999_02_22_rdf_syntax_ns_type string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:w3_2000_01_rdf_schema_label,p:w3_1999_02_22_rdf_syntax_ns_type\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";
		hiveDDL[20] = "CREATE EXTERNAL TABLE rdf1(key string, w3_2000_01_rdf_schema_label string, w3_1999_02_22_rdf_syntax_ns_type string, foaf_page string)  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( \'hbase.columns.mapping\' = \':key,p:w3_2000_01_rdf_schema_label,p:w3_1999_02_22_rdf_syntax_ns_type,p:foaf_page\', \'hbase.table.default.storage.type\' = \'binary\' ) TBLPROPERTIES (\'hbase.table.name\' = \'rdf1\');";

		hiveQL[1] = "SELECT DISTINCT subject FROM rdf1 WHERE key = \'res_Akatsi\';";
		hiveQL[2] = "SELECT DISTINCT key FROM rdf1 WHERE prop_redirect IS NOT NULL LIMIT 1000;";
		hiveQL[3] = "SELECT ont_thumbnail, foaf_page, foaf_homepage FROM rdf1 WHERE w3_2000_01_rdf_schema_label LIKE(\'%Maria Carolina van Savoye%\') AND w3_1999_02_22_rdf_syntax_ns_type = \'ont_Person\';";
		hiveQL[4] = "SELECT result.subj, result.board FROM (SELECT rdf2.w3_2004_02_skos_core_subject AS subj, rdf3.w3_2004_02_skos_core_broader AS board FROM rdf2 JOIN rdf3 ON (rdf2.w3_2004_02_skos_core_subject = rdf3.key) WHERE rdf2.key = \'res_Paul_Walker\' UNION ALL SELECT rdf2.w3_2004_02_skos_core_subject AS subj, rdf3.w3_2004_02_skos_core_broader AS board FROM rdf2 JOIN rdf3 ON (rdf2.w3_2004_02_skos_core_subject = rdf3.key) WHERE rdf2.key = \'res_Maiacetus\' ) result;";
		hiveQL[5] = "SELECT foaf_name, ont_birthDate, ont_deathDate, key FROM rdf1 WHERE prop_birthPlace LIKE(\'%Newburgh, New York%\');";
		hiveQL[6] = "SELECT DISTINCT result.var1 FROM(SELECT prop_writer AS var1 FROM rdf1 WHERE key = \'res_Cleaning_Time\' UNION ALL SELECT prop_executiveProducer AS var1 FROM rdf1 WHERE key = \'res_Cleaning_Time\' UNION ALL SELECT prop_creator AS var1 FROM rdf1 WHERE key = \'res_Cleaning_Time\' UNION ALL SELECT prop_starring AS var1 FROM rdf1 WHERE key = \'res_Cleaning_Time\' UNION ALL SELECT prop_guest AS var1 FROM rdf1 WHERE key = \'res_Cleaning_Time\' UNION ALL SELECT prop_director AS var1 FROM rdf1 WHERE key = \'res_Cleaning_Time\' UNION ALL SELECT prop_producer AS var1 FROM rdf1 WHERE key = \'res_Cleaning_Time\' UNION ALL SELECT prop_series AS var1 FROM rdf1 WHERE key = \'res_Cleaning_Time\') result;";
		hiveQL[7] = "SELECT ont_abstract FROM rdf1 WHERE key = \'res_Bazaar_e_Husn\';";
		hiveQL[9] = "SELECT key, foaf_givenName FROM rdf1 WHERE w3_1999_02_22_rdf_syntax_ns_type = \'class_yago_PakistaniQawwaliSingers\' AND foaf_givenName IS NOT NULL;";
		hiveQL[10] = "SELECT w3_1999_02_22_rdf_syntax_ns_type FROM rdf1 WHERE key = \'res_Marcelo_Estigarribia\' AND w3_1999_02_22_rdf_syntax_ns_type <> \'ont_Resource\';";
		hiveQL[11] = "SELECT foaf_name, w3_2000_01_rdf_schema_comment, key FROM rdf1 WHERE w3_2004_02_skos_core_subject = \'res_Category_McFly\';";
		hiveQL[12] = "SELECT key, prop_redirect FROM rdf1;";
		hiveQL[13] = "SELECT DISTINCT foaf_page, w3_2000_01_rdf_schema_label FROM rdf1 WHERE ont_influenced = \'res_John_Wesley\';";
		hiveQL[14] = "SELECT DISTINCT prop_instrument FROM rdf1 WHERE key = \'res_Randy_Brecker\';";
		hiveQL[15] = "SELECT w3_2000_01_rdf_schema_comment, foaf_depiction, foaf_homepage FROM rdf1 WHERE key = \'res_Cabezamesada\';";
		hiveQL[16] = "SELECT DISTINCT key, w3_2000_01_rdf_schema_label FROM rdf1 WHERE w3_1999_02_22_rdf_syntax_ns_type = \'class_yago_EnglishTranslators\';";
		hiveQL[18] = "SELECT w3_2000_01_rdf_schema_label FROM rdf1 WHERE key = \'res_Ryfylke\';";
		hiveQL[19] = "SELECT w3_2000_01_rdf_schema_label, w3_1999_02_22_rdf_syntax_ns_type FROM rdf1 WHERE key LIKE(\'%Guillermo Coria%\');";
		hiveQL[20] = "SELECT foaf_page FROM rdf1 WHERE w3_1999_02_22_rdf_syntax_ns_type = \'ont_Person\' AND w3_2000_01_rdf_schema_label LIKE(\'%Ruri Hoshino%\');";

	}

}
