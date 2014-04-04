package edu.utexas.cs.ultrawrap.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class HiveDDL {

	/**
	 * @param args
	 */
	
	String oldTablePattern = "table";
	String replacementTable = "rdf";
	String HBASE_TABLE_NAME = "rdf1";
	SparqlHiveLink link;
	boolean PRINT_HIVE_DDL = true;

	public HiveDDL() {
		link = new SparqlHiveLink();
	}
	
	public void dropAllHiveTables() {
		String cmdResult = "";
		// Get the list of current hive tables
		try {
			cmdResult = link.execute("SHOW TABLES;");
		} catch (Exception e) {
			e.printStackTrace();
		}

		ArrayList<String> currentTables = new ArrayList<String>();
		String[] rows = cmdResult.split("\n");
		boolean insideResultSegment = false;
		for (int i = 0; i < rows.length; i++) {
			if (rows[i].contains("OK")){
				insideResultSegment = true;
				continue;
			}
			else if (rows[i].contains("Time taken:")){
				insideResultSegment = false;
				break;
			}
			
			if(insideResultSegment == true) {
				currentTables.add(rows[i]);
			}
		}
		// Drop them..
		for (String table : currentTables) {
			try {
				link.execute("DROP TABLE " + table);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public String createHiveTable(String tableName, ArrayList<String> columns) {
		
		String colNameTypes = "";
		String hbaseCols = "";
		ArrayList<String> compressedCols = new ArrayList<String>();
		// Use the correct table name
		tableName = tableName.replace(oldTablePattern, replacementTable);
		
		StringBuilder build = new StringBuilder();
		// Compose the table columns and types
		build.append("key string,");
		for (String col : columns) {
			// We are handling the keys already, we don't want them duplicated in the DDL
			if (col.equals(tableName + ".key")) {
				continue;
			}
			else if (col.equals(tableName + ".s")) {
				continue;
			}
			
			String compressedCol = compressURI(col);
			int indexOfPeriod = compressedCol.indexOf('.');
			compressedCol = compressedCol.substring(indexOfPeriod+1);

			build.append(compressedCol + " string,");
			compressedCols.add(compressedCol);
		}
		// Remove the last comma
		build.deleteCharAt(build.length()-1);
		colNameTypes = build.toString();
		
		// Compose the hbase column mapping
		build.setLength(0);
		for (String cc : compressedCols) {
			build.append("p:" + cc + ",");
		}
		build.deleteCharAt(build.length()-1);
		hbaseCols = build.toString();
		
		String hiveddl = "CREATE EXTERNAL TABLE " + tableName + "(" + colNameTypes +")\r\n" + 
				"STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'\r\n" + 
				"WITH SERDEPROPERTIES (\r\n" + 
				"\"hbase.columns.mapping\" = \":key," + hbaseCols + "\",\r\n" + 
				"\"hbase.table.default.storage.type\" = \"binary\"\r\n" + 
				")\r\n" + 
				"TBLPROPERTIES (\"hbase.table.name\" = \""+HBASE_TABLE_NAME+"\");";
		return hiveddl;
	}
	
	public void createHiveTables(HashMap<String, ArrayList<String>> topology) {
		Set<String> keys = topology.keySet();
		
		for (String key : keys) {
			if(PRINT_HIVE_DDL){
			System.out.println(createHiveTable(key, topology.get(key)));
			}
			System.out.println();
		}
	}

	public String compressURI(String uri) {
		for (int i = 0; i < fullName.length; i++) {
			if (uri.contains(fullName[i])) {
				return uri.replace(fullName[i], abbrvName[i]);
			}
		}
		return uri;
	}

	public String compressAll(String input) {
		String inProgress = input;
		for (int i = 0; i < fullName.length; i++) {
			inProgress = inProgress.replaceAll(fullName[i], abbrvName[i]);
		}
		return inProgress;
	}

	public ArrayList<String> compressColumns(ArrayList<String> cols) {
		ArrayList<String> result = new ArrayList<String>();
		for (String col : cols) {
			result.add(compressURI(col));
		}
		return result;
	}

	public static void main(String[] args) {
		HiveDDL hdl = new HiveDDL();

		String sql = "SELECT DISTINCT table1.s,\r\n"
				+ "       table1.http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/price\r\n"
				+ "FROM  table1 JOIN\r\n"
				+ "      table2\r\n"
				+ "WHERE table1.http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/product = http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer4639/Product234826 AND\r\n"
				+ "      table2.http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/country = http://downlode.org/rdf/iso-3166/countries#US AND\r\n"
				+ "      table1.http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/vendor = table1.http://purl.org/dc/elements/1.1/publisher AND\r\n"
				+ "      table1.http://purl.org/dc/elements/1.1/publisher = table2.s AND\r\n"
				+ "      (table1.http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/deliveryDays <= 3) AND\r\n"
				+ "      (table1.http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validTo > '2008-06-20-00.00.00')\r\n"
				+ "ORDER BY ( table1.http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/price) ASC \r\n"
				+ "LIMIT 10";

		//System.out.println(hdl.compressAll(sql));

	}

	static String[] fullName = {
		"<http://dbpedia.org/resource/",
		"<http://dbpedia.org/property/",
		"<http://dbpedia.org/ontology/",
		"<http://dbpedia.org/class/",
		"<http://www.w3.org/",
		"<http://xmlns.com/foaf/0.1/"
};

static String[] abbrvName = {"res_","prop_","ont_","class_","w3_","foaf_"};

}
