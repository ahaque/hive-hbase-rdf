package edu.utexas.cs.ultrawrap.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import translate.sparql.SparqlMain;

import arq.query;

public class SparqlHiveLink {

	public String[] bsbm = new String[13];
	public String[] hive = new String[13];
	public int[] bsbm_len = new int[13];
	public String master = "";
	public String hiveql_isolated = null;
	
	HashMap<String, String> cf_mapping;

	String cmd = "hive --auxpath /mnt/hive-hbase-handler-0.8.1.jar,/mnt/hbase-0.94.5.jar,/mnt/zookeeper-3.4.5.jar,/mnt/guava-11.0.2.jar -hiveconf hbase.master="+master+":60000 -hiveconf mapred.max.map.failures.percent=100";
	
	String[] fullName = {"<http://www.w3.org/1999/02/22-rdf-syntax-ns#",
			"<http://www.w3.org/2000/01/rdf-schema#",	
			"<http://xmlns.com/foaf/0.1/",
			"<http://purl.org/dc/elements/1.1/",
			"<http://www.w3.org/2001/XMLSchema#",
			"<http://purl.org/stuff/rev#",
			"<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/",
			"<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/"
	};
	
	String[] abbrvName = {"rdf_","rdfs_","foaf_","dc_","xsd_","rev_","bsbm_","bsbm-inst_"};

	static final String BANNED_CHARACTERS = "[-\\<>:#//@&().]";
	public static String removeBannedChars(String s){
		return s.replace(">","").replaceAll(BANNED_CHARACTERS, "_");
	}
	
	public static void main(String[] args) {
		String sparql = "CONSTRUCT {  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:product ?productURI .\r\n" + 
				"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:productlabel ?productlabel .\r\n" + 
				"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:vendor ?vendorname .\r\n" + 
				"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:vendorhomepage ?vendorhomepage . \r\n" + 
				"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:offerURL ?offerURL .\r\n" + 
				"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:price ?price .\r\n" + 
				"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:deliveryDays ?deliveryDays .\r\n" + 
				"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm-export:validuntil ?validTo } \r\n" + 
				"WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm:product ?productURI .\r\n" + 
				"        ?productURI rdfs:label ?productlabel .\r\n" + 
				"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm:vendor ?vendorURI .\r\n" + 
				"        ?vendorURI rdfs:label ?vendorname .\r\n" + 
				"        ?vendorURI foaf:homepage ?vendorhomepage .\r\n" + 
				"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm:offerWebpage ?offerURL .\r\n" + 
				"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm:price ?price .\r\n" + 
				"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm:deliveryDays ?deliveryDays .\r\n" + 
				"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor7/Offer13035> bsbm:validTo ?validTo }\r\n" + 
				"";
		
		SparqlHiveLink link = new SparqlHiveLink();
		HiveDDL hdl = new HiveDDL();
		//String hql = link.convertToHive(sparql);
		SparqlMain sm = new SparqlMain();
		String jena_result = sm.translateQueryS(sparql);
		HashMap<String, ArrayList<String>> topology = link.getQueryTopology(jena_result);
		hdl.createHiveTables(topology);
		String hiveql = hdl.compressAll(link.getHiveQL(jena_result));
		System.out.println("\n"+hiveql);
		String result = "";
//		try {
//			result = link.execute(hql);
//		} catch (IOException e) { 
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}

		//link.getXmlResult(link.getProjectedVars(sparql), result);
	}
	
	public String sparqlToHive(String sparql) {
		SparqlMain sm = new SparqlMain();
		return sm.translateQuery(sparql);
	}
	
	public String convertToHive(String sparql) {
		int queryNum = getBsbmQueryNumber(sparql);
		//System.out.println("Query: " + queryNum);
		String[] sparql_words = sparql.split("\\s+");
		String[] hive_words = hive[queryNum].split("\\s+");		

		// Prints numbered index of each word -- helps map variables
//		System.out.println("\nSPARQL");
//		for (int i = 0; i < sparql_words.length; i++) {
//			System.out.println(i + ":" + sparql_words[i]);
//		}
//		System.out.println("\nHIVE");
//		for (int i = 0; i < hive_words.length; i++) {
//			System.out.println(i + ":" + hive_words[i]);
//		}
		
		// Reduces string size by abbreviating all subj, obj, and predicates
		int j = 0;
		for(int i = 0; i < sparql_words.length; i++) {
			for (j=0; j < 8; j++){
				if (sparql_words[i].contains(fullName[j])){
					sparql_words[i] = sparql_words[i].replace(fullName[j], abbrvName[j]).replace(">", "");
					break;
				}
			}
		}

		/*
		 * Take all relevant variables from the SPARQL input and substitute it
		 * into the HiveQL output query
		 */
		switch (queryNum) {
		case 1: {
			hive_words[8] = "= \'" + removeBannedChars(sparql_words[24]) +"\'"; // %ProductType%
			hive_words[11] = "= \'" + removeBannedChars(sparql_words[28]) +"\'"; // %ProductFeature1%
			hive_words[17] = isolateDigits(sparql_words[41]); // %x%
			break;
		}
		case 2: {
			hive_words[30] = "\'" + removeBannedChars(sparql_words[27])+"\'"; // %ProductXYZ%
			break;
		}
		case 3: {
			hive_words[7] = "= \'" + removeBannedChars(sparql_words[23]) + "\'";
			hive_words[10] = "= \'" + removeBannedChars(sparql_words[27]) +"\'";
			hive_words[16] = isolateDigits(sparql_words[37]);
			hive_words[22] = isolateDigits(sparql_words[46]);
			break;
		}
		case 4: {
			hive_words[15] = "= \'" + removeBannedChars(sparql_words[26]) + "\'"; // %ProductType%
			hive_words[18] = "= \'" + removeBannedChars(sparql_words[30]) + "\'"; // %ProductFeature1%
			hive_words[21] = "= \'" + removeBannedChars(sparql_words[34]) + "\'"; // %ProductFeature2%
			hive_words[27] = isolateDigits(sparql_words[48]); // %x%
			hive_words[38] = "= \'" + removeBannedChars(sparql_words[59]) + "\'"; // %ProductType%
			hive_words[41] = "= \'" + removeBannedChars(sparql_words[30]) + "\'"; // %ProductFeature1%
			hive_words[44] = "= \'" + removeBannedChars(sparql_words[67]) + "\'"; // %ProductFeature3%
			hive_words[50] = isolateDigits(sparql_words[80]); // %y%
			break;
		}
		case 5: {
			hive_words[15] = "\'"+removeBannedChars(sparql_words[31])+"\'"; // %ProductXYZ%
			hive_words[19] = "\'"+removeBannedChars(sparql_words[50])+"\'"; // %ProductXYZ%
			break;
		}
		case 6: {
			hive_words[8] = "\'%"+sparql_words[24].substring(1, sparql_words[24].length()-2)+"%\'";
			break;
		}
		case 7: {
			hive_words[22] = "\'"+sparql_words[37]+"\'";
			//hive_words[53] = sparql_words[66].substring(1,20); // %currentDate%;
			String date = "\'"+sparql_words[66].substring(1,11)+" "+sparql_words[66].substring(12,20)+"\'";
			hive_words[54] = date;
			break;
		}
		case 8: {
			hive_words[21] = "\'"+removeBannedChars(sparql_words[26])+"\'";
			break;
		}
		case 9: {
			hive_words[52] = "\'"+removeBannedChars(sparql_words[7])+"\'";
			break;
		}
		case 10: {
			hive_words[15] = "\'"+removeBannedChars(sparql_words[17])+"\'"; // %ProductXYZ%
			// "2008-06-20T00:00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>
			// Need to format a little
			//hive_words[24] = sparql_words[50].substring(1,11); // %currentDate%
			String date = "\'"+sparql_words[50].substring(1,11)+" "+sparql_words[50].substring(12,20)+"\'";
			hive_words[30] = date;
			break;
		}
		case 11: {
			hive_words[15] = "\'"+removeBannedChars(sparql_words[7])+"\'";
			break;
		}
		case 12: {
			hive_words[21] = "\'"+removeBannedChars(sparql_words[20])+"\'";
		}
		}

		// Convert string array to single string
		StringBuilder build = new StringBuilder();
		for (String s : hive_words) {
			build.append(s + " ");
		}
		build.append(';');
		//System.out.println("Hive QL\n"+build.toString()+"\n\n");
		return new String(build.toString());
	}

	public int getBsbmQueryNumber(String sparql) {
		// Using lengths, find out BSBM query was entered
		String[] words = sparql.split("\\s+");
		int bsbmQueryNumber = -1;
		for (int i = 1; i <= 12; i++) {
			if (bsbm_len[i] == words.length) {
				bsbmQueryNumber = i;
				break;
			}
		}
		return bsbmQueryNumber;
	}

	public String getXmlResult(ArrayList<String> projVars, String output) {
		StringBuilder build = new StringBuilder();
		build.append("<?xml version=\"1.0\"?>\n" + 
				"<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">\n");
		
		// Get the projected variables
		build.append("\n<head>\n");
		for (String s : projVars){
			// Ignore the ? mark
			build.append("\t<variable name=\""+ s +"\"/>\n");
		}
		build.append("</head>\n\n");
		
		// Start adding the results
		build.append("<results>\n");
		
		// Determine rows (1 subject per row)
		String[] rows = output.split("\n");
		String[] cols = null;
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
				build.append("\t<result>\n");
				// For each variable in that row, add it as a result
				cols = rows[i].split("\t");
				for (int j = 0; j < cols.length; j++) {
					build.append("\t\t<binding name=\"" + projVars.get(j) + "\">"
							+ cols[j].replaceAll("[\n\r<>]", "") + "</binding>\n");
				}
				build.append("\t</result>\n");
			}
			
		}
		
		build.append("</results>\n"); 
		build.append("\n</sparql>");
		//System.out.println("-----------------------------------");
		//System.out.println(build.toString());
		
		return build.toString();
	}
	
	public ArrayList<String> getProjectedVars(String query) {
		ArrayList<String> proj_vars = new ArrayList<String>();
		String[] sparqlWords = query.split("\\s+");
		int whereIndex = -1;
		int startIndex = -1;
		// If this is a DESCRIBE or CONSTRUCT...
		// Or if the hive query projects different variables than the SPARQL query
		switch (getBsbmQueryNumber(query)) {
		case 12: {
			proj_vars.add("rdf1.URI");
			proj_vars.add("rdf1.rdfs_label");
			proj_vars.add("rdf1.bsbm_vendor");
			proj_vars.add("rdf2.rdfs_label");
			proj_vars.add("rdf2.foaf_homepage");
			proj_vars.add("rdf1.bsbm_offerWebpage");
			proj_vars.add("rdf1.bsbm_price");
			proj_vars.add("rdf1.bsbm_deliveryDays");
			proj_vars.add("rdf1.bsbm_validTo");
			return proj_vars;
		}
		case 11: {
			proj_vars.add("bsbm_product");
			proj_vars.add("bsbm_producer");
			proj_vars.add("bsbm_price");
			proj_vars.add("bsbm_validFrom");
			proj_vars.add("bsbm_validTo");
			proj_vars.add("bsbm_deliveryDays");
			proj_vars.add("foaf_homepage");
			proj_vars.add("dc_publisher");
			proj_vars.add("dc_date");
			return proj_vars;
		}
		case 9: {
			proj_vars.add("rdf2.URI");
			proj_vars.add("rdf2.rdf_type");
			proj_vars.add("rdf2.rdfs_label");
			proj_vars.add("rdf2.rdfs_comment");
			proj_vars.add("rdf2.dc_publisher");
			proj_vars.add("rdf2.dc_date");
			proj_vars.add("rdf2.rdfs_subClassOf");
			proj_vars.add("rdf2.foaf_homepage");
			proj_vars.add("rdf2.bsbm_country");
			proj_vars.add("rdf2.bsbm_producer");
			proj_vars.add("rdf2.bsbm_productPropertyNumeric1");
			proj_vars.add("rdf2.bsbm_productPropertyNumeric2");
			proj_vars.add("rdf2.bsbm_productPropertyNumeric3");
			proj_vars.add("rdf2.bsbm_productPropertyNumeric5");
			proj_vars.add("rdf2.bsbm_productPropertyTextual1");
			proj_vars.add("rdf2.bsbm_productPropertyTextual2");
			proj_vars.add("rdf2.bsbm_productPropertyTextual3");
			proj_vars.add("rdf2.bsbm_productPropertyTextual4");
			proj_vars.add("rdf2.bsbm_productFeature");
			proj_vars.add("rdf2.bsbm_productPropertyTextual5");
			proj_vars.add("rdf2.bsbm_productPropertyNumeric6");
			proj_vars.add("rdf2.bsbm_productPropertyTextual6");
			proj_vars.add("rdf2.bsbm_productPropertyNumeric4");
			proj_vars.add("rdf2.bsbm_product");
			proj_vars.add("rdf2.bsbm_vendor");
			proj_vars.add("rdf2.bsbm_price");
			proj_vars.add("rdf2.bsbm_validFrom");
			proj_vars.add("rdf2.bsbm_validTo");
			proj_vars.add("rdf2.bsbm_deliveryDays");
			proj_vars.add("rdf2.bsbm_offerWebpage");
			proj_vars.add("rdf2.foaf_name");
			proj_vars.add("rdf2.foaf_mbox_sha1sum");
			proj_vars.add("rdf2.bsbm_reviewFor");
			proj_vars.add("rdf2.rev_reviewer");
			proj_vars.add("rdf2.bsbm_reviewDate");
			proj_vars.add("rdf2.dc_title");
			proj_vars.add("rdf2.rev_text");
			proj_vars.add("rdf2.bsbm_rating2");
			proj_vars.add("rdf2.bsbm_rating3");
			proj_vars.add("rdf2.bsbm_rating1");
			proj_vars.add("rdf2.bsbm_rating4");
			return proj_vars;
		}
			default: { break; }
		}
		
		// Find the position of the SELECT / FROM in the BSBM query
		// All the projected variables lie in between the indicies
		for (int i = 0; i < sparqlWords.length; i++){
			if (sparqlWords[i].equalsIgnoreCase("SELECT")) {
				startIndex = i;
			}
			else if (sparqlWords[i].equalsIgnoreCase("DISTINCT")) {
				startIndex = i;
			}
			else if (sparqlWords[i].equalsIgnoreCase("WHERE")) {
				whereIndex = i;
				break;
			}
		}
		for(int i = startIndex+1; i < whereIndex; i++){
			// Remove the question mark
			proj_vars.add(sparqlWords[i].substring(1));
		}
		return proj_vars;
	}
	
	public HashMap<String, ArrayList<String>> getQueryTopology(String jena_result) {
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		String[] words = jena_result.split("\\s+");

		boolean inSchemaRegion = false;
		boolean inHiveQLRegion = false;
		StringBuilder build = new StringBuilder();
		String currentTableName = "";
		ArrayList<String> cols = new ArrayList<String>();

		for (int i = 0; i < words.length; i++) {
			if (inSchemaRegion == false && words[i].equalsIgnoreCase("SELECT")) {
				inHiveQLRegion = true;
				build.append(words[i] + " ");
				continue;
			}
			if (inSchemaRegion == false && words[i].equals("{")) {
				inSchemaRegion = true;
				currentTableName = words[i-1];
				continue;
			}
			if (inSchemaRegion == true && words[i].equals("}")) {
				inSchemaRegion = false;
				topology.put(currentTableName, cols);
				cols = new ArrayList<String>();
				continue;
			}
			if (inSchemaRegion == true) {
				cols.add(words[i]);
			}
			if (inHiveQLRegion == true) {
				build.append(words[i] + " ");
			}
		}
		hiveql_isolated = build.toString();
		//System.out.println(hiveql_isolated);
		return topology;
	}
	
	public String getHiveQL(String jena_result) {
		if (hiveql_isolated == null) {
			getQueryTopology(jena_result);
			return hiveql_isolated;
		}
		else {
			return hiveql_isolated;
		}
	}
	
	
	public String execute(String hiveql) throws IOException, InterruptedException {

		// Runs a hive command on UNIX Bash.
		// Hive is very problematic when trying to set up a Java-Hive connection
		// so running a command from command line is the easiest way
		String cmdfull = cmd + " -e \"" + hiveql + "\"";
		// String cmd = "ls -l"; // this is the command to execute in the Unix
		//System.out.println(cmd);
		ProcessBuilder pb = new ProcessBuilder("bash", "-c", cmdfull);
		pb.redirectErrorStream(true); // use this to capture messages sent to
										// stderr
		Process shell = pb.start();
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				shell.getInputStream()));
		String line = "";
		StringBuilder build = new StringBuilder();
		while ((line = reader.readLine()) != null) {
			build.append(line + "\n");
		}

		// int shellExitStatus = shell.waitFor(); // wait for the shell to
		// finish and get the return code
		shell.waitFor();
		shell.destroy();
		return build.toString();
	}
	
	// This trims any random characters that may be surrounding a number
	public String isolateDigits(String s){
		StringBuilder build = new StringBuilder();
		for (int i=0; i<s.length(); i++){
			int c = (int)s.charAt(i);
			if(c <= 57 && c >= 48){
				build.append((char)c);
			}
		}
		return build.toString();
	}

	public SparqlHiveLink() {

		hive[1] = "SELECT DISTINCT key, rdfs_label\r\n" + 
				"FROM rdf1\r\n" + 
				"WHERE\r\n" + 
				"rdf_type %ProductType%\r\n" + 
				"AND bsbm_productFeature %ProductFeature1%\r\n" + 
				"AND cast(bsbm_productPropertyNumeric1 as int) > %x%\r\n" + 
				"ORDER BY rdfs_label\r\n" + 
				"LIMIT 10";

		hive[2] = "SELECT rdf1.rdfs_label, rdf1.rdfs_comment, rdf2.rdfs_label, rdf3.rdfs_label, rdf1.bsbm_productPropertyTextual1, rdf1.bsbm_productPropertyTextual2, rdf1.bsbm_productPropertyTextual3, rdf1.bsbm_productPropertyNumeric1, rdf1.bsbm_productPropertyNumeric2, rdf1.bsbm_productPropertyTextual4, rdf1.bsbm_productPropertyTextual5, rdf1.bsbm_productPropertyNumeric4\r\n"
				+ "FROM rdf1 JOIN rdf2 ON (rdf1.bsbm_producer = rdf2.key) JOIN rdf3 ON (rdf1.bsbm_productFeature = rdf3.key)\r\n"
				+ "WHERE rdf1.key = \"%ProductXYZ%\"";

		hive[3] = "SELECT key, rdfs_label\r\n" + "FROM rdf1\r\n"
				+ "WHERE\r\n" + "rdf_type \"%ProductType%\"\r\n"
				+ "AND bsbm_productFeature \"%ProductFeature1%\"\r\n"
				+ "AND cast(bsbm_productPropertyNumeric1 as int) > %x%\r\n"
				+ "AND cast(bsbm_productPropertyNumeric3 as int) < %y%\r\n"
				+ "ORDER BY rdfs_label\r\n" + "LIMIT 10";
		
		hive[4] = "SELECT DISTINCT key, rdfs_label, bsbm_productPropertyTextual1\r\n" + 
				"FROM (\r\n" + 
				"SELECT key, rdfs_label, bsbm_productPropertyTextual1\r\n" + 
				"FROM rdf1\r\n" + 
				"WHERE\r\n" + 
				"rdf1.rdf_type \"%ProductType%\"\r\n" + 
				"AND rdf1.bsbm_productFeature \"%ProductFeature1%\"\r\n" + 
				"AND rdf1.bsbm_productFeature \"%ProductFeature2%\"\r\n" + 
				"AND cast(rdf1.bsbm_productPropertyNumeric1 as int) > %x%\r\n" + 
				"\r\n" + 
				"UNION ALL\r\n" + 
				"\r\n" + 
				"SELECT key, rdfs_label, bsbm_productPropertyTextual1\r\n" + 
				"FROM rdf2\r\n" + 
				"WHERE\r\n" + 
				"rdf2.rdf_type \"%ProductType%\"\r\n" + 
				"AND rdf2.bsbm_productFeature \"%ProductFeature1%\"\r\n" + 
				"AND rdf2.bsbm_productFeature \"%ProductFeature3%\"\r\n" + 
				"AND cast(rdf2.bsbm_productPropertyNumeric2 as int) > %y%\r\n" + 
				") result\r\n" + 
				"ORDER BY rdfs_label\r\n" + 
				"LIMIT 10";
		
		hive[5] = "SELECT DISTINCT rdf2.key, rdf2.rdfs_label\r\n" + 
				"FROM rdf1 JOIN rdf2 ON (rdf1.bsbm_productFeature = rdf2.bsbm_productFeature)\r\n" + 
				"WHERE\r\n" + 
				"rdf2.key <> \"%ProductXYZ%\"\r\n" + 
				"AND rdf1.key = \"%ProductXYZ%\"\r\n" + 
				"AND rdf1.bsbm_productFeature = rdf2.bsbm_productFeature\r\n" + 
				"AND cast(rdf2.bsbm_productPropertyNumeric1 as int) < (cast(rdf1.bsbm_productPropertyNumeric1 as int) + 120)\r\n" + 
				"AND cast(rdf2.bsbm_productPropertyNumeric1 as int) > (cast(rdf1.bsbm_productPropertyNumeric1 as int) - 120)\r\n" + 
				"AND cast(rdf2.bsbm_productPropertyNumeric2 as int) < (cast(rdf1.bsbm_productPropertyNumeric2 as int) + 170)\r\n" + 
				"AND cast(rdf2.bsbm_productPropertyNumeric2 as int) > (cast(rdf1.bsbm_productPropertyNumeric2 as int) - 170)\r\n" + 
				"ORDER BY rdfs_label\r\n" + 
				"LIMIT 5";
		
		hive[6] = "SELECT key, rdfs_label\r\n" + 
				"FROM rdf1\r\n" + 
				"WHERE rdfs_label LIKE '%word1%'" +
				";";
		
		hive[7] = "SELECT rdf1.rdfs_label, rdf2.key, rdf2.bsbm_price, rdf2.bsbm_vendor, rdf3.rdfs_label, rdf4.key, rdf4.dc_title, rdf4.rev_reviewer, rdf5.foaf_name, rdf4.bsbm_rating1, rdf4.bsbm_rating2\r\n" + 
				"FROM rdf1 LEFT OUTER JOIN rdf2 ON ( rdf2.bsbm_product = \"%ProductXYZ%\" ) JOIN rdf3 ON (rdf2.bsbm_vendor = rdf3.key) LEFT OUTER JOIN rdf4 ON (rdf4.key = rdf1.bsbm_reviewFor) LEFT OUTER JOIN rdf5 ON (rdf4.rev_reviewer = rdf5.key)\r\n" + 
				"WHERE rdf3.bsbm_country = '_http___downlode_org_rdf_iso_3166_countries_DE'\r\n" + 
				"AND unix_timestamp(rdf2.bsbm_validTo) > unix_timestamp( %currentDate% )";
		
		hive[8] = "SELECT rdf1.dc_title, rdf1.rev_text, rdf1.bsbm_reviewDate, rdf1.rev_reviewer, rdf2.foaf_name, rdf1.bsbm_rating1, rdf1.bsbm_rating2, rdf1.bsbm_rating3, rdf1.bsbm_rating4\r\n" + 
				"FROM rdf1 JOIN rdf2 ON (rdf2.key = rdf1.rev_reviewer)\r\n" + 
				"WHERE rdf1.bsbm_reviewFor = \"%ProductXYZ%\"\r\n" + 
				"ORDER BY rdf1.bsbm_reviewDate DESC\r\n" + 
				"LIMIT 20";
		
		hive[9] = "SELECT rdf2.rdf_type, rdf2.rdfs_label, rdf2.rdfs_comment, rdf2.dc_publisher, rdf2.dc_date, rdf2.rdfs_subClassOf, rdf2.foaf_homepage, rdf2.bsbm_country, rdf2.bsbm_producer, rdf2.bsbm_productPropertyNumeric1, rdf2.bsbm_productPropertyNumeric2, rdf2.bsbm_productPropertyNumeric3, rdf2.bsbm_productPropertyNumeric5, rdf2.bsbm_productPropertyTextual1, rdf2.bsbm_productPropertyTextual2, rdf2.bsbm_productPropertyTextual3, rdf2.bsbm_productPropertyTextual4, rdf2.bsbm_productFeature, rdf2.bsbm_productPropertyTextual5, rdf2.bsbm_productPropertyNumeric6, rdf2.bsbm_productPropertyTextual6, rdf2.bsbm_productPropertyNumeric4, rdf2.bsbm_product, rdf2.bsbm_vendor, rdf2.bsbm_price, rdf2.bsbm_validFrom, rdf2.bsbm_validTo, rdf2.bsbm_deliveryDays, rdf2.bsbm_offerWebpage, rdf2.foaf_name, rdf2.foaf_mbox_sha1sum, rdf2.bsbm_reviewFor, rdf2.rev_reviewer, rdf2.bsbm_reviewDate, rdf2.dc_title, rdf2.rev_text, rdf2.bsbm_rating2, rdf2.bsbm_rating3, rdf2.bsbm_rating1, rdf2.bsbm_rating4 \r\n" + 
				"FROM rdf1 JOIN rdf2 ON (rdf1.rev_reviewer = rdf2.key)\r\n" + 
				"WHERE rdf1.key = \"%ReviewXYZ%\"";
		
		hive[10] = "SELECT DISTINCT rdf1.key, rdf1.bsbm_price FROM rdf1 JOIN rdf2 ON (rdf1.bsbm_vendor = rdf2.key)\r\n" + 
				"WHERE\r\n" + 
				"rdf1.bsbm_product = \"%ProductXYZ%\"\r\n" + 
				"AND cast(rdf1.bsbm_deliveryDays as int) <= 3 \r\n" + 
				"AND rdf2.bsbm_country = \'http___downlode_org_rdf_iso_3166_countries_US\'\r\n" + 
				"AND unix_timestamp(rdf1.bsbm_validTo) > unix_timestamp( %currentDate% )\r\n" + 
				"AND rdf1.dc_publisher = rdf2.key\n" +
				"ORDER BY bsbm_price\r\n" + 
				"LIMIT 10";
		
		hive[11] = "SELECT bsbm_product, bsbm_producer, bsbm_price, bsbm_validFrom, bsbm_validTo, bsbm_deliveryDays, foaf_homepage, dc_publisher, dc_date\r\n" + 
				"FROM rdf1\r\n" + 
				"WHERE key = \"@OfferXYZ@\"\r\n";
		
		hive[12] = "SELECT rdf1.key, rdf1.rdfs_label, " +
				"rdf1.bsbm_vendor, " +
				"rdf2.rdfs_label, rdf2.foaf_homepage, " +
				"rdf1.bsbm_offerWebpage, " +
				"rdf1.bsbm_price, " +
				"rdf1.bsbm_deliveryDays, " +
				"rdf1.bsbm_validTo\r\n" + 
				"FROM rdf1 JOIN rdf2 ON (rdf1.bsbm_vendor = rdf2.key)\n" +
				"WHERE rdf1.key = \"%OfferXYZ%\";";

		bsbm_len[1] = 48;
		bsbm_len[2] = 94;
		bsbm_len[3] = 66;
		bsbm_len[4] = 91;
		bsbm_len[5] = 75;
		bsbm_len[6] = 26;
		bsbm_len[7] = 103;
		bsbm_len[8] = 87;
		bsbm_len[9] = 11;
		bsbm_len[10] = 58;
		bsbm_len[11] = 18;
		bsbm_len[12] = 90;

		bsbm[1] = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\r\n"
				+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\r\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\r\n"
				+ "\r\n"
				+ "SELECT DISTINCT ?product ?label\r\n"
				+ "WHERE { \r\n"
				+ " ?product rdfs:label ?label .\r\n"
				+ " ?product a %ProductType% .\r\n"
				+ " ?product bsbm:productFeature %ProductFeature1% . \r\n"
				+ " ?product bsbm:productFeature %ProductFeature2% . \r\n"
				+ "?product bsbm:productPropertyNumeric1 ?value1 . \r\n"
				+ "	FILTER (?value1 > %x%) \r\n"
				+ "	}\r\n"
				+ "ORDER BY ?label\r\n" + "LIMIT 10";
		bsbm[2] = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\r\n"
				+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\r\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n"
				+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>\r\n"
				+ "\r\n"
				+ "SELECT ?label ?comment ?producer ?productFeature ?propertyTextual1 ?propertyTextual2 ?propertyTextual3\r\n"
				+ " ?propertyNumeric1 ?propertyNumeric2 ?propertyTextual4 ?propertyTextual5 ?propertyNumeric4 \r\n"
				+ "WHERE {\r\n"
				+ " %ProductXYZ% rdfs:label ?label .\r\n"
				+ "	%ProductXYZ% rdfs:comment ?comment .\r\n"
				+ "	%ProductXYZ% bsbm:producer ?p .\r\n"
				+ "	?p rdfs:label ?producer .\r\n"
				+ " %ProductXYZ% dc:publisher ?p . \r\n"
				+ "	%ProductXYZ% bsbm:productFeature ?f .\r\n"
				+ "	?f rdfs:label ?productFeature .\r\n"
				+ "	%ProductXYZ% bsbm:productPropertyTextual1 ?propertyTextual1 .\r\n"
				+ "	%ProductXYZ% bsbm:productPropertyTextual2 ?propertyTextual2 .\r\n"
				+ " %ProductXYZ% bsbm:productPropertyTextual3 ?propertyTextual3 .\r\n"
				+ "	%ProductXYZ% bsbm:productPropertyNumeric1 ?propertyNumeric1 .\r\n"
				+ "	%ProductXYZ% bsbm:productPropertyNumeric2 ?propertyNumeric2 .\r\n"
				+ "	OPTIONAL { %ProductXYZ% bsbm:productPropertyTextual4 ?propertyTextual4 }\r\n"
				+ " OPTIONAL { %ProductXYZ% bsbm:productPropertyTextual5 ?propertyTextual5 }\r\n"
				+ " OPTIONAL { %ProductXYZ% bsbm:productPropertyNumeric4 ?propertyNumeric4 }\r\n"
				+ "}";
		bsbm[3] = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\r\n"
				+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\r\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\r\n"
				+ "\r\n"
				+ "SELECT ?product ?label\r\n"
				+ "WHERE {\r\n"
				+ " ?product rdfs:label ?label .\r\n"
				+ " ?product a %ProductType% .\r\n"
				+ "	?product bsbm:productFeature %ProductFeature1% .\r\n"
				+ "	?product bsbm:productPropertyNumeric1 ?p1 .\r\n"
				+ "	FILTER ( ?p1 > %x% ) \r\n"
				+ "	?product bsbm:productPropertyNumeric3 ?p3 .\r\n"
				+ "	FILTER (?p3 < %y% )\r\n"
				+ " OPTIONAL { \r\n"
				+ " ?product bsbm:productFeature %ProductFeature2% .\r\n"
				+ " ?product rdfs:label ?testVar }\r\n"
				+ " FILTER (!bound(?testVar)) \r\n"
				+ "}\r\n"
				+ "ORDER BY ?label\r\n" + "LIMIT 10";
		bsbm[4] = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\r\n"
				+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\r\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\r\n"
				+ "\r\n"
				+ "SELECT DISTINCT ?product ?label ?propertyTextual\r\n"
				+ "WHERE {\r\n"
				+ " { \r\n"
				+ " ?product rdfs:label ?label .\r\n"
				+ " ?product rdf:type %ProductType% .\r\n"
				+ " ?product bsbm:productFeature %ProductFeature1% .\r\n"
				+ "	?product bsbm:productFeature %ProductFeature2% .\r\n"
				+ " ?product bsbm:productPropertyTextual1 ?propertyTextual .\r\n"
				+ "	?product bsbm:productPropertyNumeric1 ?p1 .\r\n"
				+ "	FILTER ( ?p1 > %x% )\r\n"
				+ " } UNION {\r\n"
				+ " ?product rdfs:label ?label .\r\n"
				+ " ?product rdf:type %ProductType% .\r\n"
				+ " ?product bsbm:productFeature %ProductFeature1% .\r\n"
				+ "	?product bsbm:productFeature %ProductFeature3% .\r\n"
				+ " ?product bsbm:productPropertyTextual1 ?propertyTextual .\r\n"
				+ "	?product bsbm:productPropertyNumeric2 ?p2 .\r\n"
				+ "	FILTER ( ?p2> %y% ) \r\n"
				+ " } \r\n"
				+ "}\r\n"
				+ "ORDER BY ?label\r\n" + "OFFSET 5\r\n" + "LIMIT 10";
		bsbm[5] = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\r\n"
				+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\r\n"
				+ "\r\n"
				+ "SELECT DISTINCT ?product ?productLabel\r\n"
				+ "WHERE { \r\n"
				+ "	?product rdfs:label ?productLabel .\r\n"
				+ " FILTER (%ProductXYZ% != ?product)\r\n"
				+ "	%ProductXYZ% bsbm:productFeature ?prodFeature .\r\n"
				+ "	?product bsbm:productFeature ?prodFeature .\r\n"
				+ "	%ProductXYZ% bsbm:productPropertyNumeric1 ?origProperty1 .\r\n"
				+ "	?product bsbm:productPropertyNumeric1 ?simProperty1 .\r\n"
				+ "	FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 – 120))\r\n"
				+ "	%ProductXYZ% bsbm:productPropertyNumeric2 ?origProperty2 .\r\n"
				+ "	?product bsbm:productPropertyNumeric2 ?simProperty2 .\r\n"
				+ "	FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 – 170))\r\n"
				+ "}\r\n" + "ORDER BY ?productLabel\r\n" + "LIMIT 5";
		bsbm[6] = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\r\n"
				+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\r\n"
				+ "\r\n" + "SELECT ?product ?label\r\n" + "WHERE {\r\n"
				+ "	?product rdfs:label ?label .\r\n"
				+ " ?product rdf:type bsbm:Product .\r\n"
				+ "	FILTER regex(?label, \"%word1%\")\r\n" + "}";
		bsbm[7] = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n"
				+ "PREFIX rev: <http://purl.org/stuff/rev#>\r\n"
				+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\r\n"
				+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\r\n"
				+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>\r\n"
				+ "\r\n"
				+ "SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle \r\n"
				+ " ?reviewer ?revName ?rating1 ?rating2\r\n"
				+ "WHERE { \r\n"
				+ "	%ProductXYZ% rdfs:label ?productLabel .\r\n"
				+ " OPTIONAL {\r\n"
				+ " ?offer bsbm:product %ProductXYZ% .\r\n"
				+ "	?offer bsbm:price ?price .\r\n"
				+ "	?offer bsbm:vendor ?vendor .\r\n"
				+ "	?vendor rdfs:label ?vendorTitle .\r\n"
				+ " ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#DE> .\r\n"
				+ " ?offer dc:publisher ?vendor . \r\n"
				+ " ?offer bsbm:validTo ?date .\r\n"
				+ " FILTER (?date > %currentDate% )\r\n" + " }\r\n"
				+ " OPTIONAL {\r\n"
				+ "	?review bsbm:reviewFor %ProductXYZ% .\r\n"
				+ "	?review rev:reviewer ?reviewer .\r\n"
				+ "	?reviewer foaf:name ?revName .\r\n"
				+ "	?review dc:title ?revTitle .\r\n"
				+ " OPTIONAL { ?review bsbm:rating1 ?rating1 . }\r\n"
				+ " OPTIONAL { ?review bsbm:rating2 ?rating2 . } \r\n"
				+ " }\r\n" + "}";
		bsbm[8] = "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\r\n"
				+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>\r\n"
				+ "PREFIX rev: <http://purl.org/stuff/rev#>\r\n"
				+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\r\n"
				+ "\r\n"
				+ "SELECT ?title ?text ?reviewDate ?reviewer ?reviewerName ?rating1 ?rating2 ?rating3 ?rating4 \r\n"
				+ "WHERE { \r\n"
				+ "	?review bsbm:reviewFor %ProductXYZ% .\r\n"
				+ "	?review dc:title ?title .\r\n"
				+ "	?review rev:text ?text .\r\n"
				+ "	FILTER langMatches( lang(?text), \"EN\" ) \r\n"
				+ "	?review bsbm:reviewDate ?reviewDate .\r\n"
				+ "	?review rev:reviewer ?reviewer .\r\n"
				+ "	?reviewer foaf:name ?reviewerName .\r\n"
				+ "	OPTIONAL { ?review bsbm:rating1 ?rating1 . }\r\n"
				+ "	OPTIONAL { ?review bsbm:rating2 ?rating2 . }\r\n"
				+ "	OPTIONAL { ?review bsbm:rating3 ?rating3 . }\r\n"
				+ "	OPTIONAL { ?review bsbm:rating4 ?rating4 . }\r\n"
				+ "}\r\n"
				+ "ORDER BY DESC(?reviewDate)\r\n" + "LIMIT 20";
		bsbm[9] = "PREFIX rev: <http://purl.org/stuff/rev#>\r\n" + "\r\n"
				+ "DESCRIBE ?x\r\n" + "WHERE { %ReviewXYZ% rev:reviewer ?x }";
		bsbm[10] = "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\r\n"
				+ "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \r\n"
				+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>\r\n"
				+ "\r\n"
				+ "SELECT DISTINCT ?offer ?price\r\n"
				+ "WHERE {\r\n"
				+ "	?offer bsbm:product %ProductXYZ% .\r\n"
				+ "	?offer bsbm:vendor ?vendor .\r\n"
				+ " ?offer dc:publisher ?vendor .\r\n"
				+ "	?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> .\r\n"
				+ "	?offer bsbm:deliveryDays ?deliveryDays .\r\n"
				+ "	FILTER (?deliveryDays <= 3)\r\n"
				+ "	?offer bsbm:price ?price .\r\n"
				+ " ?offer bsbm:validTo ?date .\r\n"
				+ " FILTER (?date > %currentDate% )\r\n"
				+ "}\r\n"
				+ "ORDER BY xsd:double(str(?price))\r\n" + "LIMIT 10";
		bsbm[11] = "SELECT ?property ?hasValue ?isValueOf\r\n" + "WHERE {\r\n"
				+ " { %OfferXYZ% ?property ?hasValue }\r\n" + " UNION\r\n"
				+ " { ?isValueOf ?property %OfferXYZ% }\r\n" + "}";
		bsbm[12] = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n"
				+ "PREFIX rev: <http://purl.org/stuff/rev#>\r\n"
				+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\r\n"
				+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\r\n"
				+ "PREFIX bsbm-export: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/>\r\n"
				+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>\r\n"
				+ "\r\n"
				+ "CONSTRUCT { %OfferXYZ% bsbm-export:product ?productURI .\r\n"
				+ " %OfferXYZ% bsbm-export:productlabel ?productlabel .\r\n"
				+ " %OfferXYZ% bsbm-export:vendor ?vendorname .\r\n"
				+ " %OfferXYZ% bsbm-export:vendorhomepage ?vendorhomepage . \r\n"
				+ " %OfferXYZ% bsbm-export:offerURL ?offerURL .\r\n"
				+ " %OfferXYZ% bsbm-export:price ?price .\r\n"
				+ " %OfferXYZ% bsbm-export:deliveryDays ?deliveryDays .\r\n"
				+ " %OfferXYZ% bsbm-export:validuntil ?validTo } \r\n"
				+ "WHERE { %OfferXYZ% bsbm:product ?productURI .\r\n"
				+ " ?productURI rdfs:label ?productlabel .\r\n"
				+ " %OfferXYZ% bsbm:vendor ?vendorURI .\r\n"
				+ " ?vendorURI rdfs:label ?vendorname .\r\n"
				+ " ?vendorURI foaf:homepage ?vendorhomepage .\r\n"
				+ " %OfferXYZ% bsbm:offerWebpage ?offerURL .\r\n"
				+ " %OfferXYZ% bsbm:price ?price .\r\n"
				+ " %OfferXYZ% bsbm:deliveryDays ?deliveryDays .\r\n"
				+ " %OfferXYZ% bsbm:validTo ?validTo }\r\n" + "";
		
	}

}
