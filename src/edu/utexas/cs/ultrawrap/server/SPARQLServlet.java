package edu.utexas.cs.ultrawrap.server;

import java.io.IOException;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;

public class SPARQLServlet extends HttpServlet {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6278335585909685759L;
	static String master = "";
	

	public SPARQLServlet() {
	}
	
	public void setMaster(String m) {
		master = m;
	}


	private void runQuery(String sparqlQueryStr, HttpServletResponse response) throws Exception {
		ResultSet sparqlResult = executeSparql(sparqlQueryStr);
		String returnResponse = ResultSetFormatter.asXMLString(sparqlResult);
		response.getWriter().write(returnResponse);
		
	}

	public ResultSet executeSparql(String sparql) throws SQLException, IOException, InterruptedException {

		SparqlHiveLink link = new SparqlHiveLink();
		HiveDDL ddl = new HiveDDL();
		link.master = master;
		
		// Converts SPARQL query to HiveQL
		// link.convertToHive() uses string substitution
		// link.sparqlToHive() uses Jena ARQ and builds syntax tree
		//String hiveql = link.convertToHive(sparql);
		String jena_result = link.sparqlToHive(sparql);
		ddl.dropAllHiveTables();
		//System.out.println("-----------------------------------------");
		//System.out.println("Query: " + link.getBsbmQueryNumber(sparql));
		jena_result = null;
		HashMap<String, ArrayList<String>> topology = link.getQueryTopology(jena_result);
		String result = null;
		String xmlresult = "";
		ArrayList<String> projVars = null;
		if (jena_result != null) {
			// Executes the Hive Query
			result = link.execute(jena_result);
			System.out.println(result);
			// Returns a String representing the SPARQL result in XML
			projVars = link.getProjectedVars(sparql);
			xmlresult = link.getXmlResult(projVars, result);
		}

		//InputStream in = new ByteArrayInputStream(xmlresult.getBytes("UTF-8"));
		return ResultSetFactory.fromXML(xmlresult);
	}


	protected void doPost(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("application/sparql-results+xml");
		final String sparqlQueryStr = req.getParameter("query");
		// System.out.println("++++++++++ SPARQL POST Query no. "+counterForm);
		try {
			System.out.println("INPUT POST SPARQL QUERY =\n"+sparqlQueryStr);
			runQuery(sparqlQueryStr, response);
		} catch (Exception e) {
			System.err.println(e);
			response.getWriter().write(e.toString());
		}
	}

	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {

		response.setStatus(HttpServletResponse.SC_OK);
		String query = request.getParameter("query");
		if (query == null) {
			response.setContentType("text/html");
			String html = "<html><head><title>SPARQL Endpoint</title></head>"
					+ "<body>"
					+ "<h1>SPARQL Endpoint</h1>"
					+ "<form method=\"POST\" action=\"sparql\"/>"
					+ "<textarea rows=\"20\" cols=\"100\" name=\"query\" /></textarea>"
					+ "</br></br><input type=\"submit\" value=\"Submit SPARQL Query\" />"
					+ "</form>" + "</body>" + "</html>";
			response.getWriter().write(html);
		} else {
			response.setContentType("application/sparql-results+xml");
			final String sparqlQueryStr = URLDecoder.decode(query, "UTF-8");
			try {
				//System.out.println("INPUT GET SPARQL QUERY =\n"+sparqlQueryStr);
				runQuery(sparqlQueryStr, response);
			} catch (Exception e) {
				System.err.println(e);
				response.getWriter().write(e.toString());
			}

		}
	}

}