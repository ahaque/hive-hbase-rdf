package translate.sparql;

import java.util.Stack;

import translate.sql.SqlOP;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;



public class SparqlMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String query1 = 
				"PREFIX ex: <http://www.example.com/>  " +
				"SELECT ?name   " +
				"WHERE {  " +
				" ?x ex:name ?name . " +
				"} ";
		
		String query2 = 
				"PREFIX ex: <http://www.example.com/>  " +
				"SELECT ?name ?age  " +
				"WHERE {  " +
				" ?x ex:name ?name . " +
				" ?x ex:age ?age . " +
				"} ";
		
		String query3 = 
				"PREFIX ex: <http://www.example.com/>  " +
				"SELECT ?name ?age ?phone  " +
				"WHERE {  " +
				" ?x ex:name ?name . " +
				" ?x ex:age ?age . " +
				" ?x ex:phone ?phone . " +
				"} ";
		
		String query4 = 
				"PREFIX ex: <http://www.example.com/>  " +
				"SELECT ?name ?y  " +
				"WHERE {  " +
				" ?x ex:name ?name . " +
				" ?x ex:knows ?y . " +
				"} ";
		
		String query5 = 
				"PREFIX ex: <http://www.example.com/>  " +
				"SELECT ?name ?age ?y ?friendsName  " +
				"WHERE {  " +
				" ?x ex:name ?name . " +
				" ?x ex:age ?age . " +
				" ?x ex:knows ?y . " +
				" ?y ex:name ?friendsName . " +
				"} ";
		
		String bsbm1 =
				"PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				"SELECT DISTINCT ?product ?label " +
				"WHERE {  " +
				"?product rdfs:label ?label . " +
				"?product a <ProductType> . " +
				"?product bsbm:productFeature <ProductFeature1> .  " +
				"?product bsbm:productFeature <ProductFeature2> .  " +
				"?product bsbm:productPropertyNumeric1 ?value1 .  " +
				"	FILTER (?value1 > <x>)  " +
				"	} " +
				"ORDER BY ?label " +
				"LIMIT 10";
		
		String bsbm2 =
				"PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"PREFIX dc: <http://purl.org/dc/elements/1.1/> " +
				"SELECT ?label ?comment ?producer ?productFeature ?propertyTextual1 ?propertyTextual2 ?propertyTextual3 " +
				"?propertyNumeric1 ?propertyNumeric2 ?propertyTextual4 ?propertyTextual5 ?propertyNumeric4  " +
				"WHERE { " +
				"<ProductXYZ> rdfs:label ?label . " +
				"	<ProductXYZ> rdfs:comment ?comment . " +
				"	<ProductXYZ> bsbm:producer ?p . " +
				"	?p rdfs:label ?producer . " +
				"<ProductXYZ> dc:publisher ?p .  " +
				"	<ProductXYZ> bsbm:productFeature ?f . " +
				"	?f rdfs:label ?productFeature . " +
				"	<ProductXYZ> bsbm:productPropertyTextual1 ?propertyTextual1 . " +
				"	<ProductXYZ> bsbm:productPropertyTextual2 ?propertyTextual2 . " +
				"<ProductXYZ> bsbm:productPropertyTextual3 ?propertyTextual3 . " +
				"	<ProductXYZ> bsbm:productPropertyNumeric1 ?propertyNumeric1 . " +
				"	<ProductXYZ> bsbm:productPropertyNumeric2 ?propertyNumeric2 . " +
				"	OPTIONAL { <ProductXYZ> bsbm:productPropertyTextual4 ?propertyTextual4 } " +
				"OPTIONAL { <ProductXYZ> bsbm:productPropertyTextual5 ?propertyTextual5 } " +
				"OPTIONAL { <ProductXYZ> bsbm:productPropertyNumeric4 ?propertyNumeric4 } " +
				"}";
				
		String bsbm3 =	
				"PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				"SELECT ?product ?label " +
				"WHERE { " +
				"?product rdfs:label ?label . " +
				"?product a <ProductType> . " +
				"	?product bsbm:productFeature <ProductFeature1> . " +
				"	?product bsbm:productPropertyNumeric1 ?p1 . " +
				"	FILTER ( ?p1 > <x> )  " +
				"	?product bsbm:productPropertyNumeric3 ?p3 . " +
				"	FILTER (?p3 < <y> ) " +
				"OPTIONAL {  " +
				"?product bsbm:productFeature <ProductFeature2> . " +
				"?product rdfs:label ?testVar } " +
				"FILTER (!bound(?testVar))  " +
				"} " +
				"ORDER BY ?label " +
				"LIMIT 10";
				
		String bsbm4 =	
				"PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				"SELECT DISTINCT ?product ?label ?propertyTextual " +
				"WHERE { " +
				"{  " +
				"?product rdfs:label ?label . " +
				"?product rdf:type <ProductType> . " +
				"?product bsbm:productFeature <ProductFeature1> . " +
				"?product bsbm:productFeature <ProductFeature2> . " +
				"?product bsbm:productPropertyTextual1 ?propertyTextual . " +
				"	?product bsbm:productPropertyNumeric1 ?p1 . " +
				"	FILTER ( ?p1 > <x> ) " +
				"} UNION { " +
				"?product rdfs:label ?label . " +
				"?product rdf:type <ProductType> . " +
				"?product bsbm:productFeature <ProductFeature1> . " +
				"	?product bsbm:productFeature <ProductFeature3> . " +
				"?product bsbm:productPropertyTextual1 ?propertyTextual . " +
				"	?product bsbm:productPropertyNumeric2 ?p2 . " +
				"	FILTER ( ?p2 > <y> )  " +
				"}  " +
				"} " +
				"ORDER BY ?label " +
				//"OFFSET 5 " +
				"LIMIT 10";
				
		String bsbm5 =	
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"SELECT DISTINCT ?product ?productLabel " +
				"WHERE {  " +
				"	?product rdfs:label ?productLabel . " +
				"FILTER (<ProductXYZ> != ?product) " +
				"	<ProductXYZ> bsbm:productFeature ?prodFeature . " +
				"	?product bsbm:productFeature ?prodFeature . " +
				"	<ProductXYZ> bsbm:productPropertyNumeric1 ?origProperty1 . " +
				"	?product bsbm:productPropertyNumeric1 ?simProperty1 . " +
				"	FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120)) " +
				"	<ProductXYZ> bsbm:productPropertyNumeric2 ?origProperty2 . " +
				"	?product bsbm:productPropertyNumeric2 ?simProperty2 . " +
				"	FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170)) " +
				"} " +
				"ORDER BY ?productLabel " +
				"LIMIT 5";
				
		String bsbm6 =
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"SELECT ?product ?label " +
				"WHERE { " +
				"	?product rdfs:label ?label . " +
				"?product rdf:type bsbm:Product . " +
				"	FILTER regex(?label, \"<word1<\") " +
				"}";
				
		String bsbm7 =
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
				"PREFIX rev: <http://purl.org/stuff/rev#> " +
				"PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"PREFIX dc: <http://purl.org/dc/elements/1.1/> " +
				"SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle  " +
				"?reviewer ?revName ?rating1 ?rating2 " +
				"WHERE {  " +
				"	<ProductXYZ> rdfs:label ?productLabel . " +
				"OPTIONAL { " +
				"?offer bsbm:product <ProductXYZ> . " +
				"	?offer bsbm:price ?price . " +
				"	?offer bsbm:vendor ?vendor . " +
				"	?vendor rdfs:label ?vendorTitle . " +
				"?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#DE> . " +
				"?offer dc:publisher ?vendor .  " +
				"?offer bsbm:validTo ?date . " +
				"FILTER (?date > <currentDate> ) " +
				"} " +
				//"OPTIONAL { " +
				"	?review bsbm:reviewFor <ProductXYZ> . " +
				"	?review rev:reviewer ?reviewer . " +
				"	?reviewer foaf:name ?revName . " +
				"	?review dc:title ?revTitle . " +
				"OPTIONAL { ?review bsbm:rating1 ?rating1 . } " +
				"OPTIONAL { ?review bsbm:rating2 ?rating2 . }  " +
				//"} " +
				"}";
				
		String bsbm8 =
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"PREFIX dc: <http://purl.org/dc/elements/1.1/> " +
				"PREFIX rev: <http://purl.org/stuff/rev#> " +
				"PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +	
				"SELECT ?title ?text ?reviewDate ?reviewer ?reviewerName ?rating1 ?rating2 ?rating3 ?rating4  " +
				"WHERE {  " +
				"	?review bsbm:reviewFor <ProductXYZ> . " +
				"	?review dc:title ?title . " +
				"	?review rev:text ?text . " +
				"	FILTER langMatches( lang(?text), \"EN\" )  " +
				"	?review bsbm:reviewDate ?reviewDate . " +
				"	?review rev:reviewer ?reviewer . " +
				"	?reviewer foaf:name ?reviewerName . " +
				"	OPTIONAL { ?review bsbm:rating1 ?rating1 . } " +
				"	OPTIONAL { ?review bsbm:rating2 ?rating2 . } " +
				"	OPTIONAL { ?review bsbm:rating3 ?rating3 . } " +
				"	OPTIONAL { ?review bsbm:rating4 ?rating4 . } " +
				"} " +
				"ORDER BY DESC(?reviewDate) " +
				"LIMIT 20";
				
		String bsbm9 =
				"PREFIX rev: <http://purl.org/stuff/rev#> " +
				"DESCRIBE ?x " +
				"WHERE { <ReviewXYZ> rev:reviewer ?x }";
				
		String bsbm10 =
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>  " +
				"PREFIX dc: <http://purl.org/dc/elements/1.1/> " +
				"SELECT DISTINCT ?offer ?price " +
				"WHERE { " +
				"	?offer bsbm:product <ProductXYZ> . " +
				"	?offer bsbm:vendor ?vendor . " +
				"?offer dc:publisher ?vendor . " +
				"	?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> . " +
				"	?offer bsbm:deliveryDays ?deliveryDays . " +
				"	FILTER (?deliveryDays <= 3) " +
				"	?offer bsbm:price ?price . " +
				"?offer bsbm:validTo ?date . " +
				"FILTER (?date > <currentDate> ) " +
				"} " +
				"ORDER BY xsd:double(str(?price)) " +
				"LIMIT 10";
				
		String bsbm11 =
				"SELECT ?property ?hasValue ?isValueOf " +
				"WHERE { " +
				"{ <OfferXYZ> ?property ?hasValue } " +
				"UNION " +
				"{ ?isValueOf ?property <OfferXYZ> } " +
				"}";
		
//		System.out.println("\nQuery 1:"); executeQuery(query1);
//		System.out.println("\nQuery 2:"); executeQuery(query2);
//		System.out.println("\nQuery 3:"); executeQuery(query3);
//		System.out.println("\nQuery 4:"); executeQuery(query4);
//		System.out.println("\nQuery 5:"); executeQuery(query5);
//		System.out.println("\nBenchmark 1:"); translateQueryS(bsbm1);
//		System.out.println("\nBenchmark 2:"); translateQueryS(bsbm2);
//		System.out.println("\nBenchmark 3:"); translateQueryS(bsbm3);
//		System.out.println("\nBenchmark 4:"); translateQueryS(bsbm4);
//		System.out.println("\nBenchmark 5:"); translateQueryS(bsbm5);
//		System.out.println("\nBenchmark 6:"); translateQueryS(bsbm6);
//		System.out.println("\nBenchmark 7:"); translateQueryS(bsbm7);
//		System.out.println("\nBenchmark 8:"); translateQueryS(bsbm8);
//		System.out.println("\nBenchmark 9:"); translateQueryS(bsbm9);
//		System.out.println("\nBenchmark 10:"); translateQueryS(bsbm10);
//		System.out.println("\nBenchmark 11:"); translateQueryS(bsbm11);
	}
	
	public SparqlMain() {
		
	}
	
	public static String translateQueryS(String query) {
		query = query.replaceAll("(?i)OPTIONAL\\s\\{(.*?)(\\.\\s)?\\}", "$1 .");
		Query qry = QueryFactory.create(query);
		Op op = Algebra.compile(qry);
		//System.out.println(op);
		SparqlVisitor visitor = new SparqlVisitor(new Stack<SqlOP>());
		OpWalker.walk(op, visitor);
		return visitor.toString();

	}
	
	public String translateQuery(String query) {
		query = query.replaceAll("(?i)OPTIONAL\\s\\{(.*?)(\\.\\s)?\\}", "$1 .");
		Query qry = QueryFactory.create(query);
		Op op = Algebra.compile(qry);
		//System.out.println(op);
		SparqlVisitor visitor = new SparqlVisitor(new Stack<SqlOP>());
		OpWalker.walk(op, visitor);
		//System.out.println(visitor.toString());
		return visitor.toString();
	}
}
