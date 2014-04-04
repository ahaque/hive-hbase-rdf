package optimization;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import translate.sql.BGP;
import translate.sql.BGP.Term;

public class SelfJoinElimination {
	
	
	public static BGP optimize(BGP bgp)
	{
		System.out.println("========== SELF JOIN ELIMINATION");
		
		Map<String, String> fromClauseMap =  bgp.getFromClauseMap();
		Map<String, String> newFromClauseMap = new TreeMap<String, String>();
		Map<String,List<Term>> variableMap = bgp.getVariableMap();
		List<Term> terms = bgp.getTerms();
		Set<String> projectedVars = bgp.getProjectedVars();
		
		System.out.println("fromClauseMap "+fromClauseMap);
		System.out.println("terms "+terms);
		
		System.out.println("variableMap "+variableMap);
		int tableIndex = 1;
		
		for (Map.Entry<String, List<Term>> entry : variableMap.entrySet()) {
			String newTableName = "hbase"+tableIndex;
		    String variableName = entry.getKey();
		    List<Term> termList = entry.getValue();
		    System.out.println("variableName = "+variableName);
		    
		    // If the termList has more than one term, then there is self join going on.
		    if(termList.size() > 1)
		    {
		    	for(int i=0; i<termList.size(); i++)
			    {
		    		
		    		Term term = termList.get(i);
			    	String table = term.getTable();
			    	String type = term.getType();
			    	System.out.println("\t table = "+table+", type = "+type);
			    	newFromClauseMap.put(table, newTableName);
			    	
			    }
		    	tableIndex++;
		    }
		    
		    
		    
		}
		System.out.println("newFromClauseMap = "+newFromClauseMap);
		
		
		System.out.println("==========END  SELF JOIN ELIMINATION");
		
		return bgp;
	}

}
