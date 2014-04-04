package edu.utexas.cs.ultrawrap.server;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class SPARQLEndpoint{
	
	private static int port = 8081;
	
    public static void main(String[] args) throws Exception
    {
    	// Uncomment to run in an IDE/testing purposes
    	args = new String[1];
    	args[0] = "localhost";
    
    	if (args == null) {
    		System.out.println("  USAGE: java -jar SPARQLEndpoint.jar <hbase_master>");
    		System.exit(-1);
    	}
    	else if (args.length != 1) {
    		System.out.println("  USAGE: java -jar SPARQLEndpoint.jar <hbase_master>");
    		System.exit(-1);
    	}
    	
    	Server server = new Server(port);
		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		
        context.setContextPath("/");
      
        server.setHandler(context);
        try
        {
        	SPARQLServlet serv = new SPARQLServlet();
        	serv.setMaster(args[0]);
        	context.addServlet(new ServletHolder(serv),"/sparql/*");
            server.start();
            System.out.println("SPARQL Endpoint running at http://localhost:"+port+"/sparql");
            server.join();
        }
        catch(Exception e)
        {
        	 System.err.println("ERROR: Unable to connect to the database");
        }
    }

}