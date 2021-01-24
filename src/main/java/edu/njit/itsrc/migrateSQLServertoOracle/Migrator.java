package edu.njit.itsrc.migrateSQLServertoOracle;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import edu.njit.itsrc.migrateSQLServertoOracle.MyScriptRunner;
import edu.njit.itsrc.migrateSQLServertoOracle.SQLServer;
import edu.njit.itsrc.migrateSQLServertoOracle.Oracle;

public class Migrator {
	private static final Logger logger = LogManager.getLogger(Migrator.class);
	PropertiesConfiguration config;
	public Migrator() {
		try {
		//SQLServer sqlServer = new SQLServer();
		Oracle oracle = new Oracle();
		//sqlServer.Sqlserverconnection();
		//sqlServer.getViewInformation(); 
		oracle.oracleconnection();
		oracle.performsViewBackup(); 
		runSQLScript();
		}
		catch(Exception e) {
			try {
				runBackupFile();
			} catch (Exception e1) {
				 logger.error(e);
				e1.printStackTrace();
			}
			 logger.error(e);
			e.printStackTrace();
		}
	}
	
	private void runSQLScript() throws Exception  {
			config = new PropertiesConfiguration("PropertyFiles\\ViewNames.properties");
			Oracle oracle = new Oracle();
			oracle.oracleconnection();
			SQLServer sqlServer = new SQLServer();
			sqlServer.Sqlserverconnection();
			String viewnames = oracle.runMySQLScript();
			String loadedview ="";
			String notloadedview ="";
			String[] views = viewnames.split(",");
			config.setProperty("ImportedViews", "");
			config.setProperty("NotImportedViews", "");
			for(String view : views) {
			  File file = new File(sqlServer.path+"\\"+view+".sql");
			  if(file.exists()) {
				  MyScriptRunner sr = new MyScriptRunner(oracle.databaseConnection);
			      Reader reader = new BufferedReader(new FileReader(sqlServer.path+"\\"+view+".sql"));
			      try {
			    	//  sr.setFullLineDelimiter(true);
			    	  sr.setEscapeProcessing(true);
			    	  sr.setDelimiter(";;");
			    	  sr.runScript(reader);
			    	  loadedview = view+", "+loadedview;
			      }
			      catch(Exception e) {
			    	  notloadedview = view+", "+notloadedview;
			    	  logger.error(view,e);
			    	  e.printStackTrace();
			    	  throw e;
			      }
			      finally {
			    	 config.setProperty("ImportedViews", loadedview);
					 config.setProperty("NotImportedViews", notloadedview);
					 config.save();
			      }
			  }
			  else {
				  notloadedview = view+", "+notloadedview;
				  config.setProperty("NotImportedViews", notloadedview);
				  config.save();
			  }
			}
	}
	private void runBackupFile() throws Exception {
		config = new PropertiesConfiguration("PropertyFiles\\ViewNames.properties");
		Oracle oracle = new Oracle();
		oracle.oracleconnection();
		String views = config.getString("NotImportedViews");
		if(!views.isEmpty()) {
			String[] viewarray = views.split(",");
			for(String view : viewarray) {
				File file = new File(oracle.path+"\\"+view+".sql");
				if(file.exists()) {
					oracle.databaseStatement.executeQuery("truncate table " + view.toUpperCase());
					MyScriptRunner sr = new MyScriptRunner(oracle.databaseConnection);
				    Reader reader = new BufferedReader(new FileReader(oracle.path+"\\"+view+".sql"));
				    try {
				    	 sr.setDelimiter(";;");
				    	sr.runScript(reader);
				    }
				    catch(Exception e) {
				    	 logger.error(e);
				    	e.printStackTrace();
				    	config.setProperty("OracleNotImportedViews", view);
				    }
				}
				else {
					boolean exists = oracle.checktableExists(view);
					if(exists) {
						oracle.databaseStatement.executeQuery("drop table " + view.toUpperCase());
					}
				}
			}
		}
	}
	public static void main(String args[]) throws Exception{
		File files = new File(System.getProperty("user.dir")+"\\DatabaseBackupTool.log");
		if(files.exists())
				files.delete();
		Migrator m = new Migrator();
	}
}
