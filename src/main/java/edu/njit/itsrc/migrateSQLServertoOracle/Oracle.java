package edu.njit.itsrc.migrateSQLServertoOracle;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import edu.njit.itsrc.migrateSQLServertoOracle.ColumnEntry;;

public class Oracle {
	 private static final Logger logger = LogManager.getLogger(Oracle.class);
	    private String databaseType;
	    private String databaseDriver;
	    private String databaseConnectionString;
	    private String databaseSchema;
		private String viewnames;
	    public Connection databaseConnection;
	    private DatabaseMetaData databaseMetaData;
	    public Statement databaseStatement;
	    public String path;
	    Parameters params = new Parameters();
 		FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
     		new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
     			.configure(params.properties()
                     .setFileName("PropertyFiles\\DatabaseBackupTool.properties"));
         FileBasedConfiguration config ;
         FileBasedConfigurationBuilder<FileBasedConfiguration> builder1 =
         		new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
         			.configure(params.properties()
             			.setFileName("PropertyFiles\\MSSQLToOracleMigrator.properties"));
     	FileBasedConfiguration config1 ;
     	//SQLServer sql;
     	public Oracle() throws Exception{
     		logger.info("Loading Configuration Data");
	    	 config = builder.getConfiguration();
	         databaseType 		= config.getString("Database.Type");
	         databaseDriver 		= config.getString("Database.Driver");
	         databaseConnectionString 		= config.getString("Database.ConnectionString");
	         databaseSchema 		= config.getString("Database.Schema");
	         path 				= config.getString("ScriptFilesPath");
	         //sql = new SQLServer();
	         //sql.Sqlserverconnection();
     	}
     	private void deleteScriptFiles(String path) {
    		File directory = new File(path+"\\");
    		File[] files = directory.listFiles();
    		for (File file : files) {
    			if(file.exists())
    				file.delete();
    		}
    	}
	    public void oracleconnection() throws Exception {
	         Class.forName(databaseDriver);
	         databaseConnection = DriverManager.getConnection(databaseConnectionString);
	         databaseMetaData = databaseConnection.getMetaData();
	         databaseStatement = databaseConnection.createStatement();
	         //( (oracle.jdbc.driver.OracleConnection)databaseConnection ).setIncludeSynonyms(true);
	    }
	    public void performsViewBackup() throws Exception {
	    	deleteScriptFiles(path);
	    	 config1 = builder1.getConfiguration();
	    	viewnames			= config1.getString("Migration.ViewNames");
	    	if(viewnames == null || viewnames.length() ==0) {
	    		//viewnames =sql.getAllTablesFromDatabase();
	    	}
			logger.info("Migration.TableNames : " + viewnames);
			String[] viewArray = viewnames.split(",");
			for(String viewName:viewArray) {
				boolean isexist = checktableExists(viewName);
				//boolean isexistinsql = sql.checkifViewExists(viewName);
				if(isexist ) {//isexistinsql) {
				 FileOutputStream fos = new FileOutputStream(path+"\\"+viewName+".sql");
				 	//downloadTableDefinition(fos,viewName);
		            downloadTableData(viewName,fos);
		            databaseStatement.executeQuery("truncate table " + viewName);
				}
				else {
					logger.info("table doesnot exist in the either oracle database or SQLServer");
					logger.info("is table present in oracle............................"+isexist);
					//logger.info("is table present in SQLServer...................."+isexistinsql);
				}
			}
			
	    }
	    private void downloadTableDefinition(FileOutputStream fos, String viewName) throws SQLException, IOException {
	             ResultSet tableDDL = databaseStatement.executeQuery(" select DBMS_METADATA.GET_DDL('TABLE','"+viewName.toUpperCase()+"') from DUAL");
	             if (tableDDL.next()) {
	                 //logger.info(tableDDL.getString(1));
	                 write(fos, tableDDL.getString(1) + "\n\n");
	             }
	             write(fos,";;");
	             tableDDL.close();
		}
		private void downloadTableData(String viewname , OutputStream os) throws Exception {
	        write(os, "/* view data */\n");
	         try {
                ResultSet columns = databaseMetaData.getColumns(null, null, viewname.toUpperCase(), null);
                ArrayList<ColumnEntry> columnData = new ArrayList<>();
                while (columns.next()) {
                    columnData.add(new ColumnEntry(columns.getString("COLUMN_NAME"), columns.getInt("DATA_TYPE"), columns.getInt("COLUMN_SIZE")));
                }
                ResultSet data = databaseStatement.executeQuery("SELECT * FROM " + viewname);
                int rowCount = 0;
                while (data.next()) {
                    rowCount++;
                    StringBuilder sqlLine = new StringBuilder("INSERT INTO " + viewname);
                    sqlLine.append(" VALUES (");
                    for (int i = 0; i < columnData.size(); i++) {
                        ColumnEntry column = columnData.get(i);
                        if(data.getString(i+1) != null && !data.getString(i+1).contains("null") && column.columnType != Types.CLOB) {
	                        switch (column.columnType) {
	                            case Types.VARCHAR: 	sqlLine.append("q'["+data.getString(i+1)+" ]'");    break;
	                            case Types.NVARCHAR: 	sqlLine.append("q'["+data.getString(i+1)+" ]'");    break;
	                            case Types.INTEGER: 	sqlLine.append(data.getInt(i+1));    break;
	                            case Types.DECIMAL: 	sqlLine.append("'"+data.getString(i+1)+"'");    break;
	                            case Types.CHAR: 		sqlLine.append("'"+data.getString(i+1)+"'");    break;
	                            case Types.DATE: 		sqlLine.append("'"+data.getString(i+1)+"'");    break;
	                            case Types.TIMESTAMP: 	sqlLine.append("TIMESTAMP '"+data.getString(i+1)+"'");    break;
	                            case Types.BLOB:        sqlLine.append("'"+data.getBlob(i+1)+"'"); break;
	                            case Types.CLOB: 		sqlLine.append("TO_CLOB('"+data.getString(i+1)+"')"); break;
	                            default: 			    sqlLine.append("'"+data.getString(i+1)+"'");    break;
	                        }
                        }
                        else if(column.columnType == Types.CLOB && data.getString(i+1)!=null) {
                        	String[] arr = data.getString(i+1).split("(?<=\\G.{3000})");
							String dat ="";
							for(String a:arr) {
								if(a.contains("]'")) {
									String[] ar = a.split("]'");
									for(String aa : ar) {
										dat =dat+"TO_CLOB(q\'["+aa+" ]\')||+TO_CLOB(']\'\'')||";
									}
									dat=dat.substring(0,dat.length()-17);
								}
								else {
									dat=dat+"TO_CLOB(q\'["+a+" ]\')||";
								}
							}
							dat=dat.substring(0,dat.length()-2);
							sqlLine.append(dat);
                        }
                        else {
                        	sqlLine.append(""+data.getString(i+1)+"");
                        }
                        
                        if (i==columnData.size()-1) {
                            sqlLine.append("");
                        } else {
                            sqlLine.append(",");
                        }
                    }
                    sqlLine.append(");;");
                    write(os, sqlLine + "\n");
                    if (rowCount%1000==0) logger.info("Parsed row " + rowCount);
                }
                data.close();
            } catch (SQLException e) {
                write(os, "/* Table could not be downloaded because of an SQLException, see logs for more info */\n");
                e.printStackTrace();
                logger.error(e);
            }
	    }
		public boolean checktableExists(String viewName) {
			logger.info("[boolean existViewInDB(String viewName[" + viewName
			        + "])]");
			    boolean existView = false;
			    try {
			        ResultSet query = databaseStatement.executeQuery("SELECT count(*) FROM "
			        		+viewName);
			        if(query.next())
			        	existView = true;
			    } catch (Exception e) {
			        if(e.toString().contains("table or view does not exist"))
			        	return existView;
			        else{
			        	e.printStackTrace();
			        	 logger.error(e, e);
			        }
			    }
			    logger.info("Exist View [" + viewName + "] ? -> " + existView);
			    return existView;
		}
		public void write(OutputStream os, String text) throws IOException {
	        byte[] b = text.getBytes();
	        os.write(b);
	    }
		public String runMySQLScript() throws ConfigurationException, SQLException {
		   config1 = builder1.getConfiguration();
		   viewnames = config1.getString("Migration.ViewNames");
		   if(viewnames == null || viewnames.length() ==0) {
	    		//viewnames =sql.getAllTablesFromDatabase();
	    	}
		   return viewnames;
		}
}
