package edu.njit.itsrc.migrateSQLServertoOracle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import edu.njit.itsrc.migrateSQLServertoOracle.ColumnEntry;

public class SQLServer {
	private static final Logger logger = LogManager.getLogger(SQLServer.class);

	private String mssqlDriver, mssqlServerURL, mssqlServerPort, mssqlDatabaseName, mssqlUsername, mssqlPassword;
	private DatabaseMetaData mssqlDatabaseMetaData;
	private String viewnames;
	private Connection mssqlConnection;
	private static String mssqlConnectionString = "";
	public String path ;
	String names = "";
	Parameters params = new Parameters();
	FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
    		new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
    			.configure(params.properties()
        			.setFileName("PropertyFiles\\MSSQLToOracleMigrator.properties"));
	FileBasedConfiguration config ;
	
	public void Sqlserverconnection() {
		logger.info("Loading Configuration Data");
				try
		{
			config = builder.getConfiguration();
			mssqlDriver 		= config.getString("MSSQL.Driver");
			mssqlServerURL 		= config.getString("MSSQL.ServerURL");
			mssqlServerPort 	= config.getString("MSSQL.ServerPort");
			mssqlDatabaseName 	= config.getString("MSSQL.DBName");
			mssqlUsername 		= config.getString("MSSQL.Username");
			mssqlPassword 		= config.getString("MSSQL.Password");
			path 				= config.getString("ScriptFilesPath");
			logger.info("MSSQL.Driver : " + mssqlDriver);
			logger.info("MSSQL.ServerURL : " + mssqlServerURL);
			logger.info("MSSQL.ServerPort : " + mssqlServerPort);
			logger.info("MSSQL.DBName : " + mssqlDatabaseName);
			logger.info("MSSQL.Username : " + mssqlUsername);
			logger.info("MSSQL.Password : " + mssqlPassword);
			
		}
		catch(Exception cex)
		{
			cex.printStackTrace();
			
		}
		logger.info("Initializing database connections");
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			mssqlConnectionString = "jdbc:sqlserver://localhost;databaseName=ECAP;integratedSecurity=true";
			//mssqlConnectionString = "jdbc:sqlserver://"+mssqlServerURL+":"+mssqlServerPort+";databaseName="+mssqlDatabaseName+";user="+mssqlUsername+";password="+mssqlPassword+"";
			logger.info("Microsoft SQL Server Connection String: " + mssqlConnectionString);
			mssqlConnection = DriverManager.getConnection(mssqlConnectionString);
			mssqlDatabaseMetaData = mssqlConnection.getMetaData();
		} catch (SQLException e) {
			logger.error(e);
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			logger.error(e);
			e.printStackTrace();
		}
		logger.info("Finished Loading Configuration Data");
	}
	private void deleteScriptFiles(String path) {
		File directory = new File(path+"\\");
		File[] files = directory.listFiles();
		for (File file : files) {
			if(file.exists())
				file.delete();
		}
	}
	public void getViewInformation() {
		try {
			deleteScriptFiles(path);
			viewnames			= config.getString("Migration.ViewNames");
			if(viewnames == null || viewnames.length()==0) {
				viewnames = getAllTablesFromDatabase();
				config.setProperty("Migration.ViewNames", viewnames);
			}
			logger.info("Migration.TableNames : " + viewnames);
			String[] viewArray = viewnames.split(",");
			for(String viewName:viewArray) {
				boolean exist= checkifViewExists(viewName);
				if(exist) {
					ArrayList<ColumnEntry> columnData = new ArrayList<>();
					FileWriter mssqlDump = new FileWriter(path+"\\"+viewName+".sql");
					Oracle oracle = new Oracle();
					oracle.oracleconnection();
					boolean existsinOracle = oracle.checktableExists(viewName);
					ResultSet rs = mssqlDatabaseMetaData.getColumns(null, null, viewName, null);
					while (rs.next()) {
						columnData.add(new ColumnEntry(rs.getString("COLUMN_NAME"), rs.getInt("DATA_TYPE"), rs.getInt("COLUMN_SIZE")));
					}
					if(!existsinOracle)
						getViewSchemaInformation(viewName,mssqlDump,columnData);
					
					//StringBuilder preparedSQL = new StringBuilder("INSERT INTO \"" + oracleTablePrefix + "_" + tableName + "\" (");
				//	mssqlDump.write("set define off;"+"\n");
					StringBuilder preparedSQL = new StringBuilder("INSERT INTO " + viewName);
					preparedSQL.append(" VALUES (");
					for (int i = 0; i < columnData.size(); i++) {
						ColumnEntry column = columnData.get(i);
						preparedSQL.append("%"+(i+1)+"$s");
						if (i==columnData.size()-1) {
							preparedSQL.append("");
						} else {
							preparedSQL.append(",");
						}
					}
					preparedSQL.append(");;");
					//mssqlDump.write(preparedSQL.toString());
					//copy data
	
					String mssqlSelectQuery = "SELECT * FROM " + viewName + "";
					logger.info("Copying data: "+mssqlSelectQuery);
					ResultSet dataRS = mssqlConnection.createStatement().executeQuery(mssqlSelectQuery);
					System.out.println(dataRS);
					int rowCount = 0;
					String[] data = null;
					while (dataRS.next()) {
						rowCount++;
						data = new String[columnData.size()];
						//System.out.println("RS next");
						for (int i = 0; i < columnData.size(); i++) {
							ColumnEntry column = columnData.get(i);
							if(column.columnName != null) {
								column.columnName = column.columnName.replace("\"", "");
							}
							data[i] = dataRS.getString(column.columnName);
							if(data[i] != null && !data[i].contains("null") && (column.columnType == Types.NVARCHAR|| column.columnType == Types.VARCHAR || column.columnType == -10 ) && column.columnLength > 4000) {
								if(data[i].contains("'"))
									data[i]=data[i].replace("'", "''");
								String[] arr = data[i].split("(?<=\\G.{3000})");
								String str = data[i];
								data[i]="";
								for(String a:arr) {
									if(a.contains(")")) {
										String[] ar = a.split("\\)");
										String val = "";
										for(String aa : ar) {
											val=val+"TO_CLOB(\'"+aa+"\')||"+"TO_CLOB(')')||";
										}
										if(!str.substring(str.length()-1).equals(")"))
											data[i]=val.substring(0,val.length()-14);
										else
											data[i]=val;
									}
									else {
										data[i]=data[i]+"TO_CLOB(\'"+a+"\')||";
									}
									System.out.println(rowCount);
									if(rowCount == 84)
										System.out.println(rowCount);
									data[i]=data[i].substring(0,data[i].length()-2);
								}
							}
							else if(data[i] != null && !data[i].contains("null") && (column.columnType == Types.NVARCHAR ||column.columnType == Types.BOOLEAN||column.columnType == Types.BIT||column.columnType == Types.LONGNVARCHAR||column.columnType == -1
									||column.columnType == Types.CHAR || column.columnType == -10 || column.columnType == Types.NCLOB||column.columnType == Types.NCHAR)) {
								if(data[i].contains("'"))
									data[i]=data[i].replace("'", "''");
								if(data[i].contains(",")) {
									String str = data[i];
									String[] ar = data[i].split(",");
									String val="";
									for(String aa : ar) {
										val=val+"TO_CLOB(\'"+aa+"\')||"+"TO_CLOB(',')||";
									}
									if(str.substring(str.length()-1).equals(","))
										data[i]=val.substring(0,val.length()-2);
									else
										data[i]=val.substring(0,val.length()-16);
								}
								else {
									data[i]=String.format("\'%s\'", data[i]);
								}
							}
							else if(data[i] == null)
								data[i] = "null";
							else if(data[i].contains("(null)"))
								data[i] = "null";
							else if(data[i] != null && !data[i].contains("null")&&(column.columnType == Types.TIMESTAMP ||column.columnType == -155 ||column.columnType == Types.DATE)) {
								//data[i]=String.format("TO_DATE(\'%s\',\'YYYY-MM-DD HH24:MI:SS\')", dataRS.getTimestamp(column.columnName));
								data[i]=String.format("TIMESTAMP \'%s\'", dataRS.getTimestamp(column.columnName));
							}
							else
								data[i]=String.format("%s", data[i]);
						}
						String s = String.format(preparedSQL.toString(), data);
						mssqlDump.write(""+s + "\n");
						}
					mssqlDump.close();
					dataRS.close();
					rs.close();
					logger.info("Parsed " + rowCount + " rows");
					logger.info("Executing ...");
					logger.info("DONE!");
				}
				else {
					logger.info(viewName+"View does not exist in database");
					logger.info("view name is adding as not importing views in property file");
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	
	}
	private void getViewSchemaInformation(String viewName, FileWriter mssqlDump, ArrayList<ColumnEntry> columnData) throws SQLException, IOException {
		logger.info("Building destination table ...");
		StringBuilder createTableSQL = new StringBuilder("CREATE TABLE " + viewName + " (\n");
		for (int i = 0; i < columnData.size(); i++) {
			ColumnEntry column = columnData.get(i);
			String col = column.columnName;
			String data = column.retrunDatatype();
			if(col!=null && col.contains(" "))
				col = col.replace(" ", "_");
			String val = "" + col.toUpperCase() + " " + data;
			createTableSQL.append(val);
			if (i==columnData.size()-1) {
				createTableSQL.append("\n");
			} else {
				createTableSQL.append(",\n");
			}
		}
		createTableSQL.append(");;");
		mssqlDump.write(createTableSQL.toString() + "\n");
		logger.info("Creating prepared statement");
	}
	public boolean checkifViewExists(String viewName) {
		logger.info("[boolean existViewInDB(String viewName[" + viewName
		        + "])]");
		    boolean existView = false;
		    try {
		        ResultSet query = mssqlConnection.createStatement().executeQuery("SELECT count(*) FROM "
		        		+viewName);
		        if(query.next())
		        	existView = true;
		    } catch (Exception e) {
		        if(e.toString().contains("Invalid object name"))
		        	return existView;
		        else{
		        	e.printStackTrace();
		        	 logger.error(e, e);
		        }
		    }
		    logger.info("Exist View [" + viewName + "] ? -> " + existView);
		    return existView;
	}
	public String getAllTablesFromDatabase() throws SQLException {
		String[] types = {"VIEW"};
        ResultSet views = mssqlDatabaseMetaData.getTables(mssqlDatabaseName, "dbo", null, types);
        while(views.next()) {
        	names = views.getString("TABLE_NAME")+","+names;
        }
		return names.substring(0, names.length()-1);
	}
	public void write(OutputStream os, String text) throws IOException {
        byte[] b = text.getBytes();
        os.write(b);
    }

}
