package edu.njit.itsrc.migrateSQLServertoOracle;

import java.sql.Types;

public class ColumnEntry {
	public String columnName;
	public int columnType;
	public int columnLength;
	public String oracleTableLine;
	public ColumnEntry(String name, int type, int length) {
		columnName = name;
		columnType = type;
		columnLength = length;
		oracleTableLine = buildOracleCreateTableLine();
	}

	String buildOracleCreateTableLine() {
		String dataType = "";
		if(columnType == -9 && columnLength > 4000)
			columnType = -10;
		switch (columnType) {
			case Types.VARCHAR: dataType = "VARCHAR(4000)";break;
			case Types.NVARCHAR: dataType = "VARCHAR2(4000)";break;
			case Types.INTEGER: dataType = "NUMBER";break;
			case Types.DECIMAL: dataType = "DECIMAL("+columnLength+")";break;
			case Types.CHAR: dataType = "CHAR("+columnLength+4+")";break;
			case Types.DATE: dataType = "DATE";break;
			case Types.TIMESTAMP: dataType = "TIMESTAMP";break;
			case Types.DOUBLE: dataType = "DOUBLE PRECISION";break;
			case Types.FLOAT: dataType = "DOUBLE PRECISION"; break;
			case Types.CLOB: dataType = "CLOB";break;
			case Types.BOOLEAN: dataType = "VARCHAR(4000)";break;
			case Types.BIT: dataType = "VARCHAR(4000)";break;
			case Types.TINYINT: dataType = "NUMBER";break;
			case Types.SMALLINT: dataType = "NUMBER";break;
			case Types.BIGINT: dataType = "NUMBER";break;
			case Types.REAL: dataType = "REAL"; break;
			case Types.NUMERIC: dataType = "NUMBER"; break;
			case Types.LONGNVARCHAR: dataType = "VARCHAR(4000)";break;
			case Types.TIME: dataType = "DATE";break;
			case Types.BINARY: dataType = "NUMBER";break;
			case Types.VARBINARY: dataType = "NUMBER";break;
			case Types.LONGVARBINARY: dataType = "NUMBER";break;
			case Types.BLOB: dataType = "BLOB";break;
			case -10 : dataType="CLOB"; break;
			case Types.NCHAR: dataType = "CHAR("+columnLength+4+")";break;
			case Types.NCLOB: dataType = "CLOB";break;
			case -1 : dataType="VARCHAR(4000)"; break;
			case -155: dataType = "TIMESTAMP";break;
			default: dataType = "SOMETHING IS WRONG";break;
		}
		if(columnName != null) {
			columnName = String.format("%s"+columnName+"%s", "\"","\"");
		}
		return "" + columnName + " " + dataType;
	}
	String retrunDatatype() {
		String dataType = "";
		if(columnType == -9 && columnLength > 4000)
			columnType = -10;
		switch (columnType) {
			case Types.VARCHAR: dataType = "VARCHAR(4000)";break;
			case Types.NVARCHAR: dataType = "VARCHAR2(4000)";break;
			case Types.INTEGER: dataType = "NUMBER";break;
			case Types.DECIMAL: dataType = "DECIMAL("+columnLength+")";break;
			case Types.CHAR: dataType = "CHAR("+columnLength+4+")";break;
			case Types.DATE: dataType = "DATE";break;
			case Types.TIMESTAMP: dataType = "TIMESTAMP";break;
			case Types.DOUBLE: dataType = "DOUBLE PRECISION";break;
			case Types.FLOAT: dataType = "DOUBLE PRECISION"; break;
			case Types.CLOB: dataType = "CLOB";break;
			case Types.BOOLEAN: dataType = "VARCHAR(4000)";break;
			case Types.BIT: dataType = "VARCHAR(4000)";break;
			case Types.TINYINT: dataType = "NUMBER";break;
			case Types.SMALLINT: dataType = "NUMBER";break;
			case Types.BIGINT: dataType = "NUMBER";break;
			case Types.REAL: dataType = "REAL"; break;
			case Types.NUMERIC: dataType = "NUMBER"; break;
			case Types.LONGNVARCHAR: dataType = "VARCHAR(4000)";break;
			case Types.TIME: dataType = "DATE";break;
			case Types.BINARY: dataType = "NUMBER";break;
			case Types.VARBINARY: dataType = "NUMBER";break;
			case Types.LONGVARBINARY: dataType = "NUMBER";break;
			case Types.BLOB: dataType = "BLOB";break;
			case -10 : dataType="CLOB"; break;
			case Types.NCHAR: dataType = "CHAR("+columnLength+4+")";break;
			case Types.NCLOB: dataType = "CLOB";break;
			case -1 : dataType="VARCHAR(4000)"; break;
			case -155: dataType = "TIMESTAMP";break;
			default: dataType = "SOMETHING IS WRONG";break;
		}
		return  dataType;
	}
}