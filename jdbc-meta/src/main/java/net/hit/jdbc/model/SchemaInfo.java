package net.hit.jdbc.model;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the Schema of a Database
 * 
 * @author edaigneault
 *
 */
public class SchemaInfo {
	private String					schemaName;
	private String					catalogName;

	private List<TableInfo>	tableInfoList;

	/**
	 * Factory Method to create a SchemaInfo from a DatabaseMetaData
	 * 
	 * @param dmd
	 *          the DatabaseMEtaData to create the SchemaInfo from
	 * @return the list of schemaInfo viewable form this database
	 * @throws SQLException
	 *           ho well!
	 */
	public static List<SchemaInfo> fromDatabaseMetaData(DatabaseMetaData dmd) throws SQLException
	{
		ResultSet schemaRs = dmd.getSchemas();
		List<SchemaInfo> siList = new ArrayList<>();
		List<ColumnInfo> schemaColumns = ColumnInfo.fromResultSet(schemaRs);
		Map<String, List<ColumnInfo>> tableColumnList = new HashMap<>();
		while (schemaRs.next())
		{
			SchemaInfo si = new SchemaInfo();
			for (ColumnInfo ci : schemaColumns) {
				switch (ci.getColumnName())
				{
					case "TABLE_SCHEM":
						si.schemaName = schemaRs.getString(ci.getColumnName());
						break;
					case "TABLE_CATALOG":
						si.catalogName = schemaRs.getString(ci.getColumnName());
						break;
				}
			}

			// now get the table list for this schema
			ResultSet tableRs = dmd.getTables(si.catalogName, si.schemaName, null, null);
			si.tableInfoList = TableInfo.fromDatabaseMetaDataResultSet(tableRs);

			// now for each table, get the columnInfoList and map it
			for (TableInfo info : si.tableInfoList) {
				ResultSet rsCol = dmd.getColumns(si.catalogName, si.schemaName, info.getTableName(), null);
				tableColumnList.put(info.getTableName(), ColumnInfo.fromDatabaseMetaDataResultSet(rsCol));
				info.setColumnInfoList(tableColumnList.get(info.getTableName()));
			}
			siList.add(si);
		}

		return siList;
	}

	/**
	 * @return the name of the schema
	 */
	public String getSchemaName() {
		return schemaName;
	}

	/**
	 * @return the Catalog's name
	 */
	public String getCatalogName() {
		return catalogName;
	}

	/**
	 * @return list of TableInfo available in this Schema
	 */
	public List<TableInfo> getTableInfoList() {
		return tableInfoList;
	}

	@Override
	public String toString() {
		return "SchemaInfo [schemaName=" + schemaName + ", catalogName=" + catalogName + "]";
	}
}
