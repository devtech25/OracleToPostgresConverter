using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;

namespace OracleToPostgres
{
    public static class Common
    {
        public static DataTable GetTablesWithKeys(OracleConnection conn, string schemaName)
        {
            string query = $@"
                 SELECT distinct * FROM (
                   SELECT cols.OWNER a, ccols.OWNER b , cons.OWNER c, rcons.OWNER d, ccols.POSITION, cols.COLUMN_ID,
                       cols.TABLE_NAME, 
                       cols.COLUMN_NAME, 
                       cols.DATA_TYPE, 
                       cols.DATA_LENGTH, 
                       cols.NULLABLE, 
                       cols.data_precision, 
                       cols.data_scale,
                       cons.CONSTRAINT_TYPE, 
                       rcons.constraint_name AS r_constraint_name,
                       rcons.TABLE_NAME AS R_TABLE_NAME
                   FROM 
                       ALL_TAB_COLUMNS cols
                   LEFT JOIN ALL_CONS_COLUMNS ccols 
                       ON cols.TABLE_NAME = ccols.TABLE_NAME AND cols.COLUMN_NAME = ccols.COLUMN_NAME and ccols.OWNER = '{schemaName}'
                   LEFT JOIN ALL_CONSTRAINTS cons 
                       ON ccols.CONSTRAINT_NAME = cons.CONSTRAINT_NAME and cons.OWNER = '{schemaName}'
                   LEFT JOIN ALL_CONSTRAINTS rcons 
                       ON cons.R_CONSTRAINT_NAME = rcons.CONSTRAINT_NAME and rcons.OWNER = '{schemaName}'
                   WHERE 
                       cols.OWNER = '{schemaName}'
                )
                ORDER BY table_name, column_id";

            OracleCommand cmd = new OracleCommand(query, conn);
            OracleDataAdapter adapter = new OracleDataAdapter(cmd);
            DataTable dt = new DataTable();
            adapter.Fill(dt);
            return dt;
        }
        public static string ConvertOracleToPostgresType(string oracleType, int length, int precision, int scale)
        {
            switch (oracleType)
            {
                // Character data types:
                case "CHAR":
                    return length > 0 ? $"CHAR({length})" : "CHAR";
                case "CHARACTER":
                    return length > 0 ? $"CHARACTER({length})" : "CHARACTER";
                case "NCHAR":
                    return length > 0 ? $"NCHAR({length})" : "NCHAR";
                case "NCHAR VARYING":
                case "VARCHAR":
                case "VARCHAR2":
                case "NVARCHAR2":
                    return length > 0 ? $"VARCHAR({length})" : "VARCHAR";
                case "CLOB":
                case "NCLOB":
                case "LONG":
                    return "TEXT";

                // Numeric data types:
                case "BINARY_FLOAT":
                    return "REAL";
                case "DECIMAL":
                    return $"DECIMAL({precision},{scale})";
                case "DEC":
                    return $"DEC({precision},{scale})";
                case "BINARY_DOUBLE":
                case "DOUBLE PRECISION":
                case "FLOAT":
                case "REAL":
                    return "DOUBLE PRECISION";
                case "INTEGER":
                case "INT":
                    return "INTEGER";
                case "NUMBER":
                    if (scale <= 0)
                    {
                        if (precision < 5 && precision >= 1)
                            return "SMALLINT";
                        if (precision < 9 && precision >= 5)
                            return "INT";
                        if (precision < 19 && precision >= 9)
                            return "BIGINT";
                        if (precision <= 38 && precision >= 19)
                            return $"DECIMAL({precision})";
                    }
                    return precision > 0 ? $"DECIMAL({precision},{scale})" : "DECIMAL";
                case "SMALLINT":
                    return "SMALLINT";
                case "NUMERIC":
                    return $"NUMERIC({precision},{scale})";

                // LOB data types
                case "BFILE":
                    return "VARCHAR(255)";

                // ROWID data types
                case "ROWID":
                    return "CHARACTER(255)";
                case "UROWID":
                    return length > 0 ? $"VARCHAR({length})" : "VARCHAR";

                // XML data type
                case "XMLTYPE":
                    return "XML";

                // CURSOR type
                case "SYS_REFCURSOR":
                    return "REFCURSOR";

                // Date and time data types:
                case "DATE":
                    return "TIMESTAMP";
                case "BLOB":
                case "RAW":
                case "LONG RAW":
                    return "BYTEA";
                default:
                    if (oracleType.Equals($"TIMESTAMP({scale})"))
                        return $"TIMESTAMP({scale})";
                    if (oracleType.Equals($"INTERVAL YEAR({precision}) TO MONTH"))
                        return $"INTERVAL YEAR TO MONTH";
                    if (oracleType.Equals($"INTERVAL DAY({precision}) TO SECOND({scale})"))
                        return $"INTERVAL DAY TO SECOND({scale})";
                    if (oracleType.Equals($"TIMESTAMP({scale}) WITH TIME ZONE"))
                        return $"TIMESTAMP({scale}) WITH TIME ZONE";
                    return oracleType;
            }
        }
        public static string ConvertOracleTypeToCSharpType(string oracleType, string nullable)
        {
            bool isNullable = nullable != "N";

            switch (oracleType)
            {
                // Character data types:
                case "CHAR":
                case "CHARACTER":
                case "NCHAR":
                case "NCHAR VARYING":
                case "VARCHAR":
                case "VARCHAR2":
                case "NVARCHAR2":
                case "CLOB":
                case "NCLOB":
                case "LONG":
                    return "string";

                // Numeric data types:
                case "BINARY_FLOAT":
                    return isNullable ? "decimal?" : "decimal";
                case "DECIMAL":
                    return isNullable ? "decimal?" : "decimal";
                case "DEC":
                    return isNullable ? "decimal?" : "decimal";
                case "BINARY_DOUBLE":
                case "DOUBLE PRECISION":
                case "FLOAT":
                case "REAL":
                    return isNullable ? "decimal?" : "decimal";
                case "INTEGER":
                case "INT":
                    return isNullable ? "int?" : "int";
                case "NUMBER":
                    return isNullable ? "decimal?" : "decimal";
                case "SMALLINT":
                    return isNullable ? "Int16?" : "Int16";
                case "NUMERIC":
                    return isNullable ? "decimal?" : "decimal";

                // LOB data types
                case "BFILE":
                    return "byte[]";

                // ROWID data types
                case "ROWID":
                    return "string";
                case "UROWID":
                    return "string";

                // XML data type
                case "XMLTYPE":
                    return "string";


                // Date and time data types:
                case "DATE":
                    return isNullable ? "DateTime?" : "DateTime";
                case "BLOB":
                case "RAW":
                case "LONG RAW":
                    return "byte[]";
                default:
                    if (oracleType.Contains("TIMESTAMP"))
                        return isNullable ? "DateTime?" : "DateTime";
                    if (oracleType.Contains($"INTERVAL YEAR"))
                        return isNullable ? "int?" : "int";
                    if (oracleType.Contains($"INTERVAL DAY"))
                        return isNullable ? "TimeSpan?" : "TimeSpan";
                    return oracleType;
            }

        }
        public static string ToTitleCase(string str)
        {
            return CultureInfo.CurrentCulture.TextInfo.ToTitleCase(str.ToLower());
        }
        public static Dictionary<string, List<(string ColumnName, string DataType, int Length, int Precision, int Scale, string Nullable, bool IsPrimaryKey, List<string> ForeignKeyTables)>> ExtractTableInfoWithKeys(DataTable dt)
        {
            var tables = new Dictionary<string, List<(string ColumnName, string DataType, int Length, int Precision, int Scale, string Nullable, bool IsPrimaryKey, List<string> ForeignKeyTables)>>();

            var primaryKeyConstraints = dt.AsEnumerable()
                .Where(row => row["CONSTRAINT_TYPE"].ToString() == "P")
                .GroupBy(row => row["TABLE_NAME"].ToString())
                .ToDictionary(g => g.Key, g => g.Select(row => row["COLUMN_NAME"].ToString()).ToList());

            var foreignKeyConstraints = dt.AsEnumerable()
                .Where(row => row["CONSTRAINT_TYPE"].ToString() == "R")
                .GroupBy(row => row["TABLE_NAME"].ToString())
                .ToDictionary(
                    g => g.Key,
                    g => g.GroupBy(row => row["COLUMN_NAME"].ToString())
                          .ToDictionary(
                              subG => subG.Key,
                              subG => subG.Select(row => row["R_TABLE_NAME"].ToString()).ToList()
                          )
                );

            foreach (DataRow row in dt.Rows)
            {
                string tableName = row["TABLE_NAME"].ToString();
                string columnName = row["COLUMN_NAME"].ToString();
                string dataType = row["DATA_TYPE"].ToString();
                int length = row["DATA_LENGTH"] != DBNull.Value ? Convert.ToInt32(row["DATA_LENGTH"]) : 0;
                int precision = row["DATA_PRECISION"] != DBNull.Value ? Convert.ToInt32(row["DATA_PRECISION"]) : 0;
                int scale = row["DATA_SCALE"] != DBNull.Value ? Convert.ToInt32(row["DATA_SCALE"]) : 0;
                string nullable = row["NULLABLE"].ToString();
                string constraint_type = row["CONSTRAINT_TYPE"].ToString();
                bool isPrimaryKey = constraint_type == "P" && primaryKeyConstraints.ContainsKey(tableName) && primaryKeyConstraints[tableName].Contains(columnName);
                List<string> foreignKeyTables = foreignKeyConstraints.ContainsKey(tableName) && foreignKeyConstraints[tableName].ContainsKey(columnName)
                    ? foreignKeyConstraints[tableName][columnName]
                    : new List<string>();

                if (!tables.ContainsKey(tableName))
                {
                    tables[tableName] = new List<(string ColumnName, string DataType, int Length, int Precision, int Scale, string Nullable, bool IsPrimaryKey, List<string> ForeignKeyTables)>();
                }
                var item = (columnName, dataType, length, precision, scale, nullable, isPrimaryKey, foreignKeyTables);
                if (!tables[tableName].Contains(item))
                    tables[tableName].Add(item);
            }

            return tables;
        }
    }
}
