using Oracle.ManagedDataAccess.Client;
using OracleToPostgres.Properties;
using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using static System.Net.Mime.MediaTypeNames;

namespace OracleToPostgres
{
    internal class Program
    {
        static OracleConnection conn;
        static string schemaName;
        static void Main(string[] args)
        {
            try
            {
                string oracleConnStr = Settings.Default.OracleConnStr;
                schemaName = Settings.Default.SchemaName;
                using (conn = new OracleConnection(oracleConnStr))
                {
                    conn.Open();
                    string query = $@"
                    SELECT table_name, column_name, data_type, data_length, data_precision, data_scale, nullable
                    FROM all_tab_columns
                    WHERE owner = UPPER('{schemaName}')
                    ORDER BY table_name, column_id";
                    OracleCommand cmd = new OracleCommand(query, conn);
                    OracleDataAdapter adapter = new OracleDataAdapter(cmd);
                    DataTable dt = new DataTable();
                    adapter.Fill(dt);

                    string createScript = GeneratePostgresCreateScript(dt);

                    SaveScriptToFile(createScript);

                    var classDefinitions = GenerateClassDefinitionsForFile(dt);

                    SaveClassDefinitions(classDefinitions);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }
        private static string GeneratePostgresCreateScript(DataTable dt)
        {
            var scriptBuilder = new StringBuilder();
            var tables = TablesDictionary(dt);
            foreach (var table in tables)
            {
                scriptBuilder.AppendLine($"CREATE TABLE {table.Key.ToLower()} (");
                var columns = new List<string>();
                foreach (var column in table.Value)
                {
                    string pgDataType = ConvertOracleToPostgresType(column.DataType, column.Length, column.Precision, column.Scale);
                    string nullable = column.Nullable == "N" ? "NOT NULL" : "";
                    columns.Add($"    {column.ColumnName.ToLower()} {pgDataType} {nullable}");
                }
                scriptBuilder.AppendLine(string.Join(",\n", columns));
                scriptBuilder.AppendLine(");\n");
            }
            return scriptBuilder.ToString();
        }

        private static Dictionary<string, List<(string ColumnName, string DataType, int Length, int Precision, int Scale, string Nullable)>> TablesDictionary(DataTable dt)
        {
            var tables = new Dictionary<string, List<(string ColumnName, string DataType, int Length, int Precision, int Scale, string Nullable)>>();
            foreach (DataRow row in dt.Rows)
            {
                string tableName = row["TABLE_NAME"].ToString();
                string columnName = row["COLUMN_NAME"].ToString();
                string dataType = row["DATA_TYPE"].ToString();
                int length = row["DATA_LENGTH"] != DBNull.Value ? Convert.ToInt32(row["DATA_LENGTH"]) : 0;
                int precision = row["DATA_PRECISION"] != DBNull.Value ? Convert.ToInt32(row["DATA_PRECISION"]) : 0;
                int scale = row["DATA_SCALE"] != DBNull.Value ? Convert.ToInt32(row["DATA_SCALE"]) : 0;
                string nullable = row["NULLABLE"].ToString();
                if (!tables.ContainsKey(tableName))
                {
                    tables[tableName] = new List<(string ColumnName, string DataType, int Length, int Precision, int scale, string Nullable)>();
                }
                tables[tableName].Add((columnName, dataType, length, precision, scale, nullable));
            }

            return tables;
        }

        private static string ConvertOracleToPostgresType(string oracleType, int length, int precision, int scale)
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
                        return $"$\"TIMESTAMP({scale}) WITH TIME ZONE";
                    return oracleType;
            }
        }
        private static void SaveScriptToFile(string createScript)
        {
            var dir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + @"\Script";
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            string fileName = Path.Combine(dir, DateTime.Now.ToString("yyyy-MM-dd-HHmmss") + ".sql");

            File.WriteAllText(fileName, createScript);
        }
        private static Dictionary<string, string> GenerateClassDefinitionsForFile(DataTable dt)
        {
            var classDefinitions = new Dictionary<string, string>();
            var tables = TablesDictionary(dt);
            foreach (var table in tables)
            {
                var classBuilder = new StringBuilder();
                classBuilder.AppendLine("using System;");
                classBuilder.AppendLine();
                classBuilder.AppendLine($"public class {ToTitleCase(table.Key)}");
                classBuilder.AppendLine("{");

                foreach (var column in table.Value)
                {
                    string csDataType = ConvertOracleTypeToCSharpType(column.DataType, column.Precision, column.Scale, column.Nullable);
                    classBuilder.AppendLine($"    public {csDataType} {ToTitleCase(column.ColumnName)} {{ get; set; }}");
                }

                classBuilder.AppendLine("}");
                classDefinitions[table.Key] = classBuilder.ToString();
            }
            return classDefinitions;
        }
        private static string ConvertOracleTypeToCSharpType(string oracleType, int precision, int scale, string nullable)
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
                    return "String";

                // Numeric data types:
                case "BINARY_FLOAT":
                    return isNullable ? "Decimal?" : "Decimal";
                case "DECIMAL":
                    return isNullable ? "Decimal?" : "Decimal";
                case "DEC":
                    return isNullable ? "Decimal?" : "Decimal";
                case "BINARY_DOUBLE":
                case "DOUBLE PRECISION":
                case "FLOAT":
                case "REAL":
                    return isNullable ? "Decimal?" : "Decimal";
                case "INTEGER":
                case "INT":
                    return isNullable ? "int?" : "int";
                case "NUMBER":
                    return isNullable ? "Decimal?" : "Decimal";
                case "SMALLINT":
                    return isNullable ? "Int16?" : "Int16";
                case "NUMERIC":
                    return isNullable ? "Decimal?" : "Decimal";

                // LOB data types
                case "BFILE":
                    return isNullable ? "Byte[]?" : "Byte[]";

                // ROWID data types
                case "ROWID":
                    return "String";
                case "UROWID":
                    return "String";

                // XML data type
                case "XMLTYPE":
                    return "String";

                // CURSOR type
                case "SYS_REFCURSOR":
                    return "REFCURSOR";

                // Date and time data types:
                case "DATE":
                    return isNullable ? "DateTime?" : "DateTime";
                case "BLOB":
                case "RAW":
                case "LONG RAW":
                    return isNullable ? "Byte[]?" : "Byte[]";
                default:
                    if (oracleType.Equals($"TIMESTAMP({scale})"))
                        return isNullable ? "DateTime?" : "DateTime";
                    if (oracleType.Equals($"INTERVAL YEAR({precision}) TO MONTH"))
                        return isNullable ? "int?" : "int";
                    if (oracleType.Equals($"INTERVAL DAY({precision}) TO SECOND({scale})"))
                        return isNullable ? "TimeSpan?" : "TimeSpan";
                    if (oracleType.Equals($"TIMESTAMP({scale}) WITH TIME ZONE"))
                        return isNullable ? "DateTime?" : "DateTime";
                    return oracleType;
            }
        
        }
        public static string ToTitleCase(string str)
        {
            return CultureInfo.CurrentCulture.TextInfo.ToTitleCase(str.ToLower());
        }
        private static void SaveClassDefinitions(Dictionary<string, string> classDefinitions)
        {
            foreach (var classDefinition in classDefinitions)
            {
                var dir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + @"\ClassDefinition\" + DateTime.Now.ToString("yyyy-MM-dd-HHmmss");
                if (!Directory.Exists(dir))
                    Directory.CreateDirectory(dir);

                string fileName = Path.Combine(dir, $"{ToTitleCase(classDefinition.Key)}.cs");

                File.WriteAllText(fileName, classDefinition.Value);
            }
        }
        private string GenerateEF6DbContextWithMappingsLowercase(Dictionary<string, string> classDefinitions)
        {
            var contextBuilder = new StringBuilder();
            contextBuilder.AppendLine("using System.Data.Entity;");
            contextBuilder.AppendLine();
            contextBuilder.AppendLine("public class ApplicationDbContext : DbContext");
            contextBuilder.AppendLine("{");
            contextBuilder.AppendLine("    public ApplicationDbContext() : base(\"name=PostgresConnection\")");
            contextBuilder.AppendLine("    {");
            contextBuilder.AppendLine("    }");
            contextBuilder.AppendLine();
            contextBuilder.AppendLine("    protected override void OnModelCreating(DbModelBuilder modelBuilder)");
            contextBuilder.AppendLine("    {");

            foreach (var tableName in classDefinitions.Keys)
            {
                contextBuilder.AppendLine($"        modelBuilder.Configurations.Add(new {tableName}Mapping());");
            }

            contextBuilder.AppendLine("        base.OnModelCreating(modelBuilder);");
            contextBuilder.AppendLine("    }");
            contextBuilder.AppendLine("}");

            return contextBuilder.ToString();
        }
        private Dictionary<string, string> GenerateMappingClassesWithKeysLowercase(Dictionary<string, List<(string ColumnName, string DataType, int Length, int Precision, int Scale, string Nullable, bool IsPrimaryKey, string ForeignKeyTable)>> tables)
        {
            var mappingClasses = new Dictionary<string, string>();

            foreach (var table in tables)
            {
                var classBuilder = new StringBuilder();
                classBuilder.AppendLine("using System.Data.Entity.ModelConfiguration;");
                classBuilder.AppendLine();
                classBuilder.AppendLine($"public class {table.Key}Mapping : EntityTypeConfiguration<{table.Key}>");
                classBuilder.AppendLine("{");
                classBuilder.AppendLine($"    public {table.Key}Mapping()");
                classBuilder.AppendLine("    {");

                // Chuyển tên bảng thành chữ thường
                classBuilder.AppendLine($"        ToTable(\"{table.Key.ToLower()}\");");

                // Xác định khóa chính
                var primaryKeys = table.Value.Where(c => c.IsPrimaryKey).Select(c => c.ColumnName).ToList();
                if (primaryKeys.Count == 1)
                {
                    classBuilder.AppendLine($"        HasKey(e => e.{primaryKeys[0]});");
                }
                else if (primaryKeys.Count > 1)
                {
                    classBuilder.AppendLine($"        HasKey(e => new {{ {string.Join(", ", primaryKeys.Select(pk => "e." + pk))} }});");
                }

                // Cấu hình các thuộc tính
                foreach (var column in table.Value)
                {
                    string csDataType = ConvertOracleTypeToCSharpType(column.DataType,column.Precision, column.Scale, column.Nullable);
                    string columnNameLower = column.ColumnName.ToLower();

                    if (column.Nullable == "Y")
                    {
                        classBuilder.AppendLine($"        Property(e => e.{column.ColumnName}).IsOptional().HasColumnName(\"{columnNameLower}\");");
                    }
                    else
                    {
                        classBuilder.AppendLine($"        Property(e => e.{column.ColumnName}).IsRequired().HasColumnName(\"{columnNameLower}\");");
                    }

                    if (csDataType == "string" && column.Length > 0)
                    {
                        classBuilder.AppendLine($"        Property(e => e.{column.ColumnName}).HasMaxLength({column.Length});");
                    }
                }

                // Cấu hình khóa ngoại
                foreach (var column in table.Value.Where(c => !string.IsNullOrEmpty(c.ForeignKeyTable)))
                {
                    string foreignKeyTableLower = column.ForeignKeyTable.ToLower();
                    classBuilder.AppendLine($"        HasRequired(e => e.{column.ForeignKeyTable})");
                    classBuilder.AppendLine($"            .WithMany() // Điều chỉnh nếu là quan hệ một-nhiều");
                    classBuilder.AppendLine($"            .HasForeignKey(e => e.{column.ColumnName}).HasColumnName(\"{foreignKeyTableLower}\");");
                }

                classBuilder.AppendLine("    }");
                classBuilder.AppendLine("}");
                mappingClasses[table.Key] = classBuilder.ToString();
            }

            return mappingClasses;
        }

        private static void SaveClassesAndMappingsWithKeys()
        {
            string query = $@"
                    SELECT table_name, column_name, data_type, data_length, nullable, constraint_type, r_constraint_name, r_table_name
                    FROM (
                        SELECT cols.table_name, cols.column_name, cols.data_type, cols.data_length, cols.nullable,
                               cons.constraint_type,
                               cons.r_constraint_name,
                               (SELECT table_name FROM all_constraints WHERE constraint_name = cons.r_constraint_name) AS r_table_name
                        FROM all_tab_columns cols
                        LEFT JOIN all_constraints cons
                        ON cols.table_name = cons.table_name AND cons.owner = cols.owner
                        WHERE cols.owner = UPPER('{schemaName}')
                    )
                    ORDER BY table_name, column_id";

            OracleCommand cmd = new OracleCommand(query, conn);
            OracleDataAdapter adapter = new OracleDataAdapter(cmd);
            DataTable dt = new DataTable();
            adapter.Fill(dt);

            //    var tables = ExtractTableInfoWithKeys(dt); // Hàm gom thông tin bảng và khóa thành Dictionary
            //    var mappingClasses = GenerateMappingClassesWithKeysLowercase(tables);
            //    SaveMappingClasses(folderPath, mappingClasses);

            //    string dbContextCode = GenerateEF6DbContextWithMappingsLowercase(tables);
            //SaveEF6DbContextWithMappings(folderPath, dbContextCode);
        }
    }
}
