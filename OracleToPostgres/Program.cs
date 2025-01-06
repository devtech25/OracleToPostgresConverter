using Oracle.ManagedDataAccess.Client;
using OracleToPostgres.Properties;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace OracleToPostgres
{

    internal class Program
    {
        static OracleConnection conn;
        static string schemaName;
        static string outputDir;
        static string dbContextDir;
        // Gom thông tin bảng và khóa thành Dictionary
        static Dictionary<string, List<(string ColumnName, string DataType, int Length, int Precision, int Scale, string Nullable, bool IsPrimaryKey, List<string> ForeignKeyTables)>> extractTableInfoWithKeys;
        static void Main(string[] args)
        {
            try
            {
                string oracleConnStr = Settings.Default.OracleConnStr;
                schemaName = Settings.Default.SchemaName;
                using (conn = new OracleConnection(oracleConnStr))
                {
                    Console.WriteLine("Connecting to database");
                    conn.Open();
                    DataTable dt = Common.GetTablesWithKeys(conn, schemaName);
                    extractTableInfoWithKeys = Common.ExtractTableInfoWithKeys(dt);


                    Console.WriteLine("Creatting output directory");
                    outputDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + @"\Output\" + DateTime.Now.ToString("yyyy-MM-dd-HHmmss");
                    if (!Directory.Exists(outputDir))
                        Directory.CreateDirectory(outputDir);
                    dbContextDir = Path.Combine(outputDir, "DbContext");
                    if (!Directory.Exists(dbContextDir))
                        Directory.CreateDirectory(dbContextDir);

                    Console.WriteLine("Generating Postgres Create Script...");
                    GeneratePostgresCreateScript();

                    Console.WriteLine("Generating class definitions for file...");
                    GenerateClassDefinitions();

                    Console.WriteLine("Generating Mapping Classes With Keys...");
                    var mappingClasses = GenerateMappingClassesWithKeys();

                    Console.WriteLine("Generating EF6 DbContext With Mappings...");
                    string dbContextCode = GenerateEF6DbContextWithMappings(mappingClasses);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void GeneratePostgresCreateScript()
        {
            var scriptBuilder = new StringBuilder();
            foreach (var table in extractTableInfoWithKeys)
            {
                scriptBuilder.AppendLine($"CREATE TABLE {table.Key.ToLower()} (");
                var columns = new List<string>();
                var columnsPrimary = new List<string>();
                foreach (var column in table.Value)
                {
                    string pgDataType = Common.ConvertOracleToPostgresType(column.DataType, column.Length, column.Precision, column.Scale);
                    string nullable = column.Nullable == "N" ? "NOT NULL" : "";
                    string columnName = column.ColumnName.ToLower();
                    if (!columns.Contains(columnName))
                        columns.Add($"    {columnName} {pgDataType} {nullable}");

                    if (column.IsPrimaryKey)
                        columnsPrimary.Add(column.ColumnName);
                }
                scriptBuilder.AppendLine(string.Join(",\n", columns));
                scriptBuilder.AppendLine(");\n");

                if (columnsPrimary.Count > 0)
                {
                    scriptBuilder.AppendLine($"ALTER TABLE {table.Key.ToLower()}");
                    scriptBuilder.AppendLine($"ADD PRIMARY KEY ({string.Join(",\n", columnsPrimary).ToLower()});\n");
                }
            }

            var createScript = scriptBuilder.ToString();
            // Save to file
            string fileName = Path.Combine(outputDir, "script.sql");

            File.WriteAllText(fileName, createScript);
        }
        private static void GenerateClassDefinitions()
        {
            var classDefinitions = new Dictionary<string, string>();
            foreach (var table in extractTableInfoWithKeys)
            {
                var classBuilder = new StringBuilder();
                classBuilder.AppendLine("using System;");
                classBuilder.AppendLine();
                classBuilder.AppendLine($"public partial class {Common.ToTitleCase(table.Key)}");
                classBuilder.AppendLine("{");

                var columns = table.Value.Select(x => new { x.ColumnName, x.DataType, x.Nullable }).Distinct();
                var columnsKey = table.Value.Where(x => x.IsPrimaryKey).Select(x => x.ColumnName).ToList();
                foreach (var column in columns)
                {
                    if (columnsKey.Contains(column.ColumnName))
                        classBuilder.AppendLine($"    [Key]");
                    classBuilder.AppendLine($"    [Column(\"{column.ColumnName.ToLower()}\")]");
                    string csDataType = Common.ConvertOracleTypeToCSharpType(column.DataType, column.Nullable);
                    classBuilder.AppendLine($"    public {csDataType} {Common.ToTitleCase(column.ColumnName)} {{ get; set; }}");
                }

                classBuilder.AppendLine("}");
                classDefinitions[table.Key] = classBuilder.ToString();
            }
            // Save to file
            foreach (var classDefinition in classDefinitions)
            {
                string fileName = Path.Combine(outputDir, $"{Common.ToTitleCase(classDefinition.Key)}.cs");
                File.WriteAllText(fileName, classDefinition.Value);
            }
        }

        private static Dictionary<string, string> GenerateMappingClassesWithKeys()
        {
            var mappingClasses = new Dictionary<string, string>();

            foreach (var table in extractTableInfoWithKeys)
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
                    string csDataType = Common.ConvertOracleTypeToCSharpType(column.DataType, column.Nullable);
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
                foreach (var column in table.Value.Where(c => c.ForeignKeyTables.Count > 0))
                {
                    foreach (var foreignKeyTable in column.ForeignKeyTables)
                    {
                        string foreignKeyTableLower = foreignKeyTable.ToLower();
                        classBuilder.AppendLine($"        HasRequired(e => e.{foreignKeyTable})");
                        classBuilder.AppendLine($"            .WithMany() // Điều chỉnh nếu là quan hệ một-nhiều");
                        classBuilder.AppendLine($"            .HasForeignKey(e => e.{column.ColumnName}).HasColumnName(\"{column.ColumnName.ToLower()}\");");
                    }
                }

                classBuilder.AppendLine("    }");
                classBuilder.AppendLine("}");
                //if (!mappingClasses.ContainsKey(table.Key))
                //{
                //    mappingClasses[table.Key] = classBuilder.ToString();
                //}
                mappingClasses.Add(table.Key, classBuilder.ToString());
            }

            //Save to file
            foreach (var mappingClass in mappingClasses)
            {
                string filePath = Path.Combine(dbContextDir, $"{mappingClass.Key}Mapping.cs");
                File.WriteAllText(filePath, mappingClass.Value);
            }

            return mappingClasses;
        }
        private static string GenerateEF6DbContextWithMappings(Dictionary<string, string> classDefinitions)
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

            var dbContextCode = contextBuilder.ToString();

            //Save to file
            string filePath = Path.Combine(dbContextDir, "ApplicationDbContext.cs");
            File.WriteAllText(filePath, dbContextCode);

            return dbContextCode;
        }


    }
}
