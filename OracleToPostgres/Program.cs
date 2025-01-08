using Oracle.ManagedDataAccess.Client;
using OracleToPostgres.Properties;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace OracleToPostgres
{
    static class Program
    {
        static OracleConnection conn;
        static string outputDir;
        static string dbContextDir;
        // Gom thông tin bảng và khóa thành Dictionary
        static Dictionary<string, List<(string ColumnName, string DataType, int Length, int Precision, int Scale, string Nullable, bool IsPrimaryKey, List<string> ForeignKeyTables)>> extractTableInfoWithKeys;
        static void Main(string[] args)
        {
            try
            {
                string oracleConnStr = Settings.Default.OracleConnStr;
                using (conn = new OracleConnection(oracleConnStr))
                {
                    Console.WriteLine("Connecting to database");
                    conn.Open();
                    DataTable dt = Common.GetTablesWithKeys(conn, Settings.Default.SchemaName);
                    extractTableInfoWithKeys = Common.ExtractTableInfoWithKeys(dt);


                    Console.WriteLine("Creatting output directory");
                    outputDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + @"\Output\" + DateTime.Now.ToString("yyyy-MM-dd-HHmmss");
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
                    GenerateEF6DbContextWithMappings(mappingClasses);
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
                foreach (var column in table.Value)
                {
                    string pgDataType = Common.ConvertOracleToPostgresType(column.DataType, column.Length, column.Precision, column.Scale);
                    string nullable = column.Nullable == "N" ? " NOT NULL" : "";
                    string columnName = column.ColumnName.ToLower();
                    if (!columns.Contains(columnName))
                        columns.Add($"    {columnName} {pgDataType}{nullable}");
                }
                scriptBuilder.AppendLine(string.Join(",\n", columns));
                scriptBuilder.AppendLine(");\n");

                // Xác định khóa chính
                var primaryKeys = table.Value.Where(c => c.IsPrimaryKey).Select(c => c.ColumnName).ToList();
                if (primaryKeys.Count > 0)
                {
                    scriptBuilder.AppendLine($"ALTER TABLE {table.Key.ToLower()}");
                    scriptBuilder.AppendLine($"ADD PRIMARY KEY ({string.Join(",\n", primaryKeys[0]).ToLower()});\n");
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
                string tableName = table.Key;
                var columns = table.Value.Select(x => new { x.ColumnName, x.DataType, x.Nullable }).Distinct();
                var primaryKeyColumns = table.Value.Where(x => x.IsPrimaryKey);
                var foreignKeyColumns = table.Value.Where(x => x.ForeignKeyTables.Count > 0);

                var classBuilder = new StringBuilder();
                classBuilder.AppendLine("using System;");
                classBuilder.AppendLine("using System.ComponentModel.DataAnnotations;");
                classBuilder.AppendLine("using System.ComponentModel.DataAnnotations.Schema;");
                classBuilder.AppendLine();
                classBuilder.AppendLine($"public partial class {Common.ToTitleCase(table.Key)}");
                classBuilder.AppendLine("{");

                // Liệt kê các thuộc tính điều hướng cho khoá ngoại (một-nhiều) (HasMany)
                List<string> relatedTables = new List<string>();
                foreach (var relatedTable in extractTableInfoWithKeys)
                {
                    string relatedTableName = relatedTable.Key;
                    var relatedColumns = relatedTable.Value;
                    var foreignKeys = relatedColumns.Where(c => c.ForeignKeyTables.Contains(tableName)).Select(c => c.ColumnName).ToList();
                    if(foreignKeys.Any())
                    {
                        relatedTables.Add(relatedTableName);
                    }    
                }

                // Khởi tạo các thuộc tính điều hướng cho khoá ngoại (một-nhiều) (HasMany) trong contructor
                if(relatedTables.Any())
                {
                    classBuilder.AppendLine($"    public {Common.ToTitleCase(table.Key)}()");
                    classBuilder.AppendLine($"    (");
                    foreach (var relatedTable in relatedTables)
                    {
                            string tableTitleCase = Common.ToTitleCase(relatedTable);
                            classBuilder.AppendLine($"        {tableTitleCase}s = new HashSet<{tableTitleCase}>();");
                    }
                    classBuilder.AppendLine($"    )");
                }

                // Sinh các thuộc tính cột
                foreach (var column in columns)
                {
                    if (primaryKeyColumns.Select(x=>x.ColumnName).ToList().Contains(column.ColumnName))
                    {
                        classBuilder.AppendLine($"    [Key]");
                        classBuilder.AppendLine($"    [DatabaseGenerated(DatabaseGeneratedOption.None)]");
                    }
                    classBuilder.AppendLine($"    [Column(\"{column.ColumnName.ToLower()}\")]");
                    string csDataType = Common.ConvertOracleTypeToCSharpType(column.DataType, column.Nullable);
                    classBuilder.AppendLine($"    public {csDataType} {Common.ToTitleCase(column.ColumnName)} {{ get; set; }}");
                }

                classBuilder.AppendLine();

                // Sinh các thuộc tính cho khoá ngoại (nhiều-một)
                foreach (var column in foreignKeyColumns)
                {
                    foreach (var foreignTable in column.ForeignKeyTables)
                    {
                        string tableTitleCase = Common.ToTitleCase(foreignTable);
                        classBuilder.AppendLine($"    public virtual {tableTitleCase} {tableTitleCase} {{ get; set; }}");
                    }
                }

                // Sinh các thuộc tính điều hướng cho khoá ngoại (một-nhiều) (HasMany)
                foreach (var relatedTable in relatedTables)
                {
                    string tableTitleCase = Common.ToTitleCase(relatedTable);
                    classBuilder.AppendLine($"    public ICollection<{tableTitleCase}> {tableTitleCase}s {{ get; set; }}");
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
                var tableName = Common.ToTitleCase(table.Key);

                var classBuilder = new StringBuilder();
                classBuilder.AppendLine("using System.Data.Entity.ModelConfiguration;");
                classBuilder.AppendLine();
                classBuilder.AppendLine($"public class {tableName}Mapping : EntityTypeConfiguration<{tableName}>");
                classBuilder.AppendLine("{");
                classBuilder.AppendLine($"    public {tableName}Mapping()");
                classBuilder.AppendLine("    {");

                // Chuyển tên bảng thành chữ thường
                classBuilder.AppendLine($"        ToTable(\"{tableName.ToLower()}\");");

                // Xác định khóa chính
                var primaryKeys = table.Value.Where(c => c.IsPrimaryKey).Select(c => c.ColumnName).ToList();
                if (primaryKeys.Count == 1)
                {
                    classBuilder.AppendLine($"        HasKey(e => e.{Common.ToTitleCase(primaryKeys[0])});");
                }
                else if (primaryKeys.Count > 1)
                {
                    classBuilder.AppendLine($"        HasKey(e => new {{ {string.Join(", ", primaryKeys.Select(pk => "e." + Common.ToTitleCase(pk)))} }});");
                }

                // Cấu hình các thuộc tính
                var columns = table.Value.Select(x => new { x.ColumnName, x.DataType, x.Length, x.Nullable }).Distinct();
                foreach (var column in columns)
                {
                    string csDataType = Common.ConvertOracleTypeToCSharpType(column.DataType, column.Nullable);
                    string columnNameTitleCase = Common.ToTitleCase(column.ColumnName);
                    string columnNameLower = column.ColumnName.ToLower();

                    if (column.Nullable == "Y")
                    {
                        classBuilder.AppendLine($"        Property(e => e.{columnNameTitleCase}).IsOptional().HasColumnName(\"{columnNameLower}\");");
                    }
                    else
                    {
                        classBuilder.AppendLine($"        Property(e => e.{columnNameTitleCase}).IsRequired().HasColumnName(\"{columnNameLower}\");");
                    }

                    if (csDataType == "string" && column.Length > 0)
                    {
                        classBuilder.AppendLine($"        Property(e => e.{columnNameTitleCase}).HasMaxLength({column.Length});");
                    }
                }

                // Cấu hình khóa ngoại
                foreach (var column in table.Value.Where(c => c.ForeignKeyTables.Count > 0))
                {
                    foreach (var foreignKeyTable in column.ForeignKeyTables)
                    {
                        classBuilder.AppendLine($"        HasRequired(e => e.{Common.ToTitleCase(foreignKeyTable)})");
                        classBuilder.AppendLine($"            .WithMany(c => c.{tableName}s)");
                        classBuilder.AppendLine($"            .HasForeignKey(e => e.{Common.ToTitleCase(column.ColumnName)}).HasColumnName(\"{column.ColumnName.ToLower()}\");");
                    }
                }

                classBuilder.AppendLine("    }");
                classBuilder.AppendLine("}");

                mappingClasses.Add(table.Key, classBuilder.ToString());
            }

            //Save to file
            foreach (var mappingClass in mappingClasses)
            {
                string filePath = Path.Combine(dbContextDir, $"{Common.ToTitleCase(mappingClass.Key)}Mapping.cs");
                File.WriteAllText(filePath, mappingClass.Value);
            }

            return mappingClasses;
        }
        private static void GenerateEF6DbContextWithMappings(Dictionary<string, string> classDefinitions)
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
                contextBuilder.AppendLine($"        modelBuilder.Configurations.Add(new {Common.ToTitleCase(tableName)}Mapping());");
            }

            contextBuilder.AppendLine("        base.OnModelCreating(modelBuilder);");
            contextBuilder.AppendLine("    }");
            contextBuilder.AppendLine("}");

            var dbContextCode = contextBuilder.ToString();

            //Save to file
            string filePath = Path.Combine(dbContextDir, "ApplicationDbContext.cs");
            File.WriteAllText(filePath, dbContextCode);
        }
    }
}