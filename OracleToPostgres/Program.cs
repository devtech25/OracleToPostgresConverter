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
                    DataTable dt = Common.GetTables(conn, schemaName);
                    Console.WriteLine("Generating Postgres Create Script...");
                    string createScript = GeneratePostgresCreateScript(dt);

                    Console.WriteLine("Saving Postgres Create Script...");
                    SavePostgresCreateScriptToFile(createScript);

                    Console.WriteLine("Generating class definitions for file...");
                    var classDefinitions = GenerateClassDefinitions(dt);

                    Console.WriteLine("Saving class definitions...");
                    SaveClassDefinitionsToFile(classDefinitions);

                    Console.WriteLine("Generating Mapping Classes With Keys...");
                    dt = Common.GetTablesWithKeys(conn, schemaName);
                    var mappingClasses = GenerateMappingClassesWithKeys(dt);

                    Console.WriteLine("Saving Mapping Classes...");
                    SaveMappingClassesWithKeys(mappingClasses);

                    Console.WriteLine("Generating EF6 DbContext With Mappings...");
                    string dbContextCode = GenerateEF6DbContextWithMappings(mappingClasses);

                    Console.WriteLine("Saving EF6 DbContext With Mappings...");
                    SaveEF6DbContextWithMappings(dbContextCode);
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
            var tables = Common.TablesDictionary(dt);
            foreach (var table in tables)
            {
                scriptBuilder.AppendLine($"CREATE TABLE {table.Key.ToLower()} (");
                var columns = new List<string>();
                foreach (var column in table.Value)
                {
                    string pgDataType = Common.ConvertOracleToPostgresType(column.DataType, column.Length, column.Precision, column.Scale);
                    string nullable = column.Nullable == "N" ? "NOT NULL" : "";
                    columns.Add($"    {column.ColumnName.ToLower()} {pgDataType} {nullable}");
                }
                scriptBuilder.AppendLine(string.Join(",\n", columns));
                scriptBuilder.AppendLine(");\n");
            }
            return scriptBuilder.ToString();
        }


        private static void SavePostgresCreateScriptToFile(string createScript)
        {
            var dir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + @"\Script";
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            string fileName = Path.Combine(dir, DateTime.Now.ToString("yyyy-MM-dd-HHmmss") + ".sql");

            File.WriteAllText(fileName, createScript);
        }
        private static Dictionary<string, string> GenerateClassDefinitions(DataTable dt)
        {
            var classDefinitions = new Dictionary<string, string>();
            var tables = Common.TablesDictionary(dt);
            foreach (var table in tables)
            {
                var classBuilder = new StringBuilder();
                classBuilder.AppendLine("using System;");
                classBuilder.AppendLine();
                classBuilder.AppendLine($"public partial class {ToTitleCase(table.Key)}");
                classBuilder.AppendLine("{");

                foreach (var column in table.Value)
                {
                    string csDataType = Common.ConvertOracleTypeToCSharpType(column.DataType, column.Nullable);
                    classBuilder.AppendLine($"    public {csDataType} {ToTitleCase(column.ColumnName)} {{ get; set; }}");
                }

                classBuilder.AppendLine("}");
                classDefinitions[table.Key] = classBuilder.ToString();
            }
            return classDefinitions;
        }

        public static string ToTitleCase(string str)
        {
            return CultureInfo.CurrentCulture.TextInfo.ToTitleCase(str.ToLower());
        }
        private static void SaveClassDefinitionsToFile(Dictionary<string, string> classDefinitions)
        {
            var dir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + @"\ClassDefinition\" + DateTime.Now.ToString("yyyy-MM-dd-HHmmss");
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);
            foreach (var classDefinition in classDefinitions)
            {
                string fileName = Path.Combine(dir, $"{ToTitleCase(classDefinition.Key)}.cs");

                File.WriteAllText(fileName, classDefinition.Value);
            }
        }

        private static Dictionary<string, string> GenerateMappingClassesWithKeys(DataTable dt)
        {
            var mappingClasses = new Dictionary<string, string>();

            var tables = Common.ExtractTableInfoWithKeys(dt); // Hàm gom thông tin bảng và khóa thành Dictionary

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
                foreach (var column in table.Value.Where(c => c.ForeignKeyTables.Count>0))
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

            return mappingClasses;
        }
        private static void SaveMappingClassesWithKeys(Dictionary<string, string> mappingClasses)
        {
            var dir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + @"\MappingClasses\" + DateTime.Now.ToString("yyyy-MM-dd-HHmmss");
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            foreach (var mappingClass in mappingClasses)
            {
                string filePath = Path.Combine(dir, $"{mappingClass.Key}Mapping.cs");
                File.WriteAllText(filePath, mappingClass.Value);
            }
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

            return contextBuilder.ToString();
        }

        private static void SaveEF6DbContextWithMappings(string dbContextCode)
        {
            var dir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + @"\ApplicationDbContext\" + DateTime.Now.ToString("yyyy-MM-dd-HHmmss");
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            string filePath = Path.Combine(dir, "ApplicationDbContext.cs");
            File.WriteAllText(filePath, dbContextCode);
        }


    }
}
