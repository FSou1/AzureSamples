using Dapper;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureSQL.Recommender
{
    public static class SqlDBHelper
    {
        private static readonly string _queriesLocation = ConfigurationManager.AppSettings["AssetsPath"];

        public static void CreateTables(string connectionString)
        {
            var query = File.ReadAllText(Path.Combine(_queriesLocation, "CreateTables.sql"));

            try
            {
                using (IDbConnection db = new SqlConnection(connectionString))
                {
                    db.Query(query);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Operation aborted. Reason: " + e.Message);
            }
        }

        public static void DropTables(string connectionString)
        {
            var query = File.ReadAllText(Path.Combine(_queriesLocation, "DropTables.sql"));

            try
            {
                using (IDbConnection db = new SqlConnection(connectionString))
                {
                    db.Query(query);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Operation aborted. Reason: " + e.Message);
            }
        }
    }
}
