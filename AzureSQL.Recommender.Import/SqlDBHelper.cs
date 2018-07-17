using AzureSQL.Recommender.Import.Data.Entities;
using Dapper;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace AzureSQL.Recommender.Import
{
    public static class SqlDBHelper
    {
        private static readonly int _chunkSize = int.Parse(ConfigurationManager.AppSettings["Upload.ChunkSize"]);
        private static readonly int _latencyBetweenRequests = int.Parse(ConfigurationManager.AppSettings["Upload.LatencyBetweenRequests"]);
        private static readonly string _queriesLocation = ConfigurationManager.AppSettings["QueriesLocation"];

        public static Task CreateTablesAsync(string connectionString)
        {
            return ExecuteQueryAsync(
                connectionString: connectionString,
                query: File.ReadAllText(Path.Combine(_queriesLocation, "CreateTables.sql")));
        }

        public static Task DropTablesAsync(string connectionString)
        {
            return ExecuteQueryAsync(
                connectionString: connectionString,
                query: File.ReadAllText(Path.Combine(_queriesLocation, "DropTables.sql")));
        }

        public static Task EnableClusterIndexAsync(string connectionString)
        {
            return ExecuteQueryAsync(
                connectionString: connectionString,
                query: File.ReadAllText(Path.Combine(_queriesLocation, "EnableIndexPrimaryAndForeignKeys.sql")));
        }

        public static void AddItems<T>(List<T> items, string connectionString)
        {
            const string brandsQuery = "INSERT INTO [dbo].[Brands] (Id, Name) VALUES(@Id, @Name)";
            const string productsQuery = "INSERT INTO [dbo].[Products] (Id, Name, BrandId) VALUES(@Id, @Name, @BrandId)";
            const string peopleQuery = "INSERT INTO [dbo].[People] (Id, Name) VALUES(@Id, @Name)";
            const string ordersQuery = "INSERT INTO [dbo].[Orders] (Id, PersonId, ProductId) VALUES(@Id, @PersonId, @ProductId)";

            try
            {
                switch (items)
                {
                    case List<Brand> brands:
                        AddToDb(brands, brandsQuery, connectionString);
                        break;
                    case List<Product> products:
                        AddToDb(products, productsQuery, connectionString);
                        break;
                    case List<Person> people:
                        AddToDb(people, peopleQuery, connectionString);
                        break;
                    case List<Order> orders:
                        AddToDb(orders, ordersQuery, connectionString);
                        break;
                    default:
                        throw new ArgumentException("Not supported.");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Operation aborted. Reason: " + e.Message);
            }
        }

        private static void AddToDb<T>(IList<T> objects, string sqlQuery, string connectionString)
        {
            try
            {
                var sw = new Stopwatch();
                var tasks = new List<Task>(_chunkSize);
                foreach (var chunk in objects.ByChunks(_chunkSize))
                {
                    sw.Restart();
                    foreach (var item in chunk)
                    {
                        var task = Task.Run(async () =>
                        {
                            await ExecuteQueryAsync(connectionString, sqlQuery, item);
                        });
                        tasks.Add(task);
                    }
                    Task.WaitAll(tasks.ToArray());
                    tasks.Clear();

                    int waitingTime = _latencyBetweenRequests - (int)sw.ElapsedMilliseconds;
                    if (waitingTime < 0)
                    {
                        waitingTime = 0;
                    }

                    Thread.Sleep(waitingTime);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Operation aborted. Reason: " + e.Message);
            }
        }

        private static async Task ExecuteQueryAsync(string connectionString, string query, object param = null)
        {
            try
            {
                using (var db = new SqlConnection(connectionString))
                {
                    await db.ExecuteAsync(query, param);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Operation aborted. Reason: " + e.Message);
            }
        }
    }
}
