using AzureSQL.Recommender.Import.Data.Entities;
using Dapper;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AzureSQL.Recommender.Import
{
    public static class SqlDBHelper
    {
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

        public static Task AddItemsAsync<T>(List<T> items, string connectionString)
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
                        return AddToDbAsync(brands, brandsQuery, connectionString);
                    case List<Product> products:
                        return AddToDbAsync(products, productsQuery, connectionString);
                    case List<Person> people:
                        return AddToDbAsync(people, peopleQuery, connectionString);
                    case List<Order> orders:
                        return AddToDbAsync(orders, ordersQuery, connectionString);
                    default:
                        throw new ArgumentException("Not supported.");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Operation aborted. Reason: " + e.Message);
                return Task.FromException(e);
            }
        }

        private static async Task AddToDbAsync<T>(IList<T> objects, string sqlQuery, string connectionString)
        {
            try
            {
                List<Task> tasks = new List<Task>();
                foreach (var item in objects.ByChunks(250))
                {
                    var task = Task.Run(async () =>
                    {
                        using (var db = new SqlConnection(connectionString))
                        {
                            await db.ExecuteAsync(sqlQuery, item);
                        }
                    });
                    tasks.Add(task);
                }

                await Task.WhenAll(tasks);
            }
            catch (Exception e)
            {
                Console.WriteLine("Operation aborted. Reason: " + e.Message);
                //return Task.FromException(e);
            }
        }

        private static async Task ExecuteQueryAsync(string connectionString, string query)
        {
            try
            {
                using (var db = new SqlConnection(connectionString))
                {
                    await db.ExecuteAsync(query);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Operation aborted. Reason: " + e.Message);
            }
        }
    }
}
