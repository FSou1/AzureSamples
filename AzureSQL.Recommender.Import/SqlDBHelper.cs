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
        [ThreadStatic]
        private static IDbConnection _db;

        static int _connectionCount = 0;

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

        public static Task AddBrandsAsync(List<Brand> brands, string connectionString)
        {
            var sqlQuery = "INSERT INTO [dbo].[Brands] (Id, Name) VALUES(@Id, @Name)";

            try
            {
                return AddToDbAsync(brands, sqlQuery, connectionString);
            }
            catch (Exception e)
            {
                Console.WriteLine("Operation aborted. Reason: " + e.Message);
                return Task.FromException(e);
            }
        }

        public static Task AddProductsAsync(List<Product> products, string connectionString)
        {
            var sqlQuery = "INSERT INTO [dbo].[Products] (Id, Name, BrandId) VALUES(@Id, @Name, @BrandId)";

            try
            {
                return AddToDbAsync(products, sqlQuery, connectionString);
            }
            catch (Exception e)
            {
                Console.WriteLine("Operation aborted. Reason: " + e.Message);
                return Task.FromException(e);
            }
        }

        public static Task AddPeopleAsync(List<Person> people, string connectionString)
        {
            var sqlQuery = "INSERT INTO [dbo].[People] (" +
                "               Id, Name" +
                "           ) VALUES(" +
                "               @Id, @Name" +
                "           )";

            try
            {
                return AddToDbAsync(people, sqlQuery, connectionString);
            }
            catch (Exception e)
            {
                Console.WriteLine("Operation aborted. Reason: " + e.Message);
                return Task.FromException(e);
            }
        }

        public static Task AddOrdersAsync(List<Order> orders, string connectionString)
        {
            var sqlQuery = "INSERT INTO [dbo].[Orders] (Id, PersonId, ProductId) VALUES(@Id, @PersonId, @ProductId)";

            try
            {
                return AddToDbAsync(orders, sqlQuery, connectionString);
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
                foreach (var item in objects.ByChunk(250))
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
                //return Task.FromException(e);
            }
        }
    }

    public static class ListExtensions
    {
        public static IEnumerable<IList<T>> ByChunk<T>(this IList<T> list, int chunkSize)
        {
            if (chunkSize < 1)
            {
                throw new ArgumentException("Chunk size can not be less than 1.");
            }

            int chunksCount = list.Count / chunkSize;

            int lastChunksSize = list.Count % chunkSize;
            if (lastChunksSize == 0)
            {
                lastChunksSize = chunkSize;
            }
            else
            {
                ++chunksCount;
            }

            for (int chunkNumber = 0; chunkNumber < chunksCount; ++chunkNumber)
            {
                int offset = chunkNumber * chunkSize;
                int currentChunkSize = chunkSize;
                if (lastChunksSize > 0 && chunkNumber == chunksCount - 1)
                {
                    currentChunkSize = lastChunksSize;
                }

                var result = new List<T>(currentChunkSize);
                for (int i = offset; i < offset + currentChunkSize; ++i)
                {
                    result.Add(list[i]);
                }
                yield return result;
            }
        }
    }
}
