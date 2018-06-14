using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace CosmosDb.Table
{
    class Program
    {
        public static string connString = "<Your connection string>";
        
        private const int InsertCount = 50;
        private const int InsertBatchCount = 500;
        private const int ReadCount = 500;
        private const int MergeCount = 50;
        private const int MergeBatchCount = 500;
        private const int ReplaceCount = 50;
        private const int DeleteBatchCount = 500;

        static async Task<int> Main(string[] args)
        {
            int menuchoice = 0;
            while (menuchoice != 9)
            {
                Console.WriteLine("MENU");
                Console.WriteLine("Please enter the number that you want to do:");
                Console.WriteLine("1. Create a 'Customer' table if not exists");
                Console.WriteLine($"2. Insert {InsertCount} rows into a 'Customer' table");
                Console.WriteLine($"3. Insert {InsertBatchCount} rows into a 'Customer' table using batch");
                Console.WriteLine($"4. Read {ReadCount} rows sequentially from a 'Customer' table");
                Console.WriteLine($"5. Merge {MergeCount} rows from a 'Customer' table");
                Console.WriteLine($"6. Merge {MergeBatchCount} rows from a 'Customer' table using batch");
                Console.WriteLine($"7. Replace {ReplaceCount} rows of a 'Customer' table");
                Console.WriteLine("8. Delete 500 rows of a 'Customer' table");
                Console.WriteLine("9. Exit");

                int.TryParse(Console.ReadLine(), out menuchoice);

                switch (menuchoice)
                {
                    case 1:
                        Initialize();
                        Console.WriteLine("Create has been done");
                        break;
                    case 2:
                        await Insert(InsertCount);
                        Console.WriteLine($"Insert {InsertCount} rows has been done");
                        break;
                    case 3:
                        await InsertBatch(InsertBatchCount);
                        Console.WriteLine($"Insert {InsertBatchCount} rows using batch has been done");
                        break;
                    case 4:
                        await Read(ReadCount);
                        Console.WriteLine($"Read {ReadCount} rows sequentially has been done");
                        break;
                    case 5:
                        await Merge(MergeCount);
                        Console.WriteLine($"Merge {MergeCount} rows has been done");
                        break;
                    case 6:
                        await MergeBatch(MergeBatchCount);
                        Console.WriteLine($"Merge {MergeBatchCount} rows using batch has been done");
                        break;
                    case 7:
                        await Replace(ReplaceCount);
                        Console.WriteLine($"Replace {ReplaceCount} rows has been done");
                        break;
                    case 8:
                        await DeleteBatch(DeleteBatchCount);
                        Console.WriteLine($"Delete {DeleteBatchCount} rows using batch has been done");
                        break;
                    case 9:
                        break;
                    default:
                        Console.WriteLine("Sorry, invalid selection");
                        break;
                }
            }

            return 1;
        }

        public static void Initialize()
        {
            var table = GetTable("Customer");

            // Create the table if it doesn't exist.
            table.CreateIfNotExists();
        }

        public static void Cleanup()
        {
            var table = GetTable("Customer");

            // Remove the table
            table.DeleteIfExists();
        }

        private static async Task Insert(int quantity)
        {
            var table = GetTable("Customer");

            var entities = new CustomerEntity[quantity];
            for (var i = 0; i < quantity; i++)
            {
                var region = GetRegion(i);

                entities[i] = new CustomerEntity(region, i.ToString())
                {
                    Name = "Customer #" + i,
                    CreatedAtUtc = DateTime.UtcNow,
                    IsDeleted = false
                };
            }

            await ExecuteOperations(entities.Select(TableOperation.InsertOrMerge), 100);
        }

        private static async Task InsertBatch(int quantity)
        {
            const int maxBatchEntities = 100;

            var table = GetTable("Customer");

            var entities = new CustomerEntity[quantity];
            for (var i = 0; i < quantity; i++)
            {
                var region = GetRegion(i);

                entities[i] = new CustomerEntity(region, i.ToString())
                {
                    Name = "Customer #" + i,
                    CreatedAtUtc = DateTime.UtcNow,
                    IsDeleted = false,
                    ETag = "*"
                };
            }

            var numberOfBatches = quantity / maxBatchEntities;
            var operations = new TableBatchOperation[numberOfBatches];
            for (var i = 0; i < quantity; i++)
            {
                var hash = i / maxBatchEntities;
                if (operations[hash] == null)
                {
                    operations[hash] = new TableBatchOperation();
                }

                operations[hash].InsertOrMerge(entities[i]);
            }

            await ExecuteBatchOperations(operations);
        }

        private static async Task Read(int quantity)
        {
            var entities = new CustomerEntity[quantity];
            for (int i = 0; i < quantity; i++)
            {
                var id = i;
                var region = GetRegion(id);
                entities[i] = new CustomerEntity(region, id.ToString());
            }

            await ExecuteOperations(entities.Select(e =>
                TableOperation.Retrieve<CustomerEntity>(e.PartitionKey, e.RowKey)));
        }

        public static async Task Merge(int quantity)
        {
            var entities = new CustomerEntity[quantity];
            for (var i = 0; i < quantity; i++)
            {
                var region = GetRegion(i);

                entities[i] = new CustomerEntity(region, i.ToString())
                {
                    MergedAtUtc = DateTime.UtcNow,
                    ETag = "*"
                };
            }

            await ExecuteOperations(entities.Select(TableOperation.Merge));
        }

        public static async Task MergeBatch(int quantity)
        {
            const int maxBatchEntities = 100;

            var entities = new CustomerEntity[quantity];
            for (var i = 0; i < quantity; i++)
            {
                var region = GetRegion(i);

                entities[i] = new CustomerEntity(region, i.ToString())
                {
                    MergedAtUtc = DateTime.UtcNow,
                    ETag = "*"
                };
            }

            var numberOfBatches = quantity / maxBatchEntities;
            var operations = new TableBatchOperation[numberOfBatches];
            for (var i = 0; i < quantity; i++)
            {
                var hash = i / maxBatchEntities;
                if (operations[hash] == null)
                {
                    operations[hash] = new TableBatchOperation();
                }

                operations[hash].Merge(entities[i]);
            }

            await ExecuteBatchOperations(operations);
        }

        public static async Task Replace(int quantity)
        {
            var entities = new CustomerEntity[quantity];
            for (int i = 0; i < quantity; i++)
            {
                var id = i;
                var region = GetRegion(id);
                entities[i] = new CustomerEntity(region, id.ToString())
                {
                    ReplacedAtUtc = DateTime.UtcNow,
                    ETag = "*"
                };
            }

            await ExecuteOperations(entities.Select(TableOperation.InsertOrReplace));
        }
        
        public static async Task DeleteBatch(int quantity)
        {
            const int maxBatchEntities = 100;

            var entities = new CustomerEntity[quantity];
            for (var i = 0; i < quantity; i++)
            {
                var region = GetRegion(i);

                entities[i] = new CustomerEntity(region, i.ToString())
                {
                    MergedAtUtc = DateTime.UtcNow,
                    ETag = "*"
                };
            }

            var numberOfBatches = quantity / maxBatchEntities;
            var operations = new TableBatchOperation[numberOfBatches];
            for (var i = 0; i < quantity; i++)
            {
                var hash = i / maxBatchEntities;
                if (operations[hash] == null)
                {
                    operations[hash] = new TableBatchOperation();
                }

                operations[hash].Delete(entities[i]);
            }

            await ExecuteBatchOperations(operations);
        }

        private static async Task ExecuteOperations(IEnumerable<TableOperation> operations, int timeoutMs = 50)
        {
            var table = GetTable("Customer");

            foreach (var operation in operations)
            {
                Thread.Sleep(timeoutMs);

                await table.ExecuteAsync(operation)
                    .ContinueWith(task => Console.WriteLine("Operation status code: " + task.Result.HttpStatusCode));
            }
        }

        private static async Task ExecuteBatchOperations(IEnumerable<TableBatchOperation> operations, int timeoutMs = 1000)
        {
            var table = GetTable("Customer");

            foreach (var operation in operations)
            {
                Thread.Sleep(timeoutMs);

                await table.ExecuteBatchAsync(operation)
                    .ContinueWith(task => Console.WriteLine("Batch operation results count: " + task.Result.Count));
            }
        }

        public static string GetRegion(int i)
        {
            return "AD";
        }

        private static CloudTable GetTable(string tableName)
        {
            var storageAccount = CloudStorageAccount.Parse(connString);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Retrieve a reference to the table.
            return tableClient.GetTableReference(tableName);
        }
    }

    public class CustomerEntity : TableEntity
    {
        public CustomerEntity() { }

        public CustomerEntity(string region, string id)
        {
            this.PartitionKey = region;
            this.RowKey = id;
        }

        public string Name { get; set; }
        public DateTime CreatedAtUtc { get; set; }
        public DateTime? MergedAtUtc { get; set; }
        public DateTime? ReplacedAtUtc { get; set; }
        public bool IsDeleted { get; set; }
    }
}
