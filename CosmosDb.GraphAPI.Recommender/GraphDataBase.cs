using CosmosDb.GraphAPI.Recommender.Data.Entites;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Graphs;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CosmosDb.GraphAPI.Recommender
{
    public class GraphDatabase
    {
        private readonly string _endpoint;
        private readonly string _authKey;

        private readonly ConnectionPolicy _connectionPolicy;
        private readonly DocumentClient _client;

        public GraphDatabase(string endpoint, string authKey)
        {
            _endpoint = endpoint;
            _authKey = authKey;

            _connectionPolicy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
                RequestTimeout = new TimeSpan(1, 0, 0),
                MaxConnectionLimit = 1000,
                RetryOptions = new RetryOptions
                {
                    MaxRetryAttemptsOnThrottledRequests = 10,
                    MaxRetryWaitTimeInSeconds = 60
                }
            };

            _client = new DocumentClient(new Uri(_endpoint), _authKey, _connectionPolicy);
        }


        public async Task<Database> CreateDatabase(string databaseName)
        {
            return await _client.CreateDatabaseAsync(new Database { Id = databaseName });
        }

        public Database GetDatabase(string databaseName)
        {
            return _client.CreateDatabaseQuery()
                .Where(d => d.Id == databaseName)
                .AsEnumerable()
                .FirstOrDefault();
        }

        public async Task DeleteDatabase(Database database)
        {
            if (database != null)
            {
                await _client.DeleteDatabaseAsync(database.SelfLink);
            }
        }

        public async Task<DocumentCollection> CreateCollection(Database database, string name, string partitionKey, int collectionThroughput)
        {
            DocumentCollection collection = new DocumentCollection();
            collection.Id = name;
            collection.PartitionKey.Paths.Add(partitionKey);

            return await _client.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(database.Id),
                    collection,
                    new RequestOptions { OfferThroughput = collectionThroughput });
        }

        public async Task DeleteCollection(Database database, string collectionName)
        {
            var collection = this.GetCollection(database, collectionName);
            if (collection != null)
            {
                await _client.DeleteDocumentCollectionAsync(collection.SelfLink);
            }
        }

        public DocumentCollection GetCollection(Database database, string collectionName)
            => GetDatabase(database.Id) == null ? null : _client
                .CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(database.Id))
                .Where(c => c.Id == collectionName)
                .AsEnumerable()
                .FirstOrDefault();
        

        private int pendingTaskCount;
        private long documentsInserted;
        private ConcurrentDictionary<int, double> requestUnitsConsumed = new ConcurrentDictionary<int, double>();

        public async Task Add<T>(string databaseName, DocumentCollection dataCollection, int collectionThroughput, List<T> list, Func<T, string> func)
        {
            //ResourceResponse<Document> response = await _client.CreateDocumentAsync(
            //                UriFactory.CreateDocumentCollectionUri(databaseName, dataCollection.Id),
            //new
            //{
            //    id = "David",
            //    label = "person",
            //    age = new[] {
            //                                    new
            //                                    {
            //                                        id = Guid.NewGuid().ToString(),
            //                                        _value = 48
            //                                    }
            //                    },
            //    department = "support character"
            //},
            //                new RequestOptions() { });


            var taskCount = Math.Max(collectionThroughput / 1000, 1);
            taskCount = Math.Min(taskCount, 250);


            pendingTaskCount = taskCount;
            var tasks = new List<Task>();
            tasks.Add(this.LogOutputStats());

            int chunkSize = list.Count / taskCount;
            for (var i = 0; i < taskCount; i++)
            {
                var a = list.Skip(i * taskCount).Take(chunkSize).ToList();

                tasks.Add(this.InsertDocument(databaseName, i, this._client, dataCollection, a, func));
            }

            await Task.WhenAll(tasks);
        }

        
        private async Task InsertDocument<T>(string databaseName, int taskId, DocumentClient client, DocumentCollection collection, List<T> list, Func<T, string> func)
        {
            requestUnitsConsumed[taskId] = 0;

            string partitionKeyProperty = collection.PartitionKey.Paths[0].Replace("/", "");
            foreach (var item in list)
            {
                var itemJson = JsonConvert.SerializeObject(item);
                var dict = JsonConvert.DeserializeObject<Dictionary<string, object>>(itemJson);

                try
                {
                    var a = new
                    {
                        id = dict["Id"].ToString(),
                        label = "brand",
                        type = "vertex",

                        name = new[] {
                            new
                            {
                                id = Guid.NewGuid().ToString(),
                                _value = dict["Name"]
                            }
                        }
                    };

                    ResourceResponse<Document> response = await client.CreateDocumentAsync(
                            UriFactory.CreateDocumentCollectionUri(databaseName, collection.Id),
                            a,
                            new RequestOptions() { });

                    string partition = response.SessionToken.Split(':')[0];
                    requestUnitsConsumed[taskId] += response.RequestCharge;

                    Interlocked.Increment(ref this.documentsInserted);
                }
                catch (Exception e)
                {
                    if (e is DocumentClientException)
                    {
                        DocumentClientException de = (DocumentClientException)e;
                        if (de.StatusCode != HttpStatusCode.Forbidden)
                        {
                            Trace.TraceError("Failed to write {0}. Exception was {1}", JsonConvert.SerializeObject(dict), e);
                        }
                        else
                        {
                            Interlocked.Increment(ref this.documentsInserted);
                        }
                    }
                    else
                    {
                        Console.WriteLine(e.Message);
                    }
                }
            }

            Interlocked.Decrement(ref this.pendingTaskCount);
        }

        private static async Task ExecuteQuery(DocumentClient client, DocumentCollection graph, string query)
        {
            IDocumentQuery<dynamic> gremlinQuery = client.CreateGremlinQuery<dynamic>(graph, query);
            while (gremlinQuery.HasMoreResults)
            {
                foreach (dynamic result in await gremlinQuery.ExecuteNextAsync())
                {
                    Console.WriteLine($"{JsonConvert.SerializeObject(result)}");
                }
            }
        }


        private async Task LogOutputStats()
        {
            long lastCount = 0;
            double lastRequestUnits = 0;
            double lastSeconds = 0;
            double requestUnits = 0;
            double ruPerSecond = 0;
            double ruPerMonth = 0;

            Stopwatch watch = new Stopwatch();
            watch.Start();

            while (this.pendingTaskCount > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                double seconds = watch.Elapsed.TotalSeconds;

                requestUnits = 0;
                foreach (int taskId in requestUnitsConsumed.Keys)
                {
                    requestUnits += requestUnitsConsumed[taskId];
                }

                long currentCount = this.documentsInserted;
                ruPerSecond = (requestUnits / seconds);
                ruPerMonth = ruPerSecond * 86400 * 30;

                Console.WriteLine("Inserted {0} docs @ {1} writes/s, {2} RU/s ({3}B max monthly 1KB reads)",
                    currentCount,
                    Math.Round(this.documentsInserted / seconds),
                    Math.Round(ruPerSecond),
                    Math.Round(ruPerMonth / (1000 * 1000 * 1000)));

                lastCount = documentsInserted;
                lastSeconds = seconds;
                lastRequestUnits = requestUnits;
            }

            double totalSeconds = watch.Elapsed.TotalSeconds;
            ruPerSecond = (requestUnits / totalSeconds);
            ruPerMonth = ruPerSecond * 86400 * 30;

            Console.WriteLine();
            Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("Inserted {0} docs @ {1} writes/s, {2} RU/s ({3}B max monthly 1KB reads)",
                lastCount,
                Math.Round(this.documentsInserted / watch.Elapsed.TotalSeconds),
                Math.Round(ruPerSecond),
                Math.Round(ruPerMonth / (1000 * 1000 * 1000)));
            Console.WriteLine("--------------------------------------------------------------------- ");
        }
    }
}
