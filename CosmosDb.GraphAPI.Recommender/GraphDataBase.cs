using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace CosmosDb.GraphAPI.Recommender
{
    public class GraphDatabase
    {
        private readonly string _endpoint;
        private readonly string _authKey;

        public readonly DocumentClient _client;

        //+
        public GraphDatabase(string documentServerEndPoint, string authKey)
        {
            _endpoint = documentServerEndPoint;
            _authKey = authKey;

            _client = new DocumentClient(
                serviceEndpoint: new Uri(_endpoint),
                authKeyOrResourceToken: _authKey,
                connectionPolicy: new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp
                });
        }


        public async Task<Database> CreateDatabaseAsync(string databaseId)
        {
            return await _client.CreateDatabaseAsync(new Database { Id = databaseId });
        }

        //+
        public Database GetDatabase(string databaseId)
        {
            return _client.CreateDatabaseQuery()
                .Where(db => db.Id == databaseId)
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

        //+
        public async Task<DocumentCollection> CreateCollection(Database database, string collectionId, string partitionKey, int throughput, bool isPartitionedGraph)
        {
            var collection = new DocumentCollection
            {
                Id = collectionId
            };

            if (isPartitionedGraph)
            {
                if (string.IsNullOrWhiteSpace(partitionKey))
                {
                    throw new ArgumentNullException("PartionKey can't be null for a partitioned collection");
                }

                collection.PartitionKey.Paths.Add("/" + partitionKey);
            }

            return await _client.CreateDocumentCollectionAsync(
                database.SelfLink,
                collection,
                new RequestOptions { OfferThroughput = throughput });
        }

        //++
        public async Task DeleteCollection(string databaseId, string collectionid)
        {
            var collection = await this.GetCollection(databaseId, collectionid);
            if (collection != null)
            {
                await _client.DeleteDocumentCollectionAsync(collection.SelfLink);
            }
        }

        //+
        public async Task<DocumentCollection> GetCollection(string databaseId, string collectionId)
        {
            return await _client.ReadDocumentCollectionAsync(
                    UriFactory.CreateDocumentCollectionUri(databaseId, collectionId));
        }

        private long _documentsInserted;
        private bool _isProcessed;
        private ConcurrentDictionary<int, double> _requestUnitsConsumed;

        public async Task AddData<T>(string databaseName, DocumentCollection dataCollection, int collectionThroughput, List<T> list, Func<T, string, object> func)
        {
            _documentsInserted = 0;
            _isProcessed = false;
            _requestUnitsConsumed = new ConcurrentDictionary<int, double>();

            var taskCount = Math.Max(collectionThroughput / 1000, 1);
            taskCount = Math.Min(taskCount, 250);

            taskCount = 100; // --------
            ThreadPool.SetMinThreads(taskCount, taskCount);


            int chunkSize = list.Count / taskCount;
            if (chunkSize == 0)
            {
                chunkSize = list.Count;
            }

            var logStatTask = this.LogOutputStats();
            var tasks = new List<Task>();
            int i = 0;
            while (i < taskCount && i * chunkSize < list.Count)
            {
                var chunk = list.Skip(i * chunkSize).Take(chunkSize).ToList();

                tasks.Add(this.InsertDocument(i, databaseName, this._client, dataCollection, func, chunk));
                ++i;
            }

            await Task.WhenAll(tasks);
            _isProcessed = true;
            await logStatTask;
        }


        private async Task InsertDocument<T>(int taskId, string dbName, DocumentClient client, DocumentCollection collection, Func<T, string, object> generateDocument, List<T> dataList)
        {
            _requestUnitsConsumed[taskId] = 0;

            string partitionKeyProperty = collection.PartitionKey.Paths[0].Replace("/", "");
            //Console.WriteLine(partitionKeyProperty);
            foreach (var data in dataList)
            {
                Object document = generateDocument(data, partitionKeyProperty);
                try
                {
                    var response = await client.CreateDocumentAsync(
                            documentCollectionUri: UriFactory.CreateDocumentCollectionUri(dbName, collection.Id),
                            document: document
                            );

                    string partition = response.SessionToken.Split(':')[0];
                    _requestUnitsConsumed[taskId] += response.RequestCharge;

                    Interlocked.Increment(ref this._documentsInserted);
                }
                catch (Exception e)
                {
                    if (e is DocumentClientException dce)
                    {
                        if (dce.StatusCode != HttpStatusCode.Forbidden)
                        {
                            Trace.TraceError("Failed to write {0}. Exception was {1}", JsonConvert.SerializeObject(document), e);
                        }
                        else
                        {
                            Interlocked.Increment(ref this._documentsInserted);
                        }
                    }
                    else
                    {
                        Console.WriteLine(e.Message);
                    }
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

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            while (!_isProcessed)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                double seconds = stopwatch.Elapsed.TotalSeconds;

                requestUnits = 0;
                foreach (int taskId in _requestUnitsConsumed.Keys)
                {
                    requestUnits += _requestUnitsConsumed[taskId];
                }

                long currentCount = this._documentsInserted;
                ruPerSecond = (requestUnits / seconds);
                ruPerMonth = ruPerSecond * 86400 * 30;

                Console.WriteLine("Inserted {0} docs @ {1} writes/s, {2} RU/s ({3}B max monthly 1KB reads)",
                    currentCount,
                    Math.Round(this._documentsInserted / seconds),
                    Math.Round(ruPerSecond),
                    Math.Round(ruPerMonth / (1000 * 1000 * 1000)));

                lastCount = _documentsInserted;
                lastSeconds = seconds;
                lastRequestUnits = requestUnits;
            }

            double totalSeconds = stopwatch.Elapsed.TotalSeconds;
            ruPerSecond = (requestUnits / totalSeconds);
            ruPerMonth = ruPerSecond * 86400 * 30;

            Console.WriteLine();
            Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("Inserted {0} docs @ {1} writes/s, {2} RU/s ({3}B max monthly 1KB reads)",
                lastCount,
                Math.Round(this._documentsInserted / stopwatch.Elapsed.TotalSeconds),
                Math.Round(ruPerSecond),
                Math.Round(ruPerMonth / (1000 * 1000 * 1000)));
            Console.WriteLine("--------------------------------------------------------------------- ");
        }
    }
}
