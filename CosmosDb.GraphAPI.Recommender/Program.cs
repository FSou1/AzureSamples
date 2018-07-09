using CosmosDb.GraphAPI.Recommender.Data;
using CosmosDb.GraphAPI.Recommender.Data.Entites;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Graphs;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CosmosDb.GraphAPI.Recommender
{
    public class Program
    {
        public static async Task<int> Main(string[] args)
        {
            var graphDb = new GraphDatabase(
                endpoint: ConfigurationManager.AppSettings["EndPointUrl"],
                authKey: ConfigurationManager.AppSettings["AuthKey"]);

            int menuchoice = -1;
            while (menuchoice != 0)
            {
                Console.WriteLine("MENU");
                Console.WriteLine("Please enter the number that you want to do:");
                Console.WriteLine("1. Generate and save data");
                Console.WriteLine("2. Create database");
                Console.WriteLine("3. Create graph");
                Console.WriteLine("4. Add products, brands, people vertexes");

                Console.WriteLine("7. Delete database");
                Console.WriteLine("8. Delete graph");
                Console.WriteLine("9. Cleanup graph");
                Console.WriteLine("0. Exit");

                int.TryParse(Console.ReadLine(), out menuchoice);

                switch (menuchoice)
                {
                    case 0:
                        break;
                    case 1:
                        #region Generate data
                        Console.Write("Sample name: ");
                        var sampleName = Console.ReadLine();

                        Console.Write("Brands count (1-109): ");
                        var brandsCount = int.Parse(Console.ReadLine());

                        Console.Write("Max product count (1-9191): ");
                        var maxProductCount = int.Parse(Console.ReadLine());

                        Console.Write("People count: ");
                        var peopleCount = int.Parse(Console.ReadLine());

                        Console.Write("Min products count: ");
                        var minProductsCount = int.Parse(Console.ReadLine());

                        Console.Write("Max products count: ");
                        var maxProductsCount = int.Parse(Console.ReadLine());

                        Console.Write("Percent of people who have common products: ");
                        var peoplePercentHaveCommonProducts = double.Parse(Console.ReadLine());

                        var dg = new DataGenerator(new DataGenerator.DataOffsetOptions(
                            brandOffset: 1,
                            productOffset: 50_000,
                            personOffset: 1_000_000));

                        var res = dg.GenerateData(
                            sampleName: sampleName,
                            brandsCount: brandsCount,
                            maxProductCount: maxProductCount,
                            peopleCount: peopleCount,
                            minProductsCount: minProductsCount,
                            maxProductsCount: maxProductsCount,
                            peoplePercentHaveCommonProducts: peoplePercentHaveCommonProducts,
                            saveDataToFile: true);

                        Console.WriteLine("Generated and saved.");
                        #endregion
                        break;
                    case 2:
                        await graphDb.CreateDatabase(
                            databaseName: ConfigurationManager.AppSettings["DatabaseName"]);
                        break;
                    case 3:
                        await graphDb.CreateCollection(
                            database: graphDb.GetDatabase(ConfigurationManager.AppSettings["DatabaseName"]),
                            name: ConfigurationManager.AppSettings["CollectionName"],
                            partitionKey: ConfigurationManager.AppSettings["CollectionPartitionKey"],
                            collectionThroughput: int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]));
                        break;
                    case 4:
                        var collection = graphDb.GetCollection(
                            database: graphDb.GetDatabase(ConfigurationManager.AppSettings["DatabaseName"]),
                            collectionName: ConfigurationManager.AppSettings["CollectionName"]);


                        //Console.Write("Sample name: ");
                        //sampleName = Console.ReadLine();

                        sampleName = "S10000";
                        var brands = DataProvider.ReadBrands(sampleName);

                        await graphDb.AddData(
                            databaseName: ConfigurationManager.AppSettings["DatabaseName"],
                            dataCollection: collection,
                            collectionThroughput: int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]),
                            brands,
                            (x, key) => new
                            {
                                id = x.Id.ToString(),
                                label = "brand",
                                type = "vertex",
                                name = new[] {
                                    new
                                    {
                                        id = Guid.NewGuid().ToString(),
                                        _value = x.Name
                                    }
                                },
                                partitionKeyProperty = key
                            });

                        var products = DataProvider.ReadProducts(sampleName);

                        await graphDb.AddData(
                            databaseName: ConfigurationManager.AppSettings["DatabaseName"],
                            dataCollection: collection,
                            collectionThroughput: int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]),
                            products,
                            (x, key) => new
                            {
                                id = x.Id.ToString(),
                                label = "product",
                                type = "vertex",
                                name = new[] {
                                    new
                                    {
                                        id = Guid.NewGuid().ToString(),
                                        _value = x.Name
                                    }
                                },
                                partitionKeyProperty = key
                            });

                        // EDGES BRAND-PRODUCT

                        //await graphDb.AddData(
                        //    databaseName: ConfigurationManager.AppSettings["DatabaseName"],
                        //    dataCollection: collection,
                        //    collectionThroughput: int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]),
                        //    products,
                        //    (x, key) => new
                        //    {
                        //        _isEdge = true,
                        //        id = Guid.NewGuid().ToString(),
                        //        label = "made_by",

                        //        // FromVertex
                        //        _vertexId = x.Id,
                        //        _vertexLabel = "product",

                        //        // ToVertex
                        //        _sink = x.BrandId,
                        //        _sinkLabel = "brand",

                        //        partitionKeyProperty = key
                        //    });

                        var people = DataProvider.ReadPeople(sampleName);


                        return 0;

                        var cl = GetClient();
                        var co = await GetCollection(cl, "graphdbnonpart", "nonpart");

                        var brand = $"g.addV('brand').property('id', '{1}').property('name', '{"NOKIA"}')";
                        var product1 = $"g.addV('product').property('id', '{10}').property('name', '{"N95"}').addE('made_by').to(g.V('{1}'))";
                        //var product2 = $"g.addV('product').property('id', '{11}').property('name', '{"N96"}').addE('made_by').to(g.V('{1}'))";
                        var product2 = $"g.addV('product').property('id', '{11}').property('name', '{"N96"}')";

                        //var person = $"g.addV('person').property('id', '{100}').property('name', '{"Ruslan"}')";

                        //var personP1 = $"g.V('{100}').addE('bought').to(g.V('{10}'))";
                        //var personP2 = $"g.V('{100}').addE('bought').to(g.V('{11}'))";


                        ExecuteQuery(cl, co, brand).Wait();
                        ExecuteQuery(cl, co, product1).Wait();
                        ExecuteQuery(cl, co, product2).Wait();


                        //ExecuteQuery(cl, co, person).Wait();
                        //ExecuteQuery(cl, co, personP1).Wait();
                        //ExecuteQuery(cl, co, personP2).Wait();


                        var obj1 = new
                        {
                            _isEdge = true,
                            id = Guid.NewGuid().ToString(),
                            label = "made_by",

                            _vertexId = "11",                  // fromV
                            _vertexLabel = "product",

                            _sink = "1",                       // toV
                            _sinkLabel = "brand",
                            _sinkPartition = "stereotype"
                        };

                        ResourceResponse<Document> response = await cl.CreateDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri("graphdbnonpart", "nonpart"),
                            obj1,
                            new RequestOptions() { });










                        //var sw = new Stopwatch();
                        //sw.Start();
                        //AddBrands(cl, co, brands);
                        //AddProducts(graphDb._client, collection, products);
                        ////AddPersons(client, collection, people);
                        ////AddPersonProducts(client, collection, people);
                        //Console.WriteLine(sw.Elapsed);
                        break;
                    case 7:
                        await graphDb.DeleteDatabase(
                            database: graphDb.GetDatabase(ConfigurationManager.AppSettings["DatabaseName"]));
                        break;
                    case 8:
                        await graphDb.DeleteCollection(
                            database: graphDb.GetDatabase(ConfigurationManager.AppSettings["DatabaseName"]),
                            collectionName: ConfigurationManager.AppSettings["CollectionName"]);
                        break;
                    case 9:
                        //await CleanupAsync(client, collection);
                        break;
                    default:
                        Console.WriteLine("Sorry, invalid selection");
                        break;
                }

                Console.WriteLine("Completed.");
            }

            return 1;
        }




        private static void RunQueryByChunks<T>(DocumentClient client, DocumentCollection graph, List<T> list, Func<T, string> func, int chunkSize = 50)
        {
            var chunksCount = list.Count / chunkSize;
            if (list.Count % chunkSize != 0)
            {
                ++chunksCount;
            }

            for (int i = 0; i < chunksCount; ++i)
            {
                var tasksSize = chunkSize;
                if (tasksSize > list.Count || chunkSize * i > list.Count)
                {
                    tasksSize = list.Count % chunkSize;
                }
                var tasks = new Task[tasksSize];
                var offset = i * chunkSize;
                for (int j = 0; j < tasks.Length; ++j)
                {
                    var queryString = func(list[offset + j]);
                    tasks[j] = ExecuteQuery(client, graph, queryString);
                }
                Task.WaitAll(tasks);
            }
        }

        private static void RunQueryByChunks(DocumentClient client, DocumentCollection graph, List<string> queriesList, int chunkSize = 50)
        {
            var chunksCount = queriesList.Count / chunkSize;
            if (queriesList.Count % chunkSize != 0)
            {
                ++chunksCount;
            }

            for (int i = 0; i < chunksCount; ++i)
            {
                var tasksSize = chunkSize;
                if (tasksSize > queriesList.Count || chunkSize * i > queriesList.Count)
                {
                    tasksSize = queriesList.Count % chunkSize;
                }
                var tasks = new Task[tasksSize];
                var offset = i * chunkSize;
                for (int j = 0; j < tasks.Length; ++j)
                {
                    tasks[j] = ExecuteQuery(client, graph, queriesList[offset + j]);
                }
                Task.WaitAll(tasks);
            }
        }


        private static void AddBrands(
            DocumentClient client,
            DocumentCollection graph,
            List<Brand> brands)
        {
            RunQueryByChunks(client, graph, brands, x => $"g.addV('brand').property('id', '{x.Id}').property('name', '{x.Name}')");
        }

        private static void AddProducts(
            DocumentClient client,
            DocumentCollection graph,
            List<Product> products)
        {
            RunQueryByChunks(client, graph, products, x => $"g.addV('product').property('id', '{x.Id}').property('name', '{x.Name}').addE('made_by').to(g.V('{x.BrandId}'))", 10);
        }

        private static void AddPersons(
            DocumentClient client,
            DocumentCollection graph,
            List<Person> people)
        {
            RunQueryByChunks(client, graph, people, x => $"g.addV('person').property('id', '{x.Id}').property('name', '{x.Name}')", 1150);
        }

        private static void AddPersonProducts(
            DocumentClient client,
            DocumentCollection graph,
            List<Person> people)
        {
            const string template = "g.V('{0}').addE('bought').to(g.V('{1}'))";

            foreach (var person in people)
            {
                var list = new List<string>();

                foreach (var productId in person.ProductIds)
                {
                    list.Add(string.Format(template, person.Id, productId));
                }

                RunQueryByChunks(client, graph, list, 50);
            }
        }

        private static async Task CleanupAsync(DocumentClient client, DocumentCollection graph)
        {
            const string query = "g.V().drop()";

            await ExecuteQuery(client, graph, query);
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

        private static DocumentClient GetClient()
        {
            string endpoint = ConfigurationManager.AppSettings["EndPointUrl"];
            string authKey = ConfigurationManager.AppSettings["AuthKey"];

            return new DocumentClient(new Uri(endpoint), authKey, new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            });
        }

        private static async Task<DocumentCollection> GetCollection(DocumentClient client, string databaseName, string graphName)
        {
            return await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(databaseName, graphName));
        }







    }
}
