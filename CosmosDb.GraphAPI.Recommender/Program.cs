using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Graphs;
using Newtonsoft.Json;

namespace CosmosDb.GraphAPI.Recommender
{
    class Program
    {
        private static string databaseName = ConfigurationManager.AppSettings["Database.Name"];
        private static string graphName = ConfigurationManager.AppSettings["Graph.Name"];
        private static int graphThroughput = int.Parse(ConfigurationManager.AppSettings["Graph.Throughput"]);

        static async Task<int> Main(string[] args)
        {
            var client = GetClient();
            var collection = await GetCollection(client);

            int menuchoice = 0;
            while (menuchoice != 9)
            {
                Console.WriteLine("MENU");
                Console.WriteLine("Please enter the number that you want to do:");
                Console.WriteLine("1. Create database and graph if not exist");
                Console.WriteLine("2. Add products, brands, people vertexes");
                Console.WriteLine("8. Cleanup");
                Console.WriteLine("9. Exit");

                int.TryParse(Console.ReadLine(), out menuchoice);

                switch (menuchoice)
                {
                    case 1:
                        await CreateDatabaseAndGraphAsync(client);
                        break;
                    case 2:
                        await AddBrandsAsync(client, collection);
                        await AddProductsAsync(client, collection);
                        await AddPersonsAsync(client, collection);
                        await AddPersonProductsAsync(client, collection);
                        break;
                    case 3:
                        break;
                    case 4:
                        break;
                    case 5:
                        break;
                    case 6:
                        break;
                    case 7:
                        break;
                    case 8:
                        await CleanupAsync(client, collection);
                        break;
                    case 9:
                        break;
                    default:
                        Console.WriteLine("Sorry, invalid selection");
                        break;
                }

                Console.WriteLine("Completed.");
            }

            return 1;
        }

        private static async Task CreateDatabaseAndGraphAsync(DocumentClient client)
        {
            Database database = await client.CreateDatabaseIfNotExistsAsync(new Database { Id = databaseName });
            
            DocumentCollection graph = await client.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(databaseName),
                new DocumentCollection { Id = graphName },
                new RequestOptions { OfferThroughput = graphThroughput });
        }

        private static async Task AddBrandsAsync(DocumentClient client, DocumentCollection graph)
        {
            const string template = "g.addV('brand').property('id', '{0}').property('name', '{1}')";

            foreach (var brand in DataSeed.Brands)
            {
                var query = string.Format(template, brand.Id, brand.Name);

                await ExecuteQuery(client, graph, query);
            }
        }

        private static async Task AddProductsAsync(DocumentClient client, DocumentCollection graph)
        {
            const string template = "g.addV('product').property('id', '{0}').property('name', '{1}').addE('made_by').to(g.V('{2}'))";
            
            foreach (var product in DataSeed.Products)
            {
                var query = string.Format(template, product.Id, product.Name, product.BrandId);

                await ExecuteQuery(client, graph, query);
            }
        }

        private static async Task AddPersonsAsync(DocumentClient client, DocumentCollection graph)
        {
            const string template = "g.addV('person').property('id', '{0}').property('name', '{1}')";

            foreach (var person in DataSeed.Persons)
            {
                var query = string.Format(template, person.Id, person.Name);

                await ExecuteQuery(client, graph, query);
            }
        }

        private static async Task AddPersonProductsAsync(DocumentClient client, DocumentCollection graph)
        {
            const string template = "g.V('{0}').addE('bought').to(g.V('{1}'))";

            foreach (var person in DataSeed.Persons)
            {
                foreach (var productId in person.ProductIds)
                {
                    var query = string.Format(template, person.Id, productId);

                    await ExecuteQuery(client, graph, query);
                }
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
            string endpoint = ConfigurationManager.AppSettings["Endpoint"];
            string authKey = ConfigurationManager.AppSettings["AuthKey"];

            return new DocumentClient(new Uri(endpoint), authKey, new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            });
        }

        private static async Task<DocumentCollection> GetCollection(DocumentClient client)
        {
            return await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(databaseName, graphName));
        }
    }
}
