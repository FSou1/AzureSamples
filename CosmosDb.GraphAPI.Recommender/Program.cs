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
    public class Program
    {
        private static string databaseName = ConfigurationManager.AppSettings["Database.Name"];
        private static string graphName = ConfigurationManager.AppSettings["Graph.Name"];
        private static int graphThroughput = int.Parse(ConfigurationManager.AppSettings["Graph.Throughput"]);

        public static async Task<int> Main(string[] args)
        {
            var client = GetClient();
            var collection = await GetCollection(client);

            int menuchoice = 0;
            while (menuchoice != 9)
            {
                Console.WriteLine("MENU");
                Console.WriteLine("Please enter the number that you want to do:");
                Console.WriteLine("1. Create database and graph if not exist");
                Console.WriteLine("2. Generate and save data");
                Console.WriteLine("3. Add products, brands, people vertexes");

                Console.WriteLine("8. Cleanup");
                Console.WriteLine("9. Exit");

                int.TryParse(Console.ReadLine(), out menuchoice);

                switch (menuchoice)
                {
                    case 1:
                        await CreateDatabaseAndGraphAsync(client);
                        break;
                    case 2:
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

                        var dg = new DataGenerator(new OffsetOptions(
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

                        DataGenerator.FindCommonProductsBetweenPeople(res.people);

                        Console.WriteLine("Generated and saved.");

                        break;
                    case 3:
                        Console.Write("Sample name: ");
                        sampleName = Console.ReadLine();

                        var brands = DataProvider.ReadBrands(sampleName);
                        var products = DataProvider.ReadProducts(sampleName);
                        var people = DataProvider.ReadPeople(sampleName);

                        await AddBrandsAsync(client, collection, brands);
                        await AddProductsAsync(client, collection, products);
                        await AddPersonsAsync(client, collection, people);
                        await AddPersonProductsAsync(client, collection, people);

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
            var database = await client.CreateDatabaseIfNotExistsAsync(
                new Database { Id = databaseName });

            DocumentCollection graph = await client.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(databaseName),
                new DocumentCollection { Id = graphName },
                new RequestOptions { OfferThroughput = graphThroughput });
        }

        private static async Task AddBrandsAsync(
            DocumentClient client,
            DocumentCollection graph,
            List<Brand> brands)
        {
            const string template = "g.addV('brand').property('id', '{0}').property('name', '{1}')";

            foreach (var brand in brands)
            {
                var query = string.Format(template, brand.Id, brand.Name);

                await ExecuteQuery(client, graph, query);
            }
        }

        private static async Task AddProductsAsync(
            DocumentClient client,
            DocumentCollection graph,
            List<Product> products)
        {
            const string template = "g.addV('product').property('id', '{0}').property('name', '{1}').addE('made_by').to(g.V('{2}'))";

            foreach (var product in products)
            {
                var query = string.Format(template, product.Id, product.Name, product.BrandId);

                await ExecuteQuery(client, graph, query);
            }
        }

        private static async Task AddPersonsAsync(
            DocumentClient client,
            DocumentCollection graph,
            List<Person> people)
        {
            const string template = "g.addV('person').property('id', '{0}').property('name', '{1}')";

            foreach (var person in people)
            {
                var query = string.Format(template, person.Id, person.Name);

                await ExecuteQuery(client, graph, query);
            }
        }

        private static async Task AddPersonProductsAsync(
            DocumentClient client,
            DocumentCollection graph,
            List<Person> people)
        {
            const string template = "g.V('{0}').addE('bought').to(g.V('{1}'))";

            foreach (var person in people)
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
