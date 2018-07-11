extern alias graphs;
using CosmosDb.GraphAPI.Recommender.Data;
using System;
using System.Configuration;
using System.Diagnostics;
using System.Threading.Tasks;

namespace CosmosDb.GraphAPI.Recommender
{
    public class Program
    {
        public static async Task<int> Main(string[] args)
        {
            var sw = new Stopwatch();

            var docClient = GraphDBHelper.CreateClient(
                documentServerEndPoint: ConfigurationManager.AppSettings["DocumentServerEndPoint"],
                authKey: ConfigurationManager.AppSettings["AuthKey"]);

            var databaseId = ConfigurationManager.AppSettings["DatabaseId"];
            var collectionId = ConfigurationManager.AppSettings["CollectionId"];
            var partitionKey = ConfigurationManager.AppSettings["PartitionKeyName"];
            var partitionsCount = int.Parse(ConfigurationManager.AppSettings["PartitionsCount"]);

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
                Console.WriteLine("0. Exit");

                int.TryParse(Console.ReadLine(), out menuchoice);

                switch (menuchoice)
                {
                    case 0:
                        break;
                    case 1:
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
                        break;
                    case 2:
                        await GraphDBHelper.CreateDatabaseAsync(
                            documentClient: docClient,
                            databaseId: databaseId);
                        break;
                    case 3:
                        await GraphDBHelper.CreateCollectionAsync(
                            documentClient: docClient,
                            database: GraphDBHelper.GetDatabase(docClient, databaseId),
                            collectionId: collectionId,
                            partitionKey: partitionKey,
                            throughput: int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]),
                            isPartitionedGraph: true);
                        break;
                    case 4:
                        var collection = await GraphDBHelper.GetCollectionAsync(
                            documentClient: docClient,
                            databaseId: databaseId,
                            collectionId: collectionId);

                        Console.Write("Sample name: ");
                        sampleName = Console.ReadLine();

                        var brands = DataProvider.ReadBrands(sampleName);

                        var graphImporter = await GraphDBHelper.CreateAndInitGraphImporterAsync(
                            documentClient: docClient,
                            databaseId: databaseId,
                            collection: collection);

                        sw.Restart();
                        await GraphDBHelper.AddVerticesAsync(
                            graphImporter: graphImporter,
                            vertices: GraphDBHelper.GenerateBrandVertices(brands, partitionKey, partitionsCount));
                        Console.WriteLine("Brands have been added. " + sw.Elapsed);

                        var products = DataProvider.ReadProducts(sampleName);
                        sw.Restart();
                        await GraphDBHelper.AddVerticesAsync(
                            graphImporter: graphImporter,
                            vertices: GraphDBHelper.GenerateProductVertices(products, partitionKey, partitionsCount));
                        Console.WriteLine("Products have been added. " + sw.Elapsed);

                        sw.Restart();
                        await GraphDBHelper.AddEdgesAsync(
                            graphImporter: graphImporter,
                            edges: GraphDBHelper.GenerateBrandProductsEdges(products, partitionsCount));

                        Console.WriteLine("Brand-products have been added. " + sw.Elapsed);

                        var people = DataProvider.ReadPeople(sampleName);
                        sw.Restart();
                        await GraphDBHelper.AddVerticesAsync(
                            graphImporter: graphImporter,
                            vertices: GraphDBHelper.GeneratePeopleVertices(people, partitionKey, partitionsCount));
                        Console.WriteLine("People have been added. " + sw.Elapsed);

                        sw.Restart();
                        await GraphDBHelper.AddEdgesAsync(
                            graphImporter: graphImporter,
                            edges: GraphDBHelper.GeneratePersonProductsEdges(people, partitionsCount));

                        Console.WriteLine("Person-products have been added. " + sw.Elapsed);
                        break;
                    case 7:
                        await GraphDBHelper.DeleteDatabaseAsync(
                            documentClient: docClient,
                            database: GraphDBHelper.GetDatabase(docClient, databaseId));
                        break;
                    case 8:
                        await GraphDBHelper.DeleteCollectionAsync(
                            documentClient: docClient,
                            documentCollection: await GraphDBHelper.GetCollectionAsync(docClient, databaseId, collectionId));
                        break;
                    default:
                        Console.WriteLine("Sorry, invalid selection");
                        break;
                }

                Console.WriteLine("Completed.");
            }

            return 1;
        }
    }
}
