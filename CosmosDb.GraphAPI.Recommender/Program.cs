namespace CosmosDb.GraphAPI.Recommender
{
    extern alias graphs;

    using CosmosDb.GraphAPI.Recommender.Data;
    using System;
    using System.Configuration;
    using System.Diagnostics;
    using System.Threading.Tasks;

    public class Program
    {
        public static async Task<int> Main(string[] args)
        {
            var graphDb = new GraphDatabase(
                documentServerEndPoint: ConfigurationManager.AppSettings["DocumentServerEndPoint"],
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
                        await graphDb.CreateDatabaseAsync(
                            databaseId: ConfigurationManager.AppSettings["DatabaseId"]);
                        break;
                    case 3:
                        await graphDb.CreateCollection(
                            database: graphDb.GetDatabase(ConfigurationManager.AppSettings["DatabaseId"]),
                            collectionId: ConfigurationManager.AppSettings["CollectionId"],
                            partitionKey: ConfigurationManager.AppSettings["PartitionKeyName"],
                            throughput: int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]),
                            isPartitionedGraph: true);
                        break;
                    case 4:
                        var collection = await graphDb.GetCollection(
                            databaseId: ConfigurationManager.AppSettings["DatabaseId"],
                            collectionId: ConfigurationManager.AppSettings["CollectionId"]);

                        //Console.Write("Sample name: ");
                        sampleName = "S10000"; //Console.ReadLine();

                        var sw = new Stopwatch();

                        var brands = DataProvider.ReadBrands(sampleName);

                        sw.Restart();
                        await graphDb.AddVertices(
                            databaseId: ConfigurationManager.AppSettings["DatabaseId"],
                            collection: collection,
                            vertices: GraphDatabase.GenerateBrandVertices(brands, ConfigurationManager.AppSettings["PartitionKeyName"]));
                        Console.WriteLine("brands added"); Console.WriteLine(sw.Elapsed);

                        var products = DataProvider.ReadProducts(sampleName);
                        sw.Restart();
                        await graphDb.AddVertices(
                            databaseId: ConfigurationManager.AppSettings["DatabaseId"],
                            collection: collection,
                            vertices: GraphDatabase.GenerateProductVertices(products, ConfigurationManager.AppSettings["PartitionKeyName"]));
                        Console.WriteLine("products added"); Console.WriteLine(sw.Elapsed);

                        sw.Restart();
                        await graphDb.AddEdges(
                            databaseId: ConfigurationManager.AppSettings["DatabaseId"],
                            collection: collection,
                            edges: GraphDatabase.GenerateBrandProductEdges(products, ConfigurationManager.AppSettings["PartitionKeyName"]));

                        Console.WriteLine("brand-product added"); Console.WriteLine(sw.Elapsed);

                        var people = DataProvider.ReadPeople(sampleName);
                        sw.Restart();
                        await graphDb.AddVertices(
                            databaseId: ConfigurationManager.AppSettings["DatabaseId"],
                            collection: collection,
                            vertices: GraphDatabase.GeneratePeopleVertices(people, ConfigurationManager.AppSettings["PartitionKeyName"]));
                        Console.WriteLine("people added"); Console.WriteLine(sw.Elapsed);

                        sw.Restart();
                        await graphDb.AddEdges(
                           databaseId: ConfigurationManager.AppSettings["DatabaseId"],
                           collection: collection,
                           edges: GraphDatabase.GeneratePersonProductEdges(people, ConfigurationManager.AppSettings["PartitionKeyName"]));

                        Console.WriteLine("person-product added"); Console.WriteLine(sw.Elapsed);



                        break;
                    case 7:
                        await graphDb.DeleteDatabase(
                            database: graphDb.GetDatabase(ConfigurationManager.AppSettings["DatabaseId"]));
                        break;
                    case 8:
                        await graphDb.DeleteCollection(
                            databaseId: ConfigurationManager.AppSettings["DatabaseId"],
                            collectionid: ConfigurationManager.AppSettings["CollectionId"]);
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
    }
}
