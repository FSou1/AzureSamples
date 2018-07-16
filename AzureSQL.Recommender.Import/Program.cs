using AzureSQL.Recommender.Import.Data;
using System;
using System.Configuration;
using System.Diagnostics;
using System.Threading.Tasks;

namespace AzureSQL.Recommender.Import
{
    public class Program
    {
        public static async Task<int> Main(string[] args)
        {
            var connectionString = ConfigurationManager.AppSettings["ConnectionString"];
            var sw = new Stopwatch();

            int menuchoice = -1;
            while (menuchoice != 0)
            {
                Console.WriteLine("MENU");
                Console.WriteLine("Please enter the number that you want to do:");
                Console.WriteLine("1. Create tables");
                Console.WriteLine("2. Drop tables");
                Console.WriteLine("3. Add products, brands, people");
                Console.WriteLine("0. Exit");

                int.TryParse(Console.ReadLine(), out menuchoice);

                switch (menuchoice)
                {
                    case 0:
                        break;
                    case 1:
                        await SqlDBHelper.CreateTablesAsync(connectionString);
                        break;
                    case 2:
                        await SqlDBHelper.DropTablesAsync(connectionString);
                        break;
                    case 3:
                        {
                            


                            var sampleName = "S10000";

                            sw.Restart();
                            Console.WriteLine("Brands");

                            var brands = DataProvider.ReadBrands(sampleName);
                            Console.WriteLine(" Read " + sw.Elapsed);
                            sw.Restart();
                            SqlDBHelper.AddItems(brands, connectionString);
                            Console.WriteLine(" Added " + sw.Elapsed);

                            sw.Restart();
                            Console.WriteLine("Product");
                            var products = DataProvider.ReadProducts(sampleName);
                            Console.WriteLine(" Read " + sw.Elapsed);
                            sw.Restart();
                            SqlDBHelper.AddItems(products, connectionString);
                            Console.WriteLine(" Added " + sw.Elapsed);

                            sw.Restart();
                            Console.WriteLine("People and orders");
                            var (people, orders) = DataProvider.ReadPeopleAndOrders(sampleName);
                            Console.WriteLine(" Read " + sw.Elapsed);

                            sw.Restart();
                            SqlDBHelper.AddItems(people, connectionString);
                            Console.WriteLine(" Added (People) " + sw.Elapsed);

                            sw.Restart();
                            SqlDBHelper.AddItems(orders, connectionString);
                            Console.WriteLine(" Added (Orders) " + sw.Elapsed);

                            sw.Restart();
                            await SqlDBHelper.EnableClusterIndexAsync(connectionString);
                            Console.WriteLine("Clusters index has been enabled. " + sw.Elapsed);
                        }
                        break;
                    default:
                        Console.WriteLine("Sorry, invalid selection");
                        break;
                }

                Console.WriteLine("Completed.");
            }

            return 0;
        }
    }
}
