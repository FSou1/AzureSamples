using System;
using System.Configuration;
using System.Threading.Tasks;

namespace AzureSQL.Recommender
{
    public class Program
    {
        public static async Task<int> Main(string[] args)
        {
            var connectionString = ConfigurationManager.AppSettings["ConnectionString"];

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
                        SqlDBHelper.CreateTables(connectionString);
                        break;
                    case 2:
                        SqlDBHelper.DropTables(connectionString);
                        break;
                    case 3:
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
