using System;
using System.Configuration;

namespace DataGenerator
{
    class Program
    {
        static void Main(string[] args)
        {
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

            var dg = new DataGenerator(
                dataOffsetOptions: new DataGenerator.DataOffsetOptions(
                            brandOffset: 1,
                            productOffset: 50_000,
                            personOffset: 1_000_000),
                stockDataLocation: ConfigurationManager.AppSettings["StockDataLocation"]);

            var (brands, products, people) = dg.GenerateData(
                            sampleName: sampleName,
                            brandsCount: brandsCount,
                            maxProductCount: maxProductCount,
                            peopleCount: peopleCount,
                            minProductsCount: minProductsCount,
                            maxProductsCount: maxProductsCount,
                            peoplePercentHaveCommonProducts: peoplePercentHaveCommonProducts);

            var generatedDataLocation = ConfigurationManager.AppSettings["GeneratedDataLocation"];
            DataGenerator.SaveBrands(generatedDataLocation, brands, sampleName);
            DataGenerator.SaveProducts(generatedDataLocation, products, sampleName);
            DataGenerator.SavePeople(generatedDataLocation, people, sampleName);

            Console.WriteLine("Done.");
            Console.ReadLine();
        }
    }
}
