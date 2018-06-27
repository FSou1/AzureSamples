using CosmosDb.GraphAPI.Recommender.Data.Entites;
using System.Collections.Generic;
using System.Configuration;
using System.IO;

namespace CosmosDb.GraphAPI.Recommender.Data
{
    public static class DataProvider
    {
        private static readonly string _generatedDataLocation =
            ConfigurationManager.AppSettings["DataGenerator.GeneratedData"];

        public static List<Brand> ReadBrands(string sampleName)
        {
            var brands = new List<Brand>();
            var path = Path.Combine(_generatedDataLocation, $"{sampleName}-brands.csv");
            using (var sr = new StreamReader(path))
            {
                string str;
                while ((str = sr.ReadLine()) != null)
                {
                    var tokens = str.Split(';');
                    brands.Add(new Brand(
                        id: int.Parse(tokens[0]),
                        name: tokens[1]));
                }
            }

            return brands;
        }

        public static List<Product> ReadProducts(string sampleName)
        {
            var products = new List<Product>();
            var path = Path.Combine(_generatedDataLocation, $"{sampleName}-products.csv");
            using (var sr = new StreamReader(path))
            {
                string str;
                while ((str = sr.ReadLine()) != null)
                {
                    var tokens = str.Split(';');
                    products.Add(new Product(
                            id: int.Parse(tokens[0]),
                            name: tokens[2],
                            brandId: int.Parse(tokens[1])));
                }
            }

            return products;
        }

        public static List<Person> ReadPeople(string sampleName)
        {
            var people = new List<Person>();
            var path = Path.Combine(_generatedDataLocation, $"{sampleName}-people.csv");
            using (var sr = new StreamReader(path))
            {
                string str;
                while ((str = sr.ReadLine()) != null)
                {
                    var tokens = str.Split(';');
                    var productsIdTokens = tokens[2].Split(',');
                    var productIds = new int[productsIdTokens.Length];
                    for (int i = 0; i < productIds.Length; ++i)
                    {
                        productIds[i] = int.Parse(productsIdTokens[i]);
                    }
                    people.Add(new Person(
                        id: int.Parse(tokens[0]),
                        name: tokens[1],
                        productIds: productIds));
                }
            }

            return people;
        }
    }
}
