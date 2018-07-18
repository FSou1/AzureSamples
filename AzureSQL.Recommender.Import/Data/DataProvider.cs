using AzureSQL.Recommender.Import.Data.Entities;
using System.Collections.Generic;
using System.Configuration;
using System.IO;

namespace AzureSQL.Recommender.Import.Data
{
    public static class DataProvider
    {
        private static readonly string _generatedDataLocation =
            ConfigurationManager.AppSettings["DataForImport"];

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
                    brands.Add(new Brand() { Id = int.Parse(tokens[0]), Name = tokens[1] });
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
                    products.Add(new Product() { Id = int.Parse(tokens[0]), Name = tokens[2], BrandId = int.Parse(tokens[1]) });
                }
            }

            return products;
        }

        public static (List<Person> people, List<Order> orders) ReadPeopleAndOrders(string sampleName)
        {
            var people = new List<Person>();
            var orders = new List<Order>();

            int orderdId = 0;

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

                    var person = new Person()
                    {
                        Id = int.Parse(tokens[0]),
                        Name = tokens[1]
                    };
                    people.Add(person);

                    foreach (var productId in productIds)
                    {
                        orders.Add(new Order()
                        {
                            Id = orderdId,
                            PersonId = person.Id,
                            ProductId = productId
                        });
                        ++orderdId;
                    }
                }
            }

            return (people, orders);
        }
    }
}
