using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosDb.GraphAPI.Recommender
{
    class DataSeed
    {
        private const int AppleId = 100;
        private const int GoogleId = 101;
        private const int SamsungId = 102;

        public static IList<Brand> Brands = new List<Brand>()
        {
            new Brand(AppleId, "Apple"),
            new Brand(GoogleId, "Google"),
            new Brand(SamsungId, "Samsung")
        };

        public static IList<Product> Products = new List<Product>()
        {
            new Product(1, "iPad", AppleId),
            new Product(2, "iPhone", AppleId),
            new Product(3, "iMac", AppleId),
            new Product(4, "Mac Pro", AppleId),
            new Product(5, "Macbook Pro", AppleId),
            new Product(6, "Macbook Air", AppleId),
            new Product(7, "iPod Touch", AppleId),
            new Product(8, "iPod nano", AppleId),
            new Product(9, "iPod shuffle", AppleId),
            new Product(10, "iPod classic", AppleId),

            new Product(11, "Nexus One", GoogleId),
            new Product(12, "Nexus S", GoogleId),
            new Product(13, "Galaxy Nexus", GoogleId),
            new Product(14, "Nexus 7", GoogleId),
            new Product(15, "Nexus 4", GoogleId),
            new Product(16, "Nexus 5", GoogleId),
            new Product(17, "Nexus 6", GoogleId),
            new Product(18, "Nexus 9", GoogleId),
            new Product(19, "Pixel C", GoogleId),
            new Product(20, "Chromebook", GoogleId),

            new Product(21, "Samsung Galaxy J1", SamsungId),
            new Product(22, "Samsung Galaxy J2", SamsungId),
            new Product(23, "Samsung Galaxy J5", SamsungId),
            new Product(24, "Samsung Galaxy J7", SamsungId),
            new Product(25, "Samsung Galaxy Ace", SamsungId),
            new Product(26, "Samsung Galaxy Core", SamsungId),
            new Product(27, "Samsung Galaxy Grand", SamsungId),
            new Product(28, "Samsung Galaxy Grand Duos", SamsungId),
            new Product(29, "Samsung Galaxy Grand Prime Plus", SamsungId),
            new Product(30, "Samsung Galaxy Express 2", SamsungId),
        };

        public static IList<Person> Persons = new List<Person>()
        {
            new Person(1000, "John", new []{ 1, 2, 11 }),
            new Person(1001, "Steven", new []{ 1, 2, 12 }),
            new Person(1002, "Elizabeth", new []{ 1, 2, 11, 30 }),
            new Person(1003, "Mike", new []{ 1, 12, 30 }),
            new Person(1004, "Ben", new []{ 2, 11, 30 })
        };
    }

    public class Brand
    {
        public int Id { get; }
        public string Name { get; }

        public Brand(int id, string name)
        {
            this.Id = id;
            this.Name = name;
        }
    }

    public class Product
    {
        public int Id { get; }
        public string Name { get; }
        public int BrandId { get; }

        public Product(int id, string name, int brandId)
        {
            this.Id = id;
            this.Name = name;
            this.BrandId = brandId;
        }
    }

    public class Person
    {
        public int Id { get; }
        public string Name { get; }
        public int[] ProductIds { get; }

        public Person(int id, string name, int[] productIds)
        {
            this.Id = id;
            this.Name = name;
            this.ProductIds = productIds;
        }
    }
}
