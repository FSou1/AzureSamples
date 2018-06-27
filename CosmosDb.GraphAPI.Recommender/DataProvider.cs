using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;

namespace CosmosDb.GraphAPI.Recommender
{
    public class OffsetOptions
    {
        public OffsetOptions(int brandOffset, int productOffset, int personOffset)
        {
            this.Brand = brandOffset;
            this.Product = productOffset;
            this.Person = personOffset;
        }

        public int Brand { get; private set; }
        public int Product { get; private set; }
        public int Person { get; private set; }
    }

    public class DataGenerator
    {
        private static readonly Random _random = new Random();
        private List<(string brand, List<string> product)> _brandsProducts;
        private string[] _names;


        public DataGenerator(OffsetOptions offsetOptions)
        {
            this.OffsetOptions = offsetOptions;
            this.StockDataLocation = ConfigurationManager.AppSettings["DataGenerator.StockData"];
            this.GeneratedDataLocation = ConfigurationManager.AppSettings["DataGenerator.GeneratedData"];
        }


        public OffsetOptions OffsetOptions { get; private set; }

        public string StockDataLocation { get; set; }

        public string GeneratedDataLocation { get; set; }



        public (List<Brand> brands, List<Product> products, List<Person> people) GenerateData(
            string sampleName,
            int brandsCount,
            int maxProductCount,
            int peopleCount,
            int minProductsCount,
            int maxProductsCount,
            double peoplePercentHaveCommonProducts = 0.8,
            bool saveDataToFile = false)
        {
            if (_brandsProducts == null)
            {
                ReadStockBrandsProductsData();
            }
            if (_names == null)
            {
                ReadStockNamesData();
            }

            var (brands, products) = GenerateBrandsProductsSampleList(brandsCount, maxProductCount);
            var people = GeneratePeopleWithProducts(
                products: products,
                peopleCount: peopleCount,
                minProductsCount: minProductsCount,
                maxProductsCount: maxProductsCount,
                peoplePercentHaveCommonProducts: peoplePercentHaveCommonProducts
                );

            if (saveDataToFile)
            {
                SaveBrands(brands, sampleName);
                SaveProducts(products, sampleName);
                SavePeople(people, sampleName);
            }

            return (brands, products, people);
        }

        public void SaveBrands(List<Brand> brands, string sampleName)
        {
            var path = Path.Combine(GeneratedDataLocation, $"{sampleName}-brands.csv");
            using (var sw = new StreamWriter(path))
            {
                foreach (var brand in brands)
                {
                    sw.WriteLine($"{brand.Id};{brand.Name}");
                }
            }
        }

        public void SaveProducts(List<Product> products, string sampleName)
        {
            var path = Path.Combine(GeneratedDataLocation, $"{sampleName}-products.csv");
            using (var sw = new StreamWriter(path))
            {
                foreach (var product in products)
                {
                    sw.WriteLine($"{product.Id};{product.BrandId};{product.Name}");
                }
            }
        }

        public void SavePeople(List<Person> people, string sampleName)
        {
            var path = Path.Combine(GeneratedDataLocation, $"{sampleName}-people.csv");
            using (var sw = new StreamWriter(path))
            {
                foreach (var person in people)
                {
                    sw.WriteLine($"{person.Id};{person.Name};{string.Join(",", person.ProductIds)}");
                }
            }
        }

        private void ReadStockBrandsProductsData()
        {
            _brandsProducts = new List<(string brand, List<string> products)>();

            var path = Path.Combine(StockDataLocation, "Brands-Products");
            foreach (var file in Directory.EnumerateFiles(path))
            {
                using (var sr = new StreamReader(file))
                {
                    var brand = sr.ReadLine();
                    var products = new List<string>();
                    string str;
                    while ((str = sr.ReadLine()) != null)
                    {
                        products.Add(str);
                    }

                    _brandsProducts.Add((brand, products));
                }
            }
        }

        private void ReadStockNamesData()
        {
            _names = File.ReadAllLines(Path.Combine(StockDataLocation, "Names.txt"));
        }

        private (List<Brand> brands, List<Product> products) GenerateBrandsProductsSampleList(
            int brandsCount,
            int maxProductCount)
        {
            if (brandsCount > _brandsProducts.Count)
            {
                throw new ArgumentException($"brandsCount parameter can not be greater " +
                    $"than brandProductPairList contains brands.");
            }
            if (brandsCount > maxProductCount)
            {
                throw new ArgumentException($"brandsCount parameter can not be greater " +
                    $"than maxProductCount parameter.");
            }

            _brandsProducts.Sort(Comparer<(string brand, List<string> products)>.Create((x, y)
                => y.products.Count.CompareTo(x.products.Count)));

            var brands = new List<Brand>(brandsCount);

            int brandId = OffsetOptions.Brand;
            for (int i = 0; i < brandsCount; ++i)
            {
                brands.Add(new Brand(brandId, _brandsProducts[i].brand));
                ++brandId;
            }

            var products = new List<Product>(maxProductCount);

            int productId = OffsetOptions.Product;
            int avarageModelCountPerBrand = maxProductCount / brandsCount;
            int productsNeed = 0;

            int j = brandsCount - 1;
            while (j >= 0)
            {
                if (_brandsProducts[j].product.Count < avarageModelCountPerBrand)
                {
                    productsNeed += avarageModelCountPerBrand - _brandsProducts[j].product.Count;
                }

                int i = 0;
                while (i < _brandsProducts[j].product.Count && i < avarageModelCountPerBrand)
                {
                    products.Add(new Product(
                        id: productId,
                        name: _brandsProducts[j].product[i],
                        brandId: OffsetOptions.Brand + j));
                    ++productId;
                    ++i;
                }

                while (i < _brandsProducts[j].product.Count && productsNeed > 0)
                {
                    products.Add(new Product(
                        id: productId,
                        name: _brandsProducts[j].product[i],
                        brandId: OffsetOptions.Brand + j));
                    ++productId;
                    --productsNeed;
                    ++i;
                }

                --j;
            }

            return (brands, products);
        }

        private List<Person> GeneratePeopleWithProducts(
            List<Product> products,
            int peopleCount,
            int minProductsCount,
            int maxProductsCount,
            double peoplePercentHaveCommonProducts = 0.2)
        {
            if (peopleCount < 0)
            {
                throw new ArgumentException($"Wrong peopleCount argument. It can not be less than 0.");
            }
            if (minProductsCount < 0 || maxProductsCount < 0)
            {
                throw new ArgumentException($"Wrong minCountProducts or maxCountProducts " +
                    $"argument. It can not be less than 0.");
            }
            if (minProductsCount > maxProductsCount)
            {
                throw new ArgumentException($"Wrong minCountProducts or maxCountProducts. " +
                    $"minCountProducts arugment must be less than maxCountProducts.");
            }
            if (peoplePercentHaveCommonProducts < 0 || peoplePercentHaveCommonProducts > 1)
            {
                throw new ArgumentException($"Wrong peoplePercentHaveCommonProducts argument. " +
                    $"It can not be less than 0 or be greater than 1.");
            }

            int[] GenerateProductsArray(int size)
            {
                var result = new int[size];
                for (int i = 0; i < size; ++i)
                {
                    result[i] = products[_random.Next(0, products.Count)].Id;
                }

                return result;
            }

            int totalProducts = 0;
            var people = new List<Person>(peopleCount);
            for (int i = 0; i < peopleCount; ++i)
            {
                var productsCount = _random.Next(minProductsCount, maxProductsCount);
                people.Add(new Person(
                    id: OffsetOptions.Person + i,
                    name: _names[_random.Next(0, _names.Length)],
                    productIds: GenerateProductsArray(productsCount)));

                totalProducts += productsCount;
            }

            int personCountHaveCommonProducts = (int)(peoplePercentHaveCommonProducts * people.Count);
            for (int i = 0; i < personCountHaveCommonProducts; ++i)
            {
                int person1 = _random.Next(0, people.Count);
                int person2 = _random.Next(0, people.Count);

                int j = (people[person1].ProductIds.Length + people[person2].ProductIds.Length) / 2;
                while (j > 0)
                {
                    int person1ProductIdIndex = _random.Next(0, people[person1].ProductIds.Length);
                    int person2ProductIdIndex = _random.Next(0, people[person2].ProductIds.Length);

                    int person1ProductId = people[person1].ProductIds[person1ProductIdIndex];
                    int person2ProductId = people[person2].ProductIds[person2ProductIdIndex];

                    var a1 = _random.Next(0, people[person1].ProductIds.Length);
                    var a2 = _random.Next(0, people[person2].ProductIds.Length);

                    people[person1].ProductIds[a1] = person2ProductId;
                    people[person2].ProductIds[a2] = person1ProductId;

                    --j;
                }
            }

            return people;
        }
    }

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
