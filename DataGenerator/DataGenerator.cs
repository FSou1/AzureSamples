using DataGenerator.Entites;
using System;
using System.Collections.Generic;
using System.IO;

namespace DataGenerator
{
    public class DataGenerator
    {
        private readonly DataOffsetOptions _offsetOptions;
        private readonly string _stockDataLocation;
        private Random _random;
        private List<(string brand, List<string> products)> _brandsProducts;
        private string[] _names;

        public DataGenerator(DataOffsetOptions dataOffsetOptions, string stockDataLocation)
        {
            this._offsetOptions = dataOffsetOptions;
            this._stockDataLocation = stockDataLocation;
            this._random = new Random();
        }

        public (List<Brand> brands, List<Product> products, List<Person> people) GenerateData(
            string sampleName,
            int brandsCount,
            int maxProductCount,
            int peopleCount,
            int minProductsCount,
            int maxProductsCount,
            double peoplePercentHaveCommonProducts = 0.8)
        {
            if (_brandsProducts == null)
            {
                ReadStockBrandsProductsData();
            }
            if (_names == null)
            {
                ReadStockNamesData();
            }
            if (minProductsCount == 0)
            {
                throw new ArgumentException("minProductsCount must be greater than 0. (Bug)");
            }

            var (brands, products) = GenerateBrandsProductsSampleList(brandsCount, maxProductCount);
            var people = GeneratePeopleWithProducts(
                products: products,
                peopleCount: peopleCount,
                minProductsCount: minProductsCount,
                maxProductsCount: maxProductsCount,
                peoplePercentHaveCommonProducts: peoplePercentHaveCommonProducts
                );

            return (brands, products, people);
        }

        public static void SaveBrands(string generatedDataLocation, List<Brand> brands, string sampleName)
        {
            var path = Path.Combine(generatedDataLocation, $"{sampleName}-brands.csv");
            using (var sw = new StreamWriter(path))
            {
                foreach (var brand in brands)
                {
                    sw.WriteLine($"{brand.Id};{brand.Name}");
                }
            }
        }

        public static void SaveProducts(string generatedDataLocation, List<Product> products, string sampleName)
        {
            var path = Path.Combine(generatedDataLocation, $"{sampleName}-products.csv");
            using (var sw = new StreamWriter(path))
            {
                foreach (var product in products)
                {
                    sw.WriteLine($"{product.Id};{product.BrandId};{product.Name}");
                }
            }
        }

        public static void SavePeople(string generatedDataLocation, List<Person> people, string sampleName)
        {
            var path = Path.Combine(generatedDataLocation, $"{sampleName}-people.csv");
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

            var path = Path.Combine(_stockDataLocation, "Brands-Products");
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
            _names = File.ReadAllLines(Path.Combine(_stockDataLocation, "Names.txt"));
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

            int brandId = _offsetOptions.Brand;
            for (int i = 0; i < brandsCount; ++i)
            {
                brands.Add(new Brand()
                {
                    Id = brandId,
                    Name = _brandsProducts[i].brand
                });
                ++brandId;
            }

            var products = new List<Product>(maxProductCount);

            int productId = _offsetOptions.Product;
            int avarageModelCountPerBrand = maxProductCount / brandsCount;
            int productsNeed = 0;

            int j = brandsCount - 1;
            while (j >= 0)
            {
                if (_brandsProducts[j].products.Count < avarageModelCountPerBrand)
                {
                    productsNeed += avarageModelCountPerBrand - _brandsProducts[j].products.Count;
                }

                int i = 0;
                while (i < _brandsProducts[j].products.Count && i < avarageModelCountPerBrand)
                {
                    products.Add(new Product()
                    {
                        Id = productId,
                        Name = _brandsProducts[j].products[i],
                        BrandId = _offsetOptions.Brand + j
                    });
                    ++productId;
                    ++i;
                }

                while (i < _brandsProducts[j].products.Count && productsNeed > 0)
                {
                    products.Add(new Product()
                    {
                        Id = productId,
                        Name = _brandsProducts[j].products[i],
                        BrandId = _offsetOptions.Brand + j
                    });
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
                people.Add(new Person() {
                    Id = _offsetOptions.Person + i,
                    Name = _names[_random.Next(0, _names.Length)],
                    ProductIds = GenerateProductsArray(productsCount)
                    });

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

        public class DataOffsetOptions
        {
            public DataOffsetOptions(int brandOffset, int productOffset, int personOffset)
            {
                this.Brand = brandOffset;
                this.Product = productOffset;
                this.Person = personOffset;
            }

            public int Brand { get; private set; }
            public int Product { get; private set; }
            public int Person { get; private set; }
        }

    }
}
