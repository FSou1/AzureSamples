using AzureSQL.Recommender.Import.Data;
using AzureSQL.Recommender.Import.Data.Entities;
using Dapper;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
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
                            await SqlDBHelper.AddBrandsAsync(brands, connectionString);
                            Console.WriteLine(" Added " + sw.Elapsed);

                            sw.Restart();
                            Console.WriteLine("Product");
                            var products = DataProvider.ReadProducts(sampleName);
                            Console.WriteLine(" Read " + sw.Elapsed);
                            sw.Restart();
                            await SqlDBHelper.AddProductsAsync(products, connectionString);
                            Console.WriteLine(" Added " + sw.Elapsed);

                            sw.Restart();
                            Console.WriteLine("People and orders");
                            var (people, orders) = DataProvider.ReadPeopleAndOrders(sampleName);
                            Console.WriteLine(" Read " + sw.Elapsed);

                            sw.Restart();
                            await SqlDBHelper.AddPeopleAsync(people, connectionString);
                            Console.WriteLine(" Added (People) " + sw.Elapsed);

                            sw.Restart();
                            await SqlDBHelper.AddOrdersAsync(orders, connectionString);
                            Console.WriteLine(" Added (Orders) " + sw.Elapsed);


                            //sw.Restart();
                            //await SqlDBHelper.EnableClusterIndexAsync(connectionString);
                            //Console.WriteLine("Clusters index has been enabled. " + sw.Elapsed);
                        }
                        break;
                    case 4:
                        using (IDbConnection db = new SqlConnection(connectionString))
                        {
                            // Adding brands

                            var brands = new Brand[]
                            {
                                new Brand()
                                {
                                    Name = "Samsung"
                                },
                                new Brand()
                                {
                                    Name = "Meizu"
                                },
                                new Brand()
                                {
                                    Name = "Xiaomi"
                                }
                            };

                            var sqlQuery = "INSERT INTO [dbo].[Brands] (Name) VALUES(@Name); SELECT CAST(SCOPE_IDENTITY() as int)";
                            foreach (var brand in brands)
                            {
                                brand.Id = db.Query<int>(sqlQuery, brand).FirstOrDefault();
                            }

                            // Adding products

                            var products = new Product[]
                            {
                                new Product()
                                {
                                    Name = "Galaxy S4",
                                    BrandId = brands[0].Id
                                },
                                new Product()
                                {
                                    Name = "M5 Note",
                                    BrandId = brands[1].Id
                                },
                                new Product()
                                {
                                    Name = "X8",
                                    BrandId = brands[2].Id
                                },
                                new Product()
                                {
                                    Name = "Galaxy S8",
                                    BrandId = brands[0].Id
                                },
                                new Product()
                                {
                                    Name = "Galaxy S9",
                                    BrandId = brands[0].Id
                                }
                            };

                            sqlQuery = "INSERT INTO [dbo].[Products] (Name, BrandId) VALUES(@Name, @BrandId); SELECT CAST(SCOPE_IDENTITY() as int)";
                            foreach (var product in products)
                            {
                                product.Id = db.Query<int>(sqlQuery, product).FirstOrDefault();
                            }

                            // Deleting product (M5)

                            sqlQuery = "DELETE FROM Products WHERE Id = @id";
                            db.Execute(sqlQuery, new { id = 2 });

                            // Deleting brand (Xiaomi) Cascade delete.
                            sqlQuery = "DELETE FROM Brands WHERE Id = @id";
                            db.Execute(sqlQuery, new { id = 3 });

                            // Adding people

                            var people = new Person[]
                            {
                                new Person()
                                {
                                    Name = "Ruslan"
                                },
                                new Person()
                                {
                                    Name = "Vlad"
                                }
                            };

                            sqlQuery = "INSERT INTO [dbo].[People] (Name) VALUES(@Name); SELECT CAST(SCOPE_IDENTITY() as int)";
                            foreach (var person in people)
                            {
                                person.Id = db.Query<int>(sqlQuery, person).FirstOrDefault();
                            }

                            // Adding orders

                            var orders = new Order[]
                            {
                                new Order()
                                {
                                    PersonId = people[0].Id,
                                    ProductId = products[0].Id
                                },
                                new Order()
                                {
                                    PersonId = people[0].Id,
                                    ProductId = products[3].Id
                                },
                                 new Order()
                                {
                                    PersonId = people[1].Id,
                                    ProductId = products[4].Id
                                }
                            };

                            sqlQuery = "INSERT INTO [dbo].[Orders] (PersonId, ProductId) VALUES(@PersonId, @ProductId); SELECT CAST(SCOPE_IDENTITY() as int)";
                            foreach (var order in orders)
                            {
                                order.Id = db.Query<int>(sqlQuery, order).FirstOrDefault();
                            }

                            // Deleting person (cascade deleting) (will delete all his orders)
                            sqlQuery = "DELETE FROM People WHERE Id = @id";
                            db.Execute(sqlQuery, new { id = 2 });

                            // Deleting product
                            sqlQuery = "DELETE FROM Products WHERE Id = @id";
                            db.Execute(sqlQuery, new { id = 1 });

                            // Deleting brand
                            sqlQuery = "DELETE FROM Brands WHERE Id = @id";
                            db.Execute(sqlQuery, new { id = 1 });
                        }

                        using (IDbConnection db = new SqlConnection(connectionString))
                        {
                            var brands = db.Query<Brand>("SELECT * FROM Brands").ToList();

                            foreach (var item in brands)
                            {
                                Console.WriteLine(item.Name);
                            }
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
