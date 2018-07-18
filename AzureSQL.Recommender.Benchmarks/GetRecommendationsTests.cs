using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace AzureSQL.Recommender.Benchmarks
{
    public class Product
    {
        public int ProductId { get; set; }
    }

    [TestClass]
    public class GetRecommendationsTests
    {
        private static readonly string _connectionString = ConfigurationManager.AppSettings["ConnectionString"];
        private static readonly string _queriesLocation = ConfigurationManager.AppSettings["QueriesLocation"];

        [DataTestMethod]
        [DataRow(1008551, 153)] // 1 product S10_000
        [DataRow(1002784, 300)] // 3 products S10_000
        [DataRow(1008556, 410)] // 6 products S10_000
        //[DataRow(1000018, 480)] // 1 product S100_000
        //[DataRow(1000060, 497)] // 3 products S100_000
        //[DataRow(1000078, 496)] // 6 products S100_000
        public void TestRecommendationsBasedOnCommonAndDifferentBoughtProducts(
            int personId, int recommendationsCount
        )
        {
            var query = string.Format(
                format: File.ReadAllText(Path.Combine(_queriesLocation, "RecommendationsBasedOnCommonAndDifferentBoughtProducts.sql")),
                arg0: personId);

            using (var db = new SqlConnection(_connectionString))
            {
                var actual = db.Query<Product>(query).ToList();

                Assert.AreEqual(recommendationsCount, actual.Count);
            }
        }
    }
}
