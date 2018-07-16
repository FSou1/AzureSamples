using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Graphs;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CosmosDb.GraphAPI.Recommender.Benchmarks
{
    [TestClass]
    public class GetRecommendationsTests
    {
        private static string databaseName = ConfigurationManager.AppSettings["DatabaseId"];
        private static string graphName = ConfigurationManager.AppSettings["CollectionId"];
        private static string endpoint = ConfigurationManager.AppSettings["DocumentServerEndPoint"];
        private static string authKey = ConfigurationManager.AppSettings["AuthKey"];

        private DocumentClient client;
        private DocumentCollection collection;

        [TestInitialize]
        public async Task Init()
        {
            this.client = GetClient();
            this.collection = await GetCollection(client);
        }

        [DataTestMethod]
        [DataRow(1008551, 153)] // 1 product
        [DataRow(1002784, 300)] // 3 products
        [DataRow(1008556, 410)] // 6 products
        public async Task TestRecommendationsBasedOnCommonAndDifferentBoughtProducts(
            int personId, int recommendationsCount
        )
        {
            string query = @"
                g.V('{0}').as('his').
	            out('bought').aggregate('self').
	            in('bought').where(neq('his')).
	            out('bought').where(without('self')).
                dedup()
            ";

            var data = await ExecuteQueryAsync<Product>(client, collection, 
                string.Format(query, personId)
            );

            Assert.AreEqual(recommendationsCount, data.Count);
        }

        [DataTestMethod]
        [DataRow(1000856, 213)] // 1 product
        [DataRow(1000870, 530)] // 3 products
        [DataRow(1000874, 693)] // 6 products
        public async Task TestRecommendationsBasedOnCommonAndDifferentBoughtProductsWithoutDedup(
            int personId, int recommendationsCount
        )
        {
            string query = @"
                g.V('{0}').as('his')
	                .out('bought').aggregate('self')
	                .in('bought').where(neq('his')).dedup()
	                .out('bought').where(without('self'))
            ";

            var data = await ExecuteQueryAsync<Product>(client, collection,
                string.Format(query, personId)
            );

            Assert.AreEqual(recommendationsCount, data.Count);
        }

        [DataTestMethod]
        [DataRow(1001048, 7)] // 1 product
        [DataRow(1001060, 8)] // 3 products
        [DataRow(1001094, 35)] // 6 products
        public async Task TestRecommendationsBasedOnCommonAndDifferentBoughtProductsGreaterThanTwo(
            int personId, int recommendationsCount
        )
        {
            string query = @"
                g.V('{0}').as('his').
	                out('bought').aggregate('self').
	                in('bought').where(neq('his')).dedup()
	                .group()
	                .by().by(out('bought').where(within('self')).count().is(gt(2)))
	                .select(keys).unfold()
                .out('bought').where(without('self'))
            ";

            var data = await ExecuteQueryAsync<Product>(client, collection,
                string.Format(query, personId)
            );

            Assert.AreEqual(recommendationsCount, data.Count);
        }

        [DataTestMethod]
        [DataRow(1001109, 7)] // 1 product
        [DataRow(1001114, 8)] // 3 products
        [DataRow(1001144, 35)] // 6 products
        public async Task TestRecommendationsBasedOnBrandLoyality(
            int personId, int recommendationsCount
        )
        {
            string query = @"
                g.V('{0}').as('his')
                    .out('bought').aggregate('self')
                    .in('made_by').groupCount().unfold()
                    .where(select(values).is(gt(2)))
                    .select(keys)
                    .out('made_by').where(without('self'))
            ";

            var data = await ExecuteQueryAsync<Product>(client, collection,
                string.Format(query, personId)
            );

            Assert.AreEqual(recommendationsCount, data.Count);
        }

        private static async Task<IList<T>> ExecuteQueryAsync<T>(DocumentClient client, DocumentCollection graph, string query)
        {
            IDocumentQuery<dynamic> gremlinQuery = client.CreateGremlinQuery<dynamic>(graph, query);
            IList<T> results= new List<T>();
            while (gremlinQuery.HasMoreResults)
            {
                foreach (var result in await gremlinQuery.ExecuteNextAsync<T>())
                {
                    results.Add(result);
                }
            }
            return results;
        }

        private static DocumentClient GetClient()
        {
            return new DocumentClient(new Uri(endpoint), authKey, new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            });
        }

        private static async Task<DocumentCollection> GetCollection(DocumentClient client)
        {
            return await client.ReadDocumentCollectionAsync(
                UriFactory.CreateDocumentCollectionUri(databaseName, graphName));
        }
    }

    public class Product
    {
        public string Id { get; set; }
    }
}
