using CosmosDb.GraphAPI.Recommender.Data.Entites;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Graphs.BulkImport;
using Microsoft.Azure.Graphs.Elements;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CosmosDb.GraphAPI.Recommender
{
    public class GraphDatabase
    {
        private readonly string _endpoint;
        private readonly string _authKey;

        public readonly DocumentClient _client;

        //+
        public GraphDatabase(string documentServerEndPoint, string authKey)
        {
            _endpoint = documentServerEndPoint;
            _authKey = authKey;

            _client = new DocumentClient(
                serviceEndpoint: new Uri(_endpoint),
                authKeyOrResourceToken: _authKey,
                connectionPolicy: new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp
                });
        }

        //+
        public async Task<Database> CreateDatabaseAsync(string databaseId)
        {
            return await _client.CreateDatabaseAsync(new Database { Id = databaseId });
        }

        //+
        public Database GetDatabase(string databaseId)
        {
            return _client.CreateDatabaseQuery()
                .Where(db => db.Id == databaseId)
                .AsEnumerable()
                .FirstOrDefault();
        }

        //+
        public async Task DeleteDatabase(Database database)
        {
            if (database != null)
            {
                await _client.DeleteDatabaseAsync(database.SelfLink);
            }
        }

        //+
        public async Task<DocumentCollection> CreateCollection(Database database, string collectionId, string partitionKey, int throughput, bool isPartitionedGraph)
        {
            var collection = new DocumentCollection
            {
                Id = collectionId
            };

            if (isPartitionedGraph)
            {
                if (string.IsNullOrWhiteSpace(partitionKey))
                {
                    throw new ArgumentNullException("PartionKey can't be null for a partitioned collection");
                }

                collection.PartitionKey.Paths.Add("/" + partitionKey);
            }

            return await _client.CreateDocumentCollectionAsync(
                database.SelfLink,
                collection,
                new RequestOptions { OfferThroughput = throughput });
        }

        //++
        public async Task DeleteCollection(string databaseId, string collectionid)
        {
            var collection = await this.GetCollection(databaseId, collectionid);
            if (collection != null)
            {
                await _client.DeleteDocumentCollectionAsync(collection.SelfLink);
            }
        }

        //+
        public async Task<DocumentCollection> GetCollection(string databaseId, string collectionId)
        {
            return await _client.ReadDocumentCollectionAsync(
                    UriFactory.CreateDocumentCollectionUri(databaseId, collectionId));
        }



        public async Task AddVertices(string databaseId, DocumentCollection collection, IEnumerable<Vertex> vertices)
        {
            GraphBulkImport graphBulkImporter = new GraphBulkImport(_client, collection, useFlatProperty: false);
            await graphBulkImporter.InitializeAsync();

            GraphBulkImportResponse vResponse =
                await graphBulkImporter.BulkImportVerticesAsync(
                    vertices: vertices,
                    enableUpsert: true);
        }

        public async Task AddEdges(string databaseId, DocumentCollection collection, IEnumerable<Edge> edges)
        {
            GraphBulkImport graphBulkImporter = new GraphBulkImport(_client, collection, useFlatProperty: false);
            await graphBulkImporter.InitializeAsync();

            GraphBulkImportResponse eResponse = 
                await graphBulkImporter.BulkImportEdgesAsync(
                    edges: edges,
                    enableUpsert: true);
        }


        public static IEnumerable<Vertex> GenerateBrandVertices(List<Brand> brands, string partitionKey)
        {
            foreach (var brand in brands)
            {
                var vertex = new Vertex(brand.Id.ToString(), "brand");
                vertex.AddProperty(new VertexProperty(partitionKey, brand.Id.ToString()));
                vertex.AddProperty(new VertexProperty("name", brand.Name));

                yield return vertex;
            }
        }

        public static IEnumerable<Vertex> GenerateProductVertices(List<Product> products, string partitionKey)
        {
            foreach (var product in products)
            {
                var vertex = new Vertex(product.Id.ToString(), "product");
                vertex.AddProperty(new VertexProperty(partitionKey, product.Id.ToString()));
                vertex.AddProperty(new VertexProperty("name", product.Name));

                yield return vertex;
            }
        }

        public static IEnumerable<Edge> GenerateBrandProductEdges(List<Product> products, string partitionKey)
        {
            foreach (var product in products)
            {
                var edge = new Edge(
                    edgeId: Guid.NewGuid().ToString(),
                    edgeLabel: "made_by",

                    outVertexId: product.BrandId.ToString(),
                    inVertexId: product.Id.ToString(),

                    outVertexLabel: "brand",
                    inVertexLabel: "product",

                    outVertexPartitionKey: product.BrandId.ToString(),
                    inVertexPartitionKey: product.Id.ToString());

                yield return edge;
            }
        }

        public static IEnumerable<Vertex> GeneratePeopleVertices(List<Person> people, string partitionKey)
        {
            foreach (var person in people)
            {
                var vertex = new Vertex(person.Id.ToString(), "person");
                vertex.AddProperty(new VertexProperty(partitionKey, person.Id.ToString()));
                vertex.AddProperty(new VertexProperty("name", person.Name));

                yield return vertex;
            }
        }

        public static IEnumerable<Edge> GeneratePersonProductEdges(List<Person> people, string partitionKey)
        {
            foreach (var person in people)
            {
                foreach (var product in person.ProductIds)
                {
                    var edge = new Edge(
                        edgeId: Guid.NewGuid().ToString(),
                        edgeLabel: "bought",

                        outVertexId: person.Id.ToString(),
                        inVertexId: product.ToString(),

                        outVertexLabel: "person",
                        inVertexLabel: "product",

                        outVertexPartitionKey: person.Id.ToString(),
                        inVertexPartitionKey: product.ToString());

                    yield return edge;
                }
            }
        }
    }
}
