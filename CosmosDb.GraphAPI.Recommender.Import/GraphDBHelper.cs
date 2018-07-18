using CosmosDb.GraphAPI.Recommender.Import.Data.Entites;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Graphs.BulkImport;
using Microsoft.Azure.Graphs.Elements;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CosmosDb.GraphAPI.Recommender.Import
{
    public static class GraphDBHelper
    {
        public static DocumentClient CreateClient(string documentServerEndPoint, string authKey)
        {
            return new DocumentClient(
                serviceEndpoint: new Uri(documentServerEndPoint),
                authKeyOrResourceToken: authKey,
                connectionPolicy: new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp
                });
        }

        // Database

        public static async Task<Database> CreateDatabaseAsync(DocumentClient documentClient, string databaseId)
        {
            return await documentClient.CreateDatabaseAsync(new Database { Id = databaseId });
        }

        public static Database GetDatabase(DocumentClient documentClient, string databaseId)
        {
            return documentClient.CreateDatabaseQuery()
                .Where(db => db.Id == databaseId)
                .AsEnumerable()
                .FirstOrDefault();
        }

        public static async Task DeleteDatabaseAsync(DocumentClient documentClient, Database database)
        {
            if (database != null)
            {
                await documentClient.DeleteDatabaseAsync(database.SelfLink);
            }
        }

        // Collection

        public static async Task<DocumentCollection> CreateCollectionAsync(DocumentClient documentClient, 
            Database database, string collectionId, string partitionKey, int throughput, bool isPartitionedGraph)
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

            return await documentClient.CreateDocumentCollectionAsync(
                database.SelfLink,
                collection,
                new RequestOptions { OfferThroughput = throughput });
        }

        public static async Task DeleteCollectionAsync(DocumentClient documentClient, DocumentCollection documentCollection)
        {
            if (documentCollection != null)
            {
                await documentClient.DeleteDocumentCollectionAsync(documentCollection.SelfLink);
            }
        }


        public static async Task<DocumentCollection> GetCollectionAsync(DocumentClient documentClient, 
            string databaseId, string collectionId)
        {
            return await documentClient.ReadDocumentCollectionAsync(
                UriFactory.CreateDocumentCollectionUri(databaseId, collectionId));
        }

        // 
        
        public static async Task<GraphBulkImport> CreateAndInitGraphImporterAsync(DocumentClient documentClient,
            string databaseId, DocumentCollection collection)
        {
            var graphBulkImport = new GraphBulkImport(documentClient, collection, useFlatProperty: false);
            await graphBulkImport.InitializeAsync();
            return graphBulkImport;
        }

        public static Task<GraphBulkImportResponse> AddVerticesAsync(GraphBulkImport graphImporter, 
            IEnumerable<Vertex> vertices)
        {
            return graphImporter.BulkImportVerticesAsync(
                vertices: vertices,
                enableUpsert: true);
        }

        public static Task<GraphBulkImportResponse> AddEdgesAsync(GraphBulkImport graphImporter, 
            IEnumerable<Edge> edges)
        {
            return graphImporter.BulkImportEdgesAsync(
                edges: edges,
                enableUpsert: true);
        }


        // Genearting

        public static IEnumerable<Vertex> GenerateBrandVertices(List<Brand> brands, string partitionKey, int partitionsCount)
        {
            foreach (var brand in brands)
            {
                var vertex = new Vertex(brand.Id.ToString(), "brand");
                vertex.AddProperty(new VertexProperty(partitionKey, brand.Id % partitionsCount));
                vertex.AddProperty(new VertexProperty("name", brand.Name));

                yield return vertex;
            }
        }

        public static IEnumerable<Vertex> GenerateProductVertices(List<Product> products, string partitionKey, int partitionsCount)
        {
            foreach (var product in products)
            {
                var vertex = new Vertex(product.Id.ToString(), "product");
                vertex.AddProperty(new VertexProperty(partitionKey, product.Id % partitionsCount));
                vertex.AddProperty(new VertexProperty("name", product.Name));

                yield return vertex;
            }
        }

        public static IEnumerable<Vertex> GeneratePeopleVertices(List<Person> people, string partitionKey, int partitionsCount)
        {
            foreach (var person in people)
            {
                var vertex = new Vertex(person.Id.ToString(), "person");
                vertex.AddProperty(new VertexProperty(partitionKey, person.Id % partitionsCount));
                vertex.AddProperty(new VertexProperty("name", person.Name));

                yield return vertex;
            }
        }

        public static IEnumerable<Edge> GenerateBrandProductsEdges(List<Product> products, int partitionsCount)
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

                    outVertexPartitionKey: product.BrandId % partitionsCount,
                    inVertexPartitionKey: product.Id % partitionsCount);

                yield return edge;
            }
        }

        public static IEnumerable<Edge> GeneratePersonProductsEdges(List<Person> people, int partitionsCount)
        {
            foreach (var person in people)
            {
                foreach (var productId in person.ProductIds)
                {
                    var edge = new Edge(
                        edgeId: Guid.NewGuid().ToString(),
                        edgeLabel: "bought",

                        outVertexId: person.Id.ToString(),
                        inVertexId: productId.ToString(),

                        outVertexLabel: "person",
                        inVertexLabel: "product",

                        outVertexPartitionKey: person.Id % partitionsCount,
                        inVertexPartitionKey: productId % partitionsCount);

                    yield return edge;
                }
            }
        }
    }
}
