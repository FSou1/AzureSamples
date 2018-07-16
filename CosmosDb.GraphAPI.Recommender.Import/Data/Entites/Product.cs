namespace CosmosDb.GraphAPI.Recommender.Import.Data.Entites
{
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
}
