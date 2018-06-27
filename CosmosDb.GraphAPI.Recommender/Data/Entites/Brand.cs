namespace CosmosDb.GraphAPI.Recommender.Data.Entites
{
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
}
