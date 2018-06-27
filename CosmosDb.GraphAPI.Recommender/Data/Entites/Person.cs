namespace CosmosDb.GraphAPI.Recommender.Data.Entites
{
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
