namespace AzureSQL.Recommender.Data.Entities
{
    public class Order
    {
        public int Id { get; set; }
        public int PersonId { get; set; }
        public int ProductId { get; set; }
    }
}
