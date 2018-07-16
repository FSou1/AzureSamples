* [SQL queries (creating tables, and etc.)](https://github.com/ruslanlbondar/AzureSamples/tree/master/AzureSQL.Recommender.Import/Assets/Queries)

*  DB Schema
![DB Schema](https://github.com/ruslanlbondar/AzureSamples/raw/master/AzureSQL.Recommender.Import/Assets/db.png)

# Uploading time

App settings

Upload.ChunkSize = 60
| Upload.LatencyBetweenRequests | 800 ms   | 500 ms    | 250 or 200ms |
|:-----------------------------:|----------|-----------|--------------|
| Brands                        | 00:00:80 | 00:00:83  | 00:00:67     |
| Products                      | 00:10:00 | 00:09:65  | 00:10:49     |
| People                        | 02:13:84 | 01:27:22  | 01:02:83     |
| Orders                        | 07:52:66 | 04:58:35  | 03:31:53     |
| Request rate                  | 75 per/s | 129 per/s | 158 per/s    |


