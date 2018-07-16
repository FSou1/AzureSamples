Throughput = 1000
PartitionsCount = partition for each vertex/edge (PartitionsCount = maxInt)

| Data\Sample                      | S10000           | S100000             | S1000000             |
|:--------------------------------:|------------------|---------------------|----------------------|
| Brands [vertices count ; time]   | 5 ; 6.09 s       | 10 ; 12.18 s?       | 200 ; 4 min?         |
| Products [vertices count ; time] | 500 ; 3.03 s     | 2000 ; 12.12 s?     | 4000 ; 24.24 s?      |
| MadeBy [edges count ; time]      | 500 ; 3.05 s     | 2000 ; 12.2 s?      | 4000 ; 24.40 s?      |
| People [vertices count ; time]   | 10 000 ; 45.23 s | 100 000 ; 7.53 min? | 1 000 000 ; 75 min?  |
| Bought [edges count ; time]      | 190 686 ; 970 s  | 1 902 075 ; 2.6 h?  | 19 016 611 ; 26.8 h? |


## Throughput = 10 000, partition 1

| Entity  | Type   | S10000                 | S100000                  |
|---------|--------|------------------------|--------------------------|
| Brand   | Vertex | 3.0 s (5 items)        | 3.0 s (5 items)          |
| Product | Vertex | 3.0 s (500 items)      | 3.0 s (500 items)        |
| MadeBy  | Edge   | 3.0 s (500 items)      | 3.0 s (500 items)        |
| Person  | Vertex | 21.0 s (10 000 items)  | 4m 3 s (100 000 items)   |
| Bought  | Edge   | 1m 24 s (34 835 items) | 14m 48 s (350 037 items) |

### S10000
* Data size: 36,63 MB
* Index size: 1,83 MB

### S100000
* Data size: 350 MB
* Index size: 10,9 MB

Throughput = 10000
PartitionsCount = partition for each vertex/edge (PartitionsCount = maxInt)

| Data\Sample                      | S10000           | S100000             | S1000000             |
|:--------------------------------:|------------------|---------------------|----------------------|
| Brands [vertices count ; time]   | 5 ; 3.10 s       | 10 ; 6.10 s?        | 200 ; 122 s?         |
| Products [vertices count ; time] | 500 ; 6.07 s     | 2000 ; 24.28 s?     | 4000 ; 48.56 s?      |
| MadeBy [edges count ; time]      | 500 ; 6.14 s     | 2000 ; 24.56 s?     | 4000 ; 49.12 s?      |
| People [vertices count ; time]   | 10 000 ; 6.16 s  | 100 000 ; 1 min?    | 1 000 000 ; 10 min?  |
| Bought [edges count ; time]      | 190 686 ; 206 s  | 1 902 075 ; 34 min? | 19 016 611 ; 5.7 h?  |


