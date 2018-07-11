# AzureSamples


Throughput = 1000
PartitionsCount = partition for each vertex/edge (PartitionsCount > 19 016 611)

| Data\Sample                      | S10000           | S100000             | S1000000             |
|:--------------------------------:|------------------|---------------------|----------------------|
| Brands [vertices count ; time]   | 5 ; 6.09 s       | 10 ; 12.18 s?       | 200 ; 4 min?         |
| Products [vertices count ; time] | 500 ; 3.03 s     | 2000 ; 12.12 s?     | 4000 ; 24.24 s?      |
| MadeBy [edges count ; time]      | 500 ; 3.05 s     | 2000 ; 12.2 s?      | 4000 ; 24.40 s?      |
| People [vertices count ; time]   | 10 000 ; 45.23 s | 100 000 ; 7.53 min? | 1 000 000 ; 75 min?  |
| Bought [edges count ; time]      | 190 686 ; 970 s  | 1 902 075 ; 2.6 h?  | 19 016 611 ; 26.8 h? |


Throughput = 10000
PartitionsCount = 1

| Data\Sample                      | S10000          | S100000             | S1000000              |
|:--------------------------------:|-----------------|---------------------|-----------------------|
| Brands [vertices count ; time]   | 5 ; 3.0 s       | 10 ; 6.00 s?        | 200 ; 120 s?          |
| Products [vertices count ; time] | 500 ; 3.0 s     | 2000 ; 12.00 s?     | 4000 ; 24.00 s?       |
| MadeBy [edges count ; time]      | 500 ; 6.0 s     | 2000 ; 24.00 s?     | 4000 ; 48.00 s?       |
| People [vertices count ; time]   | 10 000 ; 27.3 s | 100 000 ; 4.55 min? | 1 000 000 ; 45.5 min? |
| Bought [edges count ; time]      | 190 686 ; 9 min | 1 902 075 ; 90 min? | 19 016 611 ; 15 h?    |


Throughput = 10000
PartitionsCount = partition for each vertex/edge (PartitionsCount = maxInt)

| Data\Sample                      | S10000           | S100000             | S1000000             |
|:--------------------------------:|------------------|---------------------|----------------------|
| Brands [vertices count ; time]   | 5 ; 3.10 s       | 10 ; 6.10 s?        | 200 ; 122 s?         |
| Products [vertices count ; time] | 500 ; 6.07 s     | 2000 ; 24.28 s?     | 4000 ; 48.56 s?      |
| MadeBy [edges count ; time]      | 500 ; 6.14 s     | 2000 ; 24.56 s?     | 4000 ; 49.12 s?      |
| People [vertices count ; time]   | 10 000 ; 6.16 s  | 100 000 ; 1 min?    | 1 000 000 ; 10 min?  |
| Bought [edges count ; time]      | 190 686 ; 206 s  | 1 902 075 ; 34 min? | 19 016 611 ; 5.7 h?  |


