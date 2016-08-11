![FASTTTTTTTT](logo.png)

# Spark-MySQL(/AuroraDB)-Scala benchmarking

MySQL is pretty _fast_ with spark.  Its actually not is bad, MKAY!

![FASTTTTTTTT](daim.gif)

## READS
```
Time taken : 15 seconds to insert 1000000 records
```

## WRITES
```
Time Taken : 6 seconds to read 100 records
Time Taken : 6 seconds to read 1000 records
Time Taken : 5 seconds to read 100000 records
Time Taken : 10 seconds to read 1000000 records
```

## MOTIVATION - why MySQL?
**Amazon AuroraDB** is a new cost-effective MySQL-compatible database engine for Amazon RDS which is claimed to be **5X faster** than regular MySQL.  So whatever numbers shown here, you can conceivably multiply them by 5*

You can read about [AuroraDB here](https://aws.amazon.com/blogs/aws/highly-scalable-mysql-compat-rds-db-engine/)





\* common sense required

## MY ENV
```
1 Spark cluster

Mac:
Processor Name       	   Intel Core i5
Processor Speed      	   2.4 GHz
Number of Processors 	   1
Total Number of Cores	   2
L2 Cache (per Core)  	   256 KB
L3 Cache             	   3 MB
Memory               	   16 GB
```

# RUN THE BENCHMARKS YOURSELF GURLFRIEND
```
docker build -t spark-mysql:1.0.0 .
docker run -it spark-mysql:1.0.0
```

Bye :v:
