# MaxClosingPricingMapReduceApp Here we are getting maximum selling price for each and every stock symbol for last 20 years and 
I have provided small sample dataset and run same progam with 10 GB data on cluster 
with 10 mappers and it took around 35 secs to process data

We have added Partioner just to understand how partition is partiioning data and mapper is being assigned to process that particular partition

We have used Map Reduce job and HDFS storage to get above stats later I am planning to compare it with Spark and we will
See how Spark is speeding up process and how it will reduce I/O basically

Also sample data is present in inputFile directory in same project and to see what is insite in data
