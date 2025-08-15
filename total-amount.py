from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomers")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID, amount)

lines = sc.textFile("file:///C:/Users/Kakashi/Desktop/Apache-Spark/customer-orders.csv")
rdd = lines.map(parseLine)
totalByCustomer = rdd.reduceByKey(lambda x, y : x + y)
totalByCustomerSorted = totalByCustomer.map(lambda x: (x[1], x[0])).sortByKey()
results = totalByCustomerSorted.collect();
for result in results:
    print(result)
