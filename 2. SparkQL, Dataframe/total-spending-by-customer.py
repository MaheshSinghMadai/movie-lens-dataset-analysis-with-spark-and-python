from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperature")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

def parseLine(line):
    fields = line.split(",")
    customerID = fields[0]
    amount = fields[2]
    return (int(customerID), float(amount))

lines = sc.textFile("file:///C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/resources/customer-orders.csv")
parsedLines = lines.map(parseLine)

totalsByCustomer = parsedLines.reduceByKey(lambda x,y : x + y )
totalsByCustomerSorted = totalsByCustomer.map(lambda x: (x[1],x[0])).sortByKey()

results =  totalsByCustomerSorted.collect()
for result in results:
    print(result)

