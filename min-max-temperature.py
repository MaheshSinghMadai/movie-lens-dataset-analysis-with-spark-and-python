from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperature")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])* 0.1 * (9.0/5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/resources/1800.csv")
parsedLines = lines.map(parseLine)

# for minimum temperature
minTemperature = parsedLines.filter(lambda x:"TMIN" in x[1])
stationTemp = minTemperature.map(lambda x: (x[0],x[2]))
minTemperature = stationTemp.reduceByKey(lambda x,y: min(x,y))
results = minTemperature.collect()

#for maximum temperature
maxTemperature = parsedLines.filter(lambda x:"TMAX" in x[1])
stationTemp1 = maxTemperature.map(lambda x: (x[0],x[2]))
maxTemperature = stationTemp1.reduceByKey(lambda x,y: max(x,y))
results1 = maxTemperature.collect()

print("Min Temeperature\n") 
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

print("Max Temperature\n")
for result in results1:
    print(result[0] + "\t{:.2f}F".format(result[1]))