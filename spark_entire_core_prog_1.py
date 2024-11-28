#Change line no 1

print("What we are going to learn here"
      "1. Important - How to create a spark application - sparkSession object to enter into spark application"
      "2. Not Important - How to create RDDs - multiple methods"
      "3. Not Important -Different types of transformations, characteristics of transformations and its usage"
      "4. Not Important -Characteristics of action and its usage"
      "5. Important - Spark Architecture, Core fundamentals, Performance Optimization etc.,"
      "Out of all above 5 components in spark core (component 1 and 5 are important, rest of the components 2,3,4 can be just learned once for attending interviews and for supplementing component 5")

print("1. How to create a spark application - sparkSession object to enter into spark application")
#To start write a spark application, we need sparkcontext, sqlcontext, hivecontext hence we are instantiating sparksession
#Interview question: How you are gettin spark session object created or importance of spark session or how you start write a spark application?
from pyspark.sql.session import SparkSession
#spark=SparkSession.builder.getOrCreate()#minimum needed
spark=SparkSession.builder.master("local[*]").appName("WE43 Spark Core App").enableHiveSupport().getOrCreate()
#SparkSession.builder -> class Builder -> methods master/appname/enablehivesupport/getorcreate -> SparkSession object created
#SparkSession is a class
#builder is a class variable/attribute used to call the functions like master, appname,enablehivesupport, getorcreate to build this sparksession object
#master - help us submit this spark application to the respective cluster manager
#appName - help us name this application to uniquely identify in web gui when it runs in cluster
#enablehivesupport - used to enble hive context to write complete HQL with most of the feature like catalog, udfs etc.,
#getorcreate - help us get an existing spark session already running or help us create a new spark session object.

#Lets understand how SparkSession.builder.getOrCreate() works...
print("Existing spark object",spark)

spark1=SparkSession.builder.getOrCreate()
print("New spark object (Existing)",spark1)

spark.stop()
spark2=SparkSession.builder.getOrCreate()
print("New spark object (New)",spark2)


print("2. Not Important - How to create RDDs using multiple methodologies - "
      "still we need to learn this to understand what is RDD, how memory management & partition management is happening fundamentally")
spark2.stop()
from pyspark.sql.session import SparkSession
spark=SparkSession.builder.master("local[*]").appName("WE43 Spark Core App").enableHiveSupport().getOrCreate()
sc=spark.sparkContext

#Interview Questions:
#Can we have more than one spark context in a same application? No, only one can be created
#Error org.apache.spark.SparkException: Only one SparkContext should be running in this JVM (see SPARK-2243).The currently running SparkContext was created at:


#There are 4 ways we can create RDDs -
# 1.RDDs can be created from some sources
# 2.RDDs can be created from another RDD
# 3.RDDs can be created from memory
# 4.RDDs can be created programatically
#Terminologies to remember before learning about RDDs - RDD, DAG (Direct Acyclick Graph), Lineage (direct relation between RDDs as transformation and action), transformation , action
#What is RDD? Resilient (Can be Rebuild) Distributed (across muliple nodes memory) Dataset (can come from anywhere)
#Real life simple realization - In a hotel (cluster), Tables (nodes), in plates (containers) customers (spark transformations & actions) are filling food (dataset) and sit in different tables (distributed)
#The food can be filled from different food counters (data)
#sit in different tables (distributed)
#in case if they lost the plate it can be refilled (resilient)
#customers (spark transformations & actions) - Transformations - filling food in the plate, adding curry, mixing, joining, aggregating... Actions - eating/drinking/filtering/chewing/tasting
#hotel is managed by manager (cluster manager - standalone/mesos/kb/yarn/local)

#Why we need RDD? For performing lighning speed inmemory computation in a parallel & distributed fashion in the cluster on the large scale data
#RDDs are fault tolerant via lineage inside the DAG?
#RDDs are Distributed using partitions?

print("1.RDDs can be created from some file sources - Initial RDD creation")
file_rdd1=sc.textFile("file:///home/hduser/cust.txt")#textfile takes input of a file location and returns RDD
hadoop_rdd1=sc.textFile("/user/hduser/cust.txt")
print("Total number of customers we have",file_rdd1.count())

#A spark application can be developed with minimum an rdd performed with an action (transformation is optional)

print("2.RDDs can be created from another RDD from memory (refreshed/recreated) - To transform one RDD to Another because RDDs are immutable")
function1=lambda row:"chennai" in row #Python Anonymous lambda function or we can create python named defined method
def met1(row):
      return "chennai" in row
another_rdd2=hadoop_rdd1.filter(function1) #Filter is what type of function? Higher Order Function
#hivetable1 fname,lname(non modifyable) -> insert select concat(fname,lname) where city='chennai' -> another hivvetable fullname

print("3.RDDs can be created another RDD from memory (retained) (important)- Usually when an rdd is materialized from storage layer to memory, by default"
      "it will be deleted by Garbage Collector, by applying cache on the given RDD (which requires reuse) the GC will not clean the data")
file_rdd1=sc.textFile("file:///home/hduser/cust.txt")
file_rdd1.cache()#When the first action executed on file_rdd1 or the child rdds of file_rdd1, then data will retained in the cahce rather than dleting
another_rdd2=file_rdd1.filter(function1)
another_rdd2.count()#first time when action is performed the data will come from the disk -> rdd1 (memory retained)-> rdd2 (memory cleaned) -> action
#If cached, the rdd does not show the new count if dataset is updated
another_rdd2.count()#second time onwards when action is performed the data will come from the rdd1 (memory retained) -> rdd2 -> action
file_rdd1.unpersist() #clean the memory for others to consume
#After action completion ..do we need to remove the retained memory
#Yes, If we still have some more lines of code writtened in this application where we don't have any dependency on the cached rdd file_rdd1
#No, If the line number 86 is the last line of this application, then when spark session is closed, automatically the memory objects will be removed.

file_rdd2=sc.textFile("file:///home/hduser/empdata.csv")
print(file_rdd2.count())

print("4.RDDs can be created programatically - When we wanted to convert the given data into rdd or when we do development with some sample dataset")
#1.When we want to convert the given data into RDD
customerData = [
    (1, "John", 1000, "Chennai"),
    (2, "Mary", 2000, "Bangalore"),
    (3, "Tom", 1500, "Chennai"),
    (4, "Emma", 3000, "Mumbai"),
    (5, "Peter", 2500, "Chennai")
]

# Create an RDD from the list of customer data
customerRDD = sc.parallelize(customerData)
print(customerRDD.count())

#2. For development or testing or learning we want to create RDD programatically...
lst=range(1,1001)
rdd1=sc.parallelize(lst,10)
print(rdd1.glom().collect())


print("3. How to Transform/process RDDs - Not much important")

#Interview Question: How to identify the given function is a transformation or an action?
#If a function returns another rdd then it is transformation
#If a function returns result/value then it is action
rdd1=sc.parallelize(range(1,101))
rdd2=rdd1.map(lambda val:val+.5)#This map is a transformation function as it returns another rdd"
print("This map is a transformation function as it returns another rdd",rdd2)
print("This count is an action function as it returns result", rdd2.sum())
rdd3=rdd2.filter(lambda x:x>3)
#Driver program
#DAG
#rdd2 is the child of rdd1 by applying map - lineage1
#rdd3 is the child of rdd2 by applying filter - lineage2

#Interview Question: Transformation are of 2 categories
#Active Transformation - If the output number of elements of a given rdd is different from the input number of elements of an rdd
print(rdd1.count())
rdd2=rdd1.filter(lambda x:x>=10)
print(rdd2.count())
print("If False then filter is a active transformation ",rdd1.count()==rdd2.count())
#Passive Transformation - If the output number of elements of a given rdd is same as like the input number of elements of an rdd
print(rdd1.count())
rdd2=rdd1.map(lambda val:val+.5)
print(rdd2.count())
print("If True then map is a passive transformation ",rdd1.count()==rdd2.count())

#Interview Question:
#Transformations Dependency Characteristics (spark CM, job, task, stage)
#Narrow Dependent Transformations
#Wide Dependent Transformations

#Let's learn about few important transformations (we don't use in spark SQL development),
# but we may rarely use in spark core application #development eg. unstructured data management, data cleanup etc.,
# or for understanding the core architecture/terminologies better or for attending the interviews this learning may be useful
#map, flatmap, filter, zip, zipwithindex, set transformations, join, reducebykey...

#map transformation: map is a higher of method, used to apply any functionality/transformation on every element of a given RDD.
#map is equivalent to a for/while loop but it is a distributed & parallel function that can run on rdd partitions concurrently...
#In SQL side: map is equivalent to select in SQL.
sal_lst=[10000,20000,15000,30000]
bon=1000
new_sal_lst=[]
for sal in sal_lst:
    new_sal_lst.append(sal+bon)#sequencially

sal_lst_rdd=sc.parallelize([10000,20000,15000,30000],2)
bon=1000
lam_func=lambda sal:sal+bon
rdd2=sal_lst_rdd.map(lam_func)#distributedly/parallely/concurrently across partitions using in memory

#I want to split the list(string) to list(list(string)) to list(list(some defined datatype)), we can use map deliberately
rdd1=sc.textFile("file:///home/hduser/empdata.csv")
print("list(string)",rdd1.collect())
rdd2=rdd1.map(lambda elem:elem.split(","))
print("list(list(string))",rdd2.collect())
rdd3=rdd2.map(lambda x:[int(x[0]),int(x[2]),x[1]])
print("list(list(some defined datatype))",rdd3.collect())

#Usecase: I dont need column 2 in the given dataset, i need to identify the right datatype for the col1 and col3?
rdd1=sc.textFile("file:///home/hduser/empdata.csv")

#filter transformation:
# filter is a higher of method, used to apply any conditions on every actual element or transformed element of a given RDD.
#filter equivalent construct in python is ? forloop and if condition
#filter equivalent in DB is ? select and where clause

sal_lst=[10000,20000,15000,30000]
#I wanted to filter the sal >=20000
for sal in sal_lst:
    if sal>=20000:
        print(sal)

rdd1=sc.parallelize([10000,20000,15000,30000],2)
rdd2=rdd1.filter(lambda x:x>=20000)#Filter will iterate every element as like map, then apply condition if returns True then
# filter allow the element to the next rdd, else ignore that element
print(rdd2.collect())

rdd1=sc.textFile("file:///home/hduser/empdata.csv")
rdd2=rdd1.map(lambda elem:elem.split(","))
rdd3=rdd2.filter(lambda x:int(x[2])>=20000)#transforming x[2] to int type
print(rdd3.collect())

#Usecase for standardization of data using spark core programming: map and filter function? I need the output of type int,string,int with 3 columns, if less than 3 columns, ignore it
#101,raja,10000
#102,vinay,15000
#103,karthik
#104
#We wanted to reject unwanted data and create rdd with 3 columns data then convert to dataframe asap
rdd1=sc.textFile("file:///home/hduser/empdata1.csv").map(lambda x:x.split(","))
rdd2=rdd1.filter(lambda x:len(x)==3)
print(rdd2.collect())
spark.createDataFrame(rdd2).toDF("id","name","sal").show()

#flatmap transformation (active):
# flatmap is a higher of method, used to iterate on the given list of values and flatten it (transpose it) like explode function in DB
# flatmap runs nested for loops when comparing with python programming
lst1=["hadoop spark hadoop kafka","hadoop datascience java cloud"]
flat_lst=[]
for i in lst1:
 for j in i.split(" "):
  flat_lst.append(j)
  print(j)
# flatmap uses select and explode functionalities as like SQL
rdd1=sc.textFile("file:///home/hduser/sparkdata/courses.log")
rdd2=rdd1.flatMap(lambda x:x.split(" "))

#Interview Question: Difference between map and flatMap?
#map will iterate at the first level (rows), flatmap will iterate 2 levels (rows and columns)
#map is to apply any transformation on every elements, flatmap is to apply transpose/flatten on every elements
#map can be applied on structured, flatmap can be applied on both type of struct or unstructured data also.

#Write a word count program using spark core? How to identify the occurance of the given words in a unstructured dataset?
rdd1=sc.textFile("file:///home/hduser/sparkdata/courses.log")
rdd2=rdd1.flatMap(lambda x:x.split(" "))
print(rdd2.countByValue())

#zipWithIndex - is used for adding index sequence into the elements of the RDD
#In python I want to display both element and the index value? enumerate(lst)
#In SQL we need the row and the row_number? row_number() over()
print(rdd1.zipWithIndex().collect())
#Realtime usage of zipwithindex

#Interview question: How to do you remove the header from a given dataset?
print(rdd1.zipWithIndex().filter(lambda x:x[1]>0).map(lambda x:x[0]).collect())

#Transformations that operates between RDDs

#zip (passive) and zipwithindex (passive) transformations
#zip is used to unconditionally merge/join the data horizontally with other dataset which doesn't have any identified to join
#thumb rules to use zip - the number of partitions and the partition elements between the rdd should be same
'''
1,irfan,42
2,xyz,40
3,abc,30

Chennai,IT
Banglore,MKT
Mumbai,IT
'''
rdd1=sc.textFile("file:///home/hduser/custmaster").map(lambda x:x.split(",")).map(lambda x:[x[0],x[1],x[2]])
rdd2=sc.textFile("file:///home/hduser/custdetail").map(lambda x:x.split(",")).map(lambda x:[x[0],x[1]])
rdd1.zip(rdd2).map(lambda x:(x[0][0],x[0][1],x[0][2],x[1][0],x[1][1])).collect()

#set functions (union (unionall), intersection, subtract) - all set are active trans
rdd1=sc.textFile("file:///home/hduser/custmaster1")
rdd2=sc.textFile("file:///home/hduser/custmaster2")

#union (will not ignore duplicates) will act like union all in DB (active)
print(rdd1.union(rdd2).collect())

#union with distinct will act like a union in DB (ignore duplicates) (active)
#I want to deduplicate the duplicate data in the given RDD or I wanted to implement union (deduplicated) rather than union all?
print(rdd1.union(rdd2).distinct().collect())

#intersection (active) - used to identify the common data between the rdds
print(rdd1.intersection(rdd2).collect())

#subtract (active) - used to subract an rdd from another rdd
print(rdd1.subtract(rdd2).collect())
print(rdd1.subtract(rdd2).collect())

#paired RDD - An RDD that contains the data in a form of key,value pair, then it is paired RDD.
#Certain Transformations (paired rdd transformations)/actions (paired rdd actions) only work on paired rdds, hence paired rdds are used in spark

#Eg. of paired RDD and paired RDD Transformations
#join is a paired rdd transformation, works only on paired rdd
#We can join multiple rdds using spark core join paired rdd transformation, but not much preferred rather it is preferred to
#execute joins using spark sql (because it is optimized by default using catalyst optimizer in spark sql)
rdd1=sc.textFile("file:///home/hduser/custmaster1").map(lambda x:x.split(",")).map(lambda x:(x[0],(x[1],x[2])))
rdd2=sc.textFile("file:///home/hduser/custdetail").map(lambda x:x.split(",")).map(lambda x:(x[2],(x[0],x[1])))
rdd1.join(rdd1).collect()#self
rdd1.join(rdd2).collect()#inner
rdd1.leftOuterJoin(rdd2).collect()#left
rdd1.rightOuterJoin(rdd2).collect()#right
rdd1.fullOuterJoin(rdd2).collect()#full

#reduceByKey is a paired rdd transformation, works only on paired rdd
#I need to reduce the result based on the key, we use reducebykey transformation
#reduceByKey will work like reduce action, reduceByKey work based on the key the reduction will happen but reduce will do reduction based on values
rdd1=sc.textFile("file:///home/hduser/hive/data/txns").map(lambda x:x.split(",")).map(lambda x:(x[7],float(x[3])))
#rdd1.reduceByKey()
lam=lambda b,f:b if b>f else f
print("state wise max of sales",rdd1.reduceByKey(lam).take(3))
lam=lambda b,f:b if b<f else f
print("state wise sum of sales min sales",rdd1.reduceByKey(lam).take(3))
lam=lambda b,f:b+f
print("state wise sum of sales",rdd1.reduceByKey(lam).take(3))
#Interview Questions: Difference between reduce and reducebykey, difference between countbykey and reducebykey
#difference between groupbykey and reducebykey...

print("Dependent Transformations : Narrow (map, filter) & Wide Dependent (reducebykey, join) Transformations")
#Narrow dependent Transformation
rdd1=sc.textFile("file:///home/hduser/hive/data/txns").map(lambda x:x.split(",")).filter(lambda x:x[7]=='California')
#Wide dependent Transformation
rdd1=sc.textFile("file:///home/hduser/hive/data/txns").map(lambda x:x.split(",")).map(lambda x:(x[7],float(x[3])))
rdd2=rdd1.reduceByKey(lambda x,y:x+y)
rdd2.collect()
#txns=100000 -> 50k(p1-map1)-500 Cal, 50k(p2-map2)-1000 Cal -> reducebykey1 Cal,[500+1000]
#txns=100000 -> 50k(p1-map1)-1500 Ten, 50k(p2-map2)-1000 Ten -> reducebykey2 Ten,[1500+1000]


print("4. How to Materialize/Actions RDDs - Not much important")
#Action is an RDD function/method used to return the result/value to the driver or to the storage layer
#collect action - Used to collect the rdd elements as a result to the Driver.

#Interview Questions: collect action has to be causiously used or avoid using collect because?
#collect bring all data from multiple executors to one driver, hence consumption of resources like network and memory is high
#collect may reduce the performance of the application when used on a large of volume of data
#collect may break the application with OOM exception when used on a large of volume of data
#alternative for collect?? - sampling, storage in disk, take, first, top, count ...
#Conclusion: Collect should be used for development, testing or in Production (if it is inevitable)
rdd1=sc.textFile("/user/hduser/custs")
len(rdd1.collect())
print("costly effort as we are collecting all data to driver, then counting sequencially using python len function",len(rdd1.collect()))
print(rdd1.count())
print("less cost effort as we are counting parallely/distributedly and returning the count and not the data, "
      "then consolidating by running count in the driver finally on the partial counts and not on the data",rdd1.count())

#Other alternative actions for collect if we want to see the data (few, one, ordered, sample)
print(rdd1.take(3))
#Random sampling transformation
print(rdd1.takeSample(True,3,2))#used for random sampling - withreplacement-True will ensure to return unique samples, num (total samples), seed (controlling of random samples using seed value)
print(rdd1.takeOrdered(3))#Ascending order of sorting and take the top values
print(rdd1.first())
print(rdd1.top(3))#Descending order of sorting and take the top values

#Usecase: Interview question: How to do you remove the header from a given dataset using take, first?
'''
id,name,age
1,irfan,42
2,aaa,30
'''

#quick usecase to understand all 4 parts of spark core?
#Defining spark session, creating rdds, transforming rdd, action perfomed
#Define spark session object / sc
#Create an RDD from the file:///usr/local/hadoop/logs/hadoop-hduser-namenode-localhost.localdomain.log
#Apply transformation to filter only ERROR and WARN data from the above rdd
#Store the error data in /user/hduser/errdata/ and warning data in /user/hduser/warndata/
#part1
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local[*]").appName("Spark log parsing").getOrCreate()
sc = spark.sparkContext
#part2
file_rdd1=sc.textFile("file://usr/local/hadoop/logs/hadoop-hduser-namenode-localhost.localdomain.log").map(lambda x:x.split(" "))
#part3
filtered_rdd=file_rdd1.filter(lambda x:len(x)>=2)
#file_rdd1=sc.textFile("file:///usr/local/hadoop/logs/hadoop-hduser-namenode-localhost.localdomain.log")
#err_rdd=file_rdd1.filter(lambda x:'ERROR' in x)
#warn_rdd=file_rdd1.filter(lambda x:'WARN' in x)
err_rdd=filtered_rdd.filter(lambda x:x[2]=='ERROR')
#print(err_rdd.count())
warn_rdd=filtered_rdd.filter(lambda x:x[2]=='WARN')
#print(warn_rdd.count())
#part4
err_rdd.saveAsTextFile("/user/hduser/errdata")
warn_rdd.saveAsTextFile("/user/hduser/warndata")

#code in a single line
#sc.textFile("file://usr/local/hadoop/logs/hadoop-hduser-namenode-localhost.localdomain.log").map(lambda x:x.split(" ")).filter(lambda x:len(x)>=2).filter(lambda x:x[2]=='ERROR').saveAsTextFile("/user/hduser/errdata")
#sc.textFile("file://usr/local/hadoop/logs/hadoop-hduser-namenode-localhost.localdomain.log").map(lambda x:x.split(" ")).filter(lambda x:len(x)>=2).filter(lambda x:x[2]=='WARN').saveAsTextFile("/user/hduser/warndata")


print("running countbyvalue,countbykey etc.,")
rdd1=sc.textFile("/user/hduser/custs").map(lambda x:x.split(","))
rdd2=rdd1.map(lambda x:x[4])
print(rdd2.countByValue())

#Actions also of type paired rdd actions - Actions work on paired rdd is paired rdd actions like lookup, countByKey, combineByKey etc.,
#CountByKey is a paired rdd action, since it requires the key,value pair rdd as an input
#CountByKey will be used to count the occurance of the key
rdd2=rdd1.map(lambda x:(x[4],int(x[3])))
rdd2.countByKey()#paired RDD Action

#lookup is a paired rdd action, since it requires the key,value pair rdd as an input
#lookup is used for getting the result for the given key passed as an argument (it is an alternative for left join)
#lookup and enrichment - lookup for the customerid and enrich the customer name
rdd2=rdd1.map(lambda x:(x[0],(x[1],x[2])))
rdd2.lookup('4000001')

#reduce() action help us reduce/consolidate/combine the result in any customized way we needed the result
#lambda function, lambda service in cloud, lambda architecture for data management
#How the name lambda has been brought in?
#3 5 8
#8 8
#16
lam=lambda b,f:b if b>f else f
rdd1=sc.textFile("file:///home/hduser/hive/data/txns").map(lambda x:x.split(",")).filter(lambda x:x[7]=='California').map(lambda x:float(x[3]))
lam=lambda b,f:b+f
def met1(b,f):
    return b+f
print("sum of amt",rdd1.reduce(lam))
rdd1.sum()
rdd1.min()
lam=lambda b,f:b if b<f else f
print("max of amt",rdd1.reduce(lam))
lam=lambda b,f:b if b>f else f
print("max of amt",rdd1.reduce(lam))
rdd1.max()

#Can you write word count program in spark core?


#Important (Core/SQL/Stream)- 5. Spark Arch/terminologies (first cut), Performance Optimization, Job Submission, Spark Cluster Manager & Deploy Mode concepts (VV Important) - not used only for core, used in SQL & Streaming also
print("5. Spark Arch/terminologies, Performance Optimization, Job Submission, Spark Cluster concept (VV Important) - not used only for core, used in SQL & Streaming also")
#Performance Optimization (foundation concepts) & Best practices- primary activity

#First follow the Best practices - pluck the Low hanging fruits
#1.A. Remove all unwanted (costly) actions performed while doing the development like collect & print/take/count ... and use it if it is really needed
#Removal of dead codes/dormant codes, removal of the unused rdds
#1.B. Applying predicate (row) & projection (column) pushdown where ever possible
#predicate pushdown (pick only the rows needed)- i have 100 columns and 1 billion rows in the base data, i need to filter only california rows with all columns needed?
rdd1=sc.textFile("file:///home/hduser/hive/data/txns").map(lambda x:x.split(",")).filter(lambda x:x[7]=='California') #costly, no predicate pushdown
#or
rdd2=sc.textFile("file:///home/hduser/hive/data/txns").filter(lambda x:'California' in x).map(lambda x:x.split(",")) #less cost, predicate pushdown

#Projection pushdown (pick only the columns needed) - i have 100 columns and 1 billion rows in the base data, i need to calculate howmany california data is there?
rdd1=sc.textFile("file:///home/hduser/hive/data/txns").map(lambda x:x.split(",")[7]).filter(lambda x:x=='California') #projection pushdown
print(rdd1.count())

#2. Partition management - When creating RDDs (naturally and customized),  before/after transformation , before performing action
#What is Partitioning -
# Partitioning the Horizontal division of data - hdfs (block), mr (inputsplit), sqoop (mapper), hive (bucket, partition, mr), yarn (containers), spark (RDD)..
#Partitioning is used for defining the degree of parallelism and distribution
#Partition help us distribute the data (in terms of spark partition help us distribute the data across multiple nodes in memory, in a form of RDD partitions)

#  (number of rows in a partition) partitioning or range of size of data
# coalesce - range partitioning (range of values in a given partition) or data size is random/different in diff partitions
# repartition (internally coalesce(shuffle=True)) - round robin  partitioning

#1. While creating RDDs how the partitions are assigned by default (naturally)
#Partition management - When creating RDDs (how by default the number of partitions are assigned?)
#A.When RDDs are created programatically? No. of partitions = number of cores allocated (2 executors with each 2 cores = 4 cores (executors*cores))
rdd1=sc.parallelize(range(1,200))
print(rdd1.getNumPartitions())#return 4
#B.When RDDs are created from local file system? 1 partition = 32mb size
rdd1=sc.textFile("file:///home/hduser/hive/data/txns_big1")#size of the file is 202mb then 202/32=7
print(rdd1.getNumPartitions())#return 7
#C.When RDDs are created from HDFS file system? 1 partition = 128mb size
rdd1=sc.textFile("/user/hduser/txns_big1")#size of the file is 202mb then 202/128=2
print(rdd1.getNumPartitions())#return 2
#D. When RDD is created from lfs/hdfs (if the size of data is <32mb or 128mb) ? default with 2 partitions
rdd1=sc.textFile("/user/hduser/txns")#size of the file is <32mb/128mb then default is 2 partitions
print(rdd1.getNumPartitions())#return 2

#2. Partition management - when creating RDDs (how to change the partitions manually)
#A.When RDDs are created programatically? I want to change as per my need, i can slice it with any number i want
rdd1=sc.parallelize(range(1,200),2)#mention number of slices/partitions to be applied
print(rdd1.getNumPartitions())#return 2

#B.When RDDs are created from local file system? I want to change as per my need, i can defining the minumum number of partitions
rdd1=sc.textFile("file:///home/hduser/hive/data/txns_big1",10)#mention Minimum number of partitions can be applied
print(rdd1.getNumPartitions())#return 10

rdd1=sc.textFile("file:///home/hduser/hive/data/txns_big1",4)#minimum i need 4, anything >=4 is acceptible
print(rdd1.getNumPartitions())#return 7

#C.When RDDs are created from HDFS file system? 1 partition = 128mb size (blocksize)
rdd1=sc.textFile("/user/hduser/txns_big1",4)#anything >=4 is acceptible
print(rdd1.getNumPartitions())#return 4

#D. When RDD is created from lfs/hdfs (if the size of data is <32mb or 128mb) ? default with 2 partitions
rdd1=sc.textFile("/user/hduser/txns",4)#anything >=4 is acceptible
print(rdd1.getNumPartitions())#return 4

#Conclusion in changing the partition while defining the rdd is - we can't reduce the number of partitions returned at a time of creating rdd from file sources
#We can achive it by using some functions later by creating another rdd

#3. How do we manage/handle/change the number of partitions after defining an RDD and before/after performing transformations
#How the partitions can be increased or decreased in an RDD
rdd1=sc.parallelize(range(1,200))
print(rdd1.getNumPartitions())#number of partition is 4 = total number of cores allocated

#To increase/decrease(not advisable) the number of partitions we use repartition function
rdd2_part10=rdd1.repartition(10)#increase partition (preferred)
print(rdd2_part10.getNumPartitions())

rdd2_part2=rdd1.repartition(2)#reduce partition (not prefered)
print(rdd2_part2.getNumPartitions())

#To decrease the number of partitions we use coalesce function
rdd2_part2=rdd1.coalesce(2)#reduce partition (preferred)
print(rdd2_part10.getNumPartitions())

#When the partitions can be increased or decreased in an RDD
#pyspark --master yarn --deploy-mode client --num-executors 2 --executor-memory 1g --executor-cores 2 --driver-memory 1g
#We have allocated 4 processor cores in 2 executors

#Scenario1 (increase partitions) - I want to increase the number of partitions before performing a transformation
rdd1=sc.textFile("/user/hduser/txns_big1")#2 partitions are created=128 mb 2 blocks
print(rdd1.getNumPartitions())#return 2

#Map tasks are going to be launched in 2 executors on 2 partitions, rather than 4 (which is more optimal )
rdd2=rdd1.map(lambda x:x.split(','))#Running the transformation without increasing number of partitions
rdd2.count()#performance degraded

rdd2=rdd1.repartition(4).map(lambda x:x.split(','))#Running the transformation increasing the number of partitions
rdd2.count()#performance improved

rdd2=rdd1.repartition(40).map(lambda x:x.split(','))#Running the transformation increasing the number of partitions
rdd2.count()#performance degraded

#Conclusion - When we increase the partitions the improvement of performance is good ? Yes (obvious) /No (at times)

#Scenario2 (reduce partition) - I want to reduce the number of partitions before performing a transformation
rdd2=rdd1.repartition(4).map(lambda x:float(x.split(',')[3]))
rdd3=rdd2.coalesce(2).filter(lambda x:x>10)#filter wanted the data in a reduced partition because we have only one column to filter
rdd3.count()

#Scenario3 (increase partitions) - I want to increase the number of partitions after performing a transformation
#If i am performing few transformations like this, i may require to increase number of partitions - active (union, join, flatmap)
rdd1=sc.textFile("/user/hduser/txns_big1")#2.3 million rows
rdd2=rdd1.flatMap(lambda x:x.split(",")).repartition(8)#21 million rows, hence increase the number of partition after flatMap

#Scenario4 (reduce partition) - I want to reduce the number of partitions after performing a transformation
rdd2=rdd1.repartition(4).map(lambda x:x.split(',')).filter(lambda x:x[7]>"California").coalesce(1)#the filtered count is reduced, hence lesser partition will do
rdd2.count()

#Before we run an action, we can change the number of partitions?
#Interview Question? I am going to write the output of a rdd in a filesystem, how many number of files will be created by default?
#Answer is number of files = number of partitions
rdd1.saveAsTextFile("/user/hduser/rdd1data2")#2 files will be created since 2 partitions are there for rdd1

#I need the number of files creation has to be controlled?
rdd1.coalesce(1).saveAsTextFile("/user/hduser/rdd1data3")#1 file will be created since 1 partition is coalesced on rdd1

#Conclusion is - Increase in number of partition is going to help us improve the performance (but not always), we can decrease/use right number of partitions
#before or after the usage of transformation will help use improve the performance



#For a 202mb file in Lfs whr default block is 32mb.if we set partition as 4 using coalesce how the partition size will be adjusted?

#Interview Questions
#Difference between repartition and coalesce?
#Direct answer - Partition management functions used for increasing or decreasing number of partitions respectively

#How the repartition and coalesce is working internally to distribute the partitions?
#There are different partitioning algorithms used for distributing data - range, round robin, random partitioning
#Basically coalesce is represented in 2 functions (coalesce and repartition)

#When we have use repartition & coalesce, which is more preferred for reducing the number of partitions?
#How do you produce the output as a single file in spark?
#How to find the number of elements in a given rdd?
#How to find the number of elements in the each partition of a given rdd?

#Difference between repartition and coalesce?
#Direct answer - Partition management functions used for increasing or decreasing number of partitions respectively
#Basically coalesce is represented in 2 functions (coalesce and repartition) - Arguable point (not much people knows this)
#def repartition(self, numPartitions):
#    return self.coalesce(numPartitions, shuffle=True)

rdd1=sc.parallelize(range(1,201),2)#1 minute to complete the work
rdd1.getNumPartitions()#total number of partitions
rdd1.glom().collect() #partition wise elements
#Coalesce helps reduce the number of partitions
rdd1.coalesce(1).getNumPartitions()#(2 minute to complete the work) Passing shuffle true can make coalesce behave like repartition

#not used as given below (only for understanding purpose)
rdd1.coalesce(4,shuffle=False)#this won't increase the partition because shuffle false is passed
rdd1.coalesce(4,shuffle=True).getNumPartitions()#(30 seconds to complete the work) Passing shuffle true can make coalesce behave like repartition
#or
rdd1.repartition(4).getNumPartitions()#(30 seconds to complete the work) Passing shuffle true can make coalesce behave like repartition

print("Increased Partition wise rows count using Repartition",
      "1. Benifit: Repartition uses Round Robin Distribution, hence we will equal partition size by avoiding partition skewing (one partition size is more than the other))"
      "2. Drawback: Repartition will do shuffling (by default) or redistribution of data (hence costly function to use)"
      "3. Conclusion: Repartition is a costly function to use (use it causiously), only for increasing the number of partition use repartition, not for reducing")

#"1. Repartition uses Round Robin Distribution, hence we will equal partition size by avoiding partition skewing"
rdd1=sc.parallelize(range(1,2001),4)
for i in rdd1.repartition(10).glom().collect():
 print(len(i))

print("Decreased Partition wise rows count using coalesce",
      "1. Drawback: Coalesce uses Range Distribution (partitions will be merged), hence equal partition size cannot be assured"
      "2. Benifit: Coalesce preferably will not do shuffling (but shuffling may happen at times) , hence coalaese is less cost"
      "3. Conclusion: Coalesce is a less cost function is supposed to use for reducing the number of partitions (we can't increase number of partition using coalesce)")
for i in rdd1.coalesce(3).glom().collect():
 print(len(i))

#Interview Question: How to do count the number of elements at the partition level
rdd1=sc.parallelize(range(1,2001),4)
for i in rdd1.glom().collect():
 print(len(i))

#3. Memory Optimization - Performance Optimization using Cache/Persist once RDD is created

rdd1=sc.textFile("file:///home/hduser/empdata.csv")#transformation - data will be in source disk only
rdd1.count()#REFRESH MEMORY - action - Data loaded to executor memory and deleted after action is performed
rdd1.cache()#RETRAIN MEMORY - transformation - data will be in source disk only, I am asking Garbage collector retain the data after loaded to memory
rdd1.count()#REFRESH MEMORY action - Data loaded to executor memory and not deleted after action is performed
rdd1.count()#RETAIN MEMORY action - count will be performed on the data in executor memory retained
rdd1.count()#RETAIN MEMORY action - count will be performed on the data in executor memory retained
rdd1.unpersist(blocking=False)#GC clean the data, but allow further lines of code to execute when you clean
rdd2=sc.textFile("file:///home/hduser/empdata.csv")
rdd1.cache()
rdd1.count()
rdd1.unpersist(blocking=True)#GC clean the data, Dont allow further lines of code to execute until you clean
rdd2=sc.textFile("file:///home/hduser/empdata.csv")

#Few limitations/challenges in Caching/persist:
#1. If the underlying data of the cached is changed, then we may not get expected results (happens on streaming applications)
# to overcome this issue - we have to refresh the cache frequently if the source is changeable
#2. Caching has to considered with other factors like volume of data, availability of resources, time taken for serialization/deserialization etc.
#3. Caching has to be used appropriately (if we reuse the given rdd) and cleaned appropriately (after used)
#4. Use cache appropriately before actions are performed on the parent or child rdds (subjective).
rdd1=sc.textFile("file:///home/hduser/empdata.csv")#caching of parent rdd is better? No
#rdd1.cache()
rdd2=rdd1.filter(lambda x:'bala' in x)#caching of child rdd is better? Yes
rdd2.cache()#less volume data is cached
rdd2.count()
rdd2.collect()
rdd3=rdd1.filter(lambda x:'karthik' in x)#caching of child rdd is better? Yes
rdd3.cache()#less volume data is cached
rdd3.count()
rdd3.collect()

rdd1=sc.textFile("file:///home/hduser/empdata.csv")#caching of parent rdd is better? Yes
rdd1.cache()
rdd2=rdd1.flatMap(lambda x:x.split(","))#caching of child rdd is better? No
#rdd2.cache()#huge volume data is cached
rdd2.count()
rdd2.collect()
rdd3=rdd1.flatMap(lambda x:x.split(","))#caching of child rdd is better? No
#rdd3.cache()#huge volume data is cached
rdd3.count()
rdd3.collect()

#5. Usage of the right type of cache in the name of persist is supposed to be considered.
#persist with 10 options are available, use them appropriately..

#Interview Question: I have Executor memory less than the size data in HDD, can we still process that data using Spark?
#Yes, Internally spark only bring the data into the RAM upto how much can be accomodated and retain, rest will be in source and brought iteratively

#What is the differece between cache and persist? Very very important interview question
#cache and persist are the same functions used to ask GC not to clean the data from the executor memory
#cache can only hold the data in memory only
#persist has multiple option (10 options) to hold the data in different storage levels (has to be used appropriately)

#pyspark --executor-memory 512m
#cache/persist with any storagelevels will be used when multiple actions performed
rdd2=sc.textFile("file:///home/hduser/hive/data/txns_verybig1")#size of the data is 780mb
rdd2.cache()#three are same depending on pure memory serialized with 1 time replica
rdd2.persist()#three are same depending on pure memory serialized with 1 replica

from pyspark.storagelevel import StorageLevel
rdd2.persist(StorageLevel.MEMORY_ONLY)#three are same depending on pure memory serialized with 1 replica
#all above 3 storage levels are used if the data size is fit with the executor capacity
rdd2.unpersist()
rdd2.persist(StorageLevel.MEMORY_ONLY_2)#pure Memory only serialized with 2 replica
#above storage levels are used if the data size is fit with the executor capacity
# and needed for reference with higher availability to avoid referring the data from source again
rdd2.unpersist()
rdd2.persist(StorageLevel.MEMORY_AND_DISK)#Serialized Data stored in both memory(which ever can be accomodated) and disk (balance data) with 1 replica
rdd2.unpersist()
rdd2.persist(StorageLevel.MEMORY_AND_DISK_2)#Serialized Data stored in both memory(which ever can be accomodated) and disk (balance data) with 2 replicas
rdd2.unpersist()
#above storage levels are used if the data size is not fit with the executor capacity (data size > executor capacity)
# but needed for avoid referring the data from source again, with 2 replica the high availbility will be ensured
rdd2.persist(StorageLevel.DISK_ONLY)#Serialized Data stored in both memory(which ever can be accomodated) and disk (balance data) with 2 replicas
rdd2.unpersist()
rdd2.persist(StorageLevel.DISK_ONLY_2)#Serialized Data stored in both memory(which ever can be accomodated) and disk (balance data) with 2 replicas
rdd2.unpersist()
rdd2.persist(StorageLevel.DISK_ONLY_3)#Serialized Data stored in both memory(which ever can be accomodated) and disk (balance data) with 2 replicas
rdd2.unpersist()
#above storage levels are used if we don't need to use executor capacity to hold the data, as other programs need the
#executor memory, but needed to refer data from the local storage rather than referring the data from source again,
# with 2/e replica the higher/highest availbility will be ensured
rdd2.persist(StorageLevel.OFF_HEAP)#Serialized data kept in the off heap space of our JVM
rdd2.unpersist()
#above (experimental) storage levels is used if we want to leverage the unused off heap space rather than on heap space (default)

#Broadcasting - Important to know for interview and for usage especially in the spark optimized joins
#Broadcasting is the concept of broadcast something that is referred/used/consumed by the executors frequently/anonymousle/randomly
#Real life Eg. radio, video live streaming
#Interview Question? Have you used broadcasting in spark or Did you tried optimized joins in spark SQL?
# Spark Broadcasting is the special static variable that can broadcasted once for all from driver to worker (executors),
#hence spark rdd partition rows can refer that broadcasted variable locally rather than getting it from driver for every iteration

#Interview Question?
# Can we broadcast an RDD?  How much data can be collected to the driver at given point of time?
#
#How much rows a rdd or variable can be to be broadcasted? default is 10mb
#It depends upon the capacity of the driver memory allocation

driver_bonus=100 #driver
brodcast_driver_bonus=sc.broadcast(driver_bonus) #Driver to Worker
sal_lst=[10000,20000,30000,40000] #driver
rdd1=sc.parallelize(sal_lst,2) #driver to Executor
rdd1.collect()#Executor to driver
rdd2=rdd1.map(lambda x:x+driver_bonus)#the bonus of 100 is transferred from driver to the exeutor for every salary
# iteration of the mapper n number of times hence network transfer (huge) of bonus is happening multiple times between driver to the executor
rdd2.collect()
#or
rdd2=rdd1.map(lambda x:x+brodcast_driver_bonus.value)#Worker to Executor
#the bonus of 100 is transferred oncefor all to the worker node and referred by the mapper n number of times
#hence network transfer of bonus is happening once between driver to the executor
rdd2.collect()

#Accumulator - Not much important in general usage, but needed for some interview discussions
#Accumulator is special incremental variable used for accumulating the number of tasks performed by the executors
#Accumulator is used to identify the progress completion of tasks running the in the respective executors...
#Accumulators used in spark framework for creating job counters (logging/task completition percent/number of tasks completed)
accum=sc.accumulator(0)
rdd1=sc.parallelize(range(1,101))
rdd1.foreach(lambda x:accum.add(1))
rdd1.map(lambda x:accum.add(1)).count()

##We have completed Spark Core - All 5 components, performance optimization
# (basic best practices, partitioning, memory managment, broadcast & accumulator)
# we will see more in detail (very very important) - (we see everything else including what you ask or dont ask)

#Spark core is completed here.. Important part is 1 and 5, non important items 2,3 & 4