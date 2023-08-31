import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Lab {

     def q1(sc: SparkContext) = {
        val myfile = sc.textFile("/datasets/wap")
        val mywords = myfile.flatMap{x => x.split(" ")} // mywrods is RDD[String], every item is a word
        val kv = mywords.map{word => (word.length(), 1)}  
        val wc = kv.reduceByKey{(state, y) => state + y}
        save_as(wc, "q1")
     }
     def q2(sc: SparkContext) = {
        val myfile = sc.textFile("/datasets/wap")
        val mywords = myfile.flatMap{x => x.split(" ")} // mywrods is RDD[String], every item is a word
        val filtered = mywords.filter{word => word.length() > 2} // this line is different from q1
        val kv = filtered.map{word => (word.length(), 1)}   // this line uses the previous line
        val wc = kv.reduceByKey{(state, y) => state + y}
        save_as(wc, "q2")
     }

     def q3(sc: SparkContext) = {
        val retail_file = sc.textFile("/datasets/retailtab")
        val noheader = retail_file.filter{line => ! line.startsWith("InvoiceNo")}
        val records = noheader.map{line => line.split("\t")} //RDD and every entry is an array that is a record
        val positive = records.filter{record => record(3).toDouble > 0} 
        val kv = positive.map{record => (record(7), record(3).toDouble * record(5).toDouble)} // RDD[(k, v)] where
                     //k is a country for an order and v is total spent 
        val result = kv.reduceByKey{(state, y) => state + y}
        save_as(result, "q3")
     }
    
     // q4 in mapreduce:
     // def mapper(key, line):
     //     # skip the header, code omitted
     //     parts = line.split(",")
     //     outgoing_airport = parts[?]
     //     incoming_airport = parts[??]
     //     num_pass = parts[???]
     //     yield (outgoing_airport, -num_pass)
     //     yield (incoming_airport, num_pass) 

     // def reducer(key, values):
     //     # key is an airport
     //     yield (key , sum(values))

     def q4_v1(sc: SparkContext) = {
        val flights = sc.textFile("/datasets/flight")
        val noheader = flights.filter{line => !line.startsWith("ITIN_ID")}
        val records = noheader.map{x => x.split(",")}
        val outgoing = records.map{record => (record(3), -record(7).toFloat)} // outgoing pass, num
        // might look like
        //   (DCA,  -10)
        //   (SCE,  -1)
        //   (BWI,  -15)
        val incoming = records.map{record => (record(5), record(7).toFloat)} // incoming pass, num
        // might look like
        //   (DCA, 5)
        //   (IAD, 99)
        //   (LAX, 77)
        val both = outgoing.union(incoming) // RDD[(airport, positive or negative passengers)]
        // might look like
        //   (DCA,  -10)
        //   (SCE,  -1)
        //   (BWI,  -15)
        //   (DCA, 5)
        //   (IAD, 99)
        //   (LAX, 77)
        val result = both.reduceByKey{(state, v) => state + v} 
        save_as(result, "q4_v1")   
     }

     def q4_v2(sc: SparkContext) = {
        val flights = sc.textFile("/datasets/flight")
        val noheader = flights.filter{line => !line.startsWith("ITIN_ID")}
        val records = noheader.map{x => x.split(",")}
        val outgoing = records.map{record => (record(3), -record(7).toFloat)} // outgoing pass, num
        // might look like
        //   (DCA,  -10)
        //   (DCA,  -2)
        //   (SCE,  -1)
        //   (BWI,  -15)
        val incoming = records.map{record => (record(5), record(7).toFloat)} // incoming pass, num
        // might look like
        //   (DCA, 5)
        //   (DCA, 6)
        //   (IAD, 99)
        //   (SCE, 3)
        //   (LAX, 77)
        val both = outgoing.join(incoming)
        //  might look like
        //  (DCA, (-10, 5)) // match first record of outgoing to first of incoming
        //  (DCA, (-10, 6)) // match first record of outgoing to second record of incoming 
        //  (DCA, (-2, 5)) // match 2nd record of outgoing to first of incoming
        //  (DCA, (-2, 6)) // match 2nd record of outgoing to second record of incoming 
        //  (SCE, (-1, 3))
        // too much duplicate info about DCA, no info about LAX and BWI, so we can't continue       
 
     }
     def q4_v3(sc: SparkContext) = {
        val flights = sc.textFile("/datasets/flight")
        val noheader = flights.filter{line => !line.startsWith("ITIN_ID")}
        val records = noheader.map{x => x.split(",")}
        // DCA to LAX    10
        // LAX to DCA     5
        // SCE to LAX     1
        val next_step = records.flatMap{r => List((r(3), -r(7).toFloat),    (r(5), r(7).toFloat))}
        // might look like
        // (DCA, -10) // because of outgoing from DCA, created from record 1
        // (LAX, 10) // because of incoming to LAX, created from record 1 
        // (LAX, -5)
        // (DCA, 5)
        // (SCE, -1)
        // (LAX, 1)
        val result = next_step.reduceByKey{(state, v) => state + v}
        save_as(result, "q4_v3")   
     }

     // how it might look in pythonish (not really how it happens, but we are going to illustrate pipelining
     // f = open(flights dataset)
     // kv = {}
     // for each line in the flight dataset:
     //     if not line.startswith("ITIN_ID"): # the noheader
     //         r = line.split(",")   # the records
     //         kv.add_kv_pair((r[3], -r(7)))
     //         kv.add_kv_pair((r[5], r(7)))
     //  do the reduce
     //  ^^^^ with pipelining pseudocode

     
     // flights = []
     // noheader = []
     // r = []
     // f = open(flights dataset)
     // for line in f:
     //    flights.append()
     // for item flights:
     //     if not flights.startswith("ITIN_ID"):
     //          noheader.append(item)
     // for item in noheader:
     //      r.append(item.split(","))
     // etc.
 
     def save_as[T](rdd: RDD[T], name: String) = {
         rdd.saveAsTextFile(name)

     } 
}
