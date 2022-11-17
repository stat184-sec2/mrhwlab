import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object Ex {
    def getFB(sc: SparkContext): RDD[Array[String]] = {
       sc.textFile("/datasets/facebook").map{x => x.trim()}.map{x => x.split(" ")}
    }

    def getFBTuples(sc: SparkContext): RDD[(String, String)] = {
        getFB(sc).map{x => (x(0), x(1))}
    }

    def examine_rdd[T](myrdd: RDD[T], numlines: Int = 5): Unit = {
        val small_local_array = myrdd.take(numlines)
        small_local_array.foreach{x => println(x)}
    }


    def adj_or_edgelist(fb: RDD[Array[String]]): Boolean = {
        /* Return true if adjacency list and false if edge list

          Does the input RDD look like:
          Adjacency list
          [1, 2, 5, 4]
          [2, 1, 6, 7]  # meaning 2 has neighbors 1,6,7
          [0]   # meaning that 0 has no friends
          or 
          edge list
          [1, 2]  #meaning there is an edge (1,2)
          [1, 5]
          [1, 4]
          ...
        */
        val length_rdd  = fb.map{x => x.length}  // convert an array to its length
        val filtered_rdd = length_rdd.filter{x => x != 2}  // rows that do not look like edge lists
        val num = filtered_rdd.count() // how many rows do not look like edge lists
        num != 0       
    }

    def is_redundant(fb: RDD[(String, String)]): Boolean = {
       //check whether this is in a redundant format (true) or not (false).
       // in a redundant format, if (1,2) is a  line, then so is (2,1)
       //
       // Redundant version:
       // (1, 2)     
       // (1, 3)     
       // (3, 1)     
       // (2, 1)     
       //
       // nonredundant: 
       // (1, 2)     
       // (3, 1)     


       // step 0: quality control, get rid of duplicate rows
       val clean_fb = fb.distinct()       
       // step 1: flip the order of each tuple
       // val flipped = fb.map{x => (x._2, x._1)} //yuckier version of next line
       val flipped = clean_fb.map{case (a, b) => (b, a)} 
       val common = flipped.intersection(clean_fb) //finds rows that are in fb and in flipped
                                             // but also removes duplicates in the answer
       // Example (if we did not do distinct first):
       // (1, 2)
       // (1, 2)
       // (2, 1)
       // (2, 1)
       // (3, 1)
       // flipped:
       // (1, 2)
       // (1, 2)
       // (2, 1)
       // (2, 1)
       // (1, 3)
       // intersection:
       // (1, 2)
       // (2, 1)
       val count_original = clean_fb.count()
       val count_common = common.count()
       count_original == count_common      
    }


    def to_non_redundant(fb: RDD[(String, String)]): RDD[(String, String)] = {
       // Redundant version (with possible duplicates):
       // (1, 2)     
       // (1, 2)     
       // (1, 3)     
       // (3, 1)     
       // (2, 1)     
       
       // Non redundant
       // (1, 2)
       // (1, 3)
       
       val result = fb.filter{case (a,b) => a < b}.distinct()
       // Option 1: distinct before the filter
       //   val result = fb.distinct().filter{case (a,b) => a < b}
       //   this will be better when we need to use fb.distinct() multiple times for
       //   different purposes. In this case, we would create an cached_rdd = fb.distinct().cache()
       //   once someone uses it, it will be stored in memory, and then we could use
       //   cached_rdd.filter{case (a,b) => a < b}
       // Option 2: distinct after the filter:
       //   val result = fb.filter{case (a,b) => a < b}.distinct()
       //   benefit: distinct() requires partitioning an rdd based on the rows
       //   requires moving data around, so it is good to move less data around,
       //   which happens when we use a filter first. 
       result
    }

    def toyGraph(sc: SparkContext): RDD[(String, String)] = {
       val mylist = List(
                   ("A", "B"),
                   ("B", "A"),
                   ("A", "C"),
                   ("B", "C"),
                   ("A", "D"),
                   ("C", "E"),
                   ("D", "C"),
                   ("C", "A"),
                   ("C", "B"),
                   ("D", "A"),
                   ("C", "D"),
                   ("E", "C")
        )

        //   A    ------   B
        //   |    -        |
        //   |      -      |
        //   D    -----    C ------ E
        //
        // 2 triangles: ABC, ADC   
        val myrdd = sc.parallelize(mylist, 2) // take the input list, turn it into an RDD, using
                                              // 2 workers

        myrdd
    }
  
    def numTriangles(graph: RDD[(String, String)]) = {
        // whatever the input graph is, we want to make sure we are
        // working with a redundant version, with duplicates removed
        val flipped = graph.map{case (a, b) => (b, a)}
        val combined = graph.union(flipped).distinct()
      
        val selfjoin = combined.join(combined)       
        // this could look like:
        // ("D", ("A", "A"))
        // ("D", ("A", "C")) -- joining edges (D,A) with (D, C) is a path from A to C via D
        // ("A", ("D", "B"))
        // ("A", ("D", "C"))
        // key: midpoint of the path, and values are the endpoints
  
        //now we remove useless things like ("D", ("A", "A"))
        //val cleaned = selfjoin.filter{x => x._2._1 != x._2._2}   //IMO harder to read
        val cleaned = selfjoin.filter{case (mid, (start, end)) => start != end}
        // ("D", ("A", "C")) -- joining edges (D,A) with (D, C) is a path from A to C via D
        // ("A", ("D", "B"))
        // ("A", ("D", "C"))
        // we would know that ADC is a triangle if there was an edge ("A", "C")
        // so the value tuple says if you have an edge that looks like the value,
        // then we have a triangle
        //
        // "combined" rdd has the list of edges, that is what we need to check
        // so we need to check whether ("A", "C") is in combined, whether ("D", "B") etc.
        //
        // we want to match the value in "cleaned" with the entire row of "combined" to 
        // identify triangles. But, join only allows matching on keys. Solution: flip 
        // the cleaned rdd so that the value becomes the key
        val flippled_clean = cleaned.map{case (a, b) = > (b,a)}
        //     (("A", "C"), "D") -- there is a path from A to C going through D 
        //     (("D", "B"), "A")
        //     (("D", "C"), "A")
    }  
}
