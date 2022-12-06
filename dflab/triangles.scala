// Put your import statements here

object DFlab {

    //create a test rdd, put in the correct types and create a datafrane
    def getTestDataFrame(spark) = {
      List((1,2,3), (4,5,6)).toDF("a", "b", "c")
   }

}
~
