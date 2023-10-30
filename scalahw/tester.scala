
object Tester extends App {
   val result1: Int = HW.q1_countsorted(4,2,1)
   println(result1)

   val result2: String  = HW.q2_interpolation("world", 23)
   println(result2)

   val result3a: Double = HW.q3_polynomial(List(4.0,1.0,2.0))
   
   val result3b: Double = HW.q3_polynomial(Vector(4.0,1.0,2.0))
   
   val result4: Int = HW.q4_application(1,2,3){(x,y) => x+y}
   
   val result5: Vector[String] = HW.q5_stringy(3,4)
   
   val result6: Vector[Int] = HW.q6_modab(3,2, Vector(1,2,3,4,5,6))
   
   val result7: Int = HW.q7_count(Vector(2,3,4)){x => x%2 == 0}
   
   val result8: Int = HW.q8_count_tail(Vector(2,3,4)){x => x%2 == 0}

   val result9a: Double = HW.q9_neumaier(Vector(1.0, 1e100, 1.0, -1e100))

   val result9b: Double = HW.q9_neumaier(List(1.0, 1e100, 1.0, -1e100))



}
// tester.scala

object Tester {
  def test_q1_countedsorted(): Unit = {
    // Test cases for q1_countedsorted
    val result1 = HW.q1_countedsorted(1, 3, 2)
    println(s"Test 1: $result1") // Expected: 2
    val result2 = HW.q1_countedsorted(4, 3, 2)
    println(s"Test 2: $result2") // Expected: 3
    // Add more test cases as needed
  }

  def test_q2_interpolation(): Unit = {
    // Test cases for q2_interpolation
    val result1 = HW.q2_interpolation("Carmen", 23)
    println(s"Test 1: $result1") // Expected: "hello, carmen"
    val result2 = HW.q2_interpolation("John", 18)
    println(s"Test 2: $result2") // Expected: "howdy, john"
    // Add more test cases as needed
  }

  def test_q3_polynomial(): Unit = {
    // Test cases for q3_polynomial
    val result1 = HW.q3_polynomial(List(2.0, 3.0, 4.0))
    println(s"Test 1: $result1") // Expected: 24.0
    val result2 = HW.q3_polynomial(Vector(1.0, 2.0, 3.0, 4.0, 5.0))
    println(s"Test 2: $result2") // Expected: 156.0
    // Add more test cases as needed
  }

  def test_q4_application(): Unit = {
    // Test cases for q4_application
    val result1 = HW.q4_application(2, 3, 4, (x, y) => x + y)
    println(s"Test 1: $result1") // Expected: 9
    val result2 = HW.q4_application(5, 7, 3, (x, y) => x * y)
    println(s"Test 2: $result2") // Expected: 105
    // Add more test cases as needed
  }

  def test_q5_stringy(): Unit = {
    // Test cases for q5_stringy
    val result1 = HW.q5_stringy(2, 3)
    println(s"Test 1: $result1") // Expected: Vector("(2)", "(3)", "(4)")
    val result2 = HW.q5_stringy(0, 5)
    println(s"Test 2: $result2") // Expected: Vector("(0)", "(1)", "(2)", "(3)", "(4)")
    // Add more test cases as needed
  }

  def test_q6_modab(): Unit = {
    // Test cases for q6_modab
    val result1 = HW.q6_modab(2, 3, Vector(1, 2, 3, 4, 5, 6, 7, 8, 9))
    println(s"Test 1: $result1") // Expected: Vector(2, 4, 5, 7, 8)
    val result2 = HW.q6_modab(4, 2, Vector(1, 2, 3, 4, 5, 6, 7, 8, 9))
    println(s"Test 2: $result2") // Expected: Vector(1, 3, 5, 7, 9)
    // Add more test cases as needed
  }

  def test_q7_find(): Unit = {
    // Test cases for q7_find
    val result1 = HW.q7_find(Vector(1, 3, 4, 5, 6, 7, 8))(x => x % 2 == 0)
    println(s"Test 1: $result1") // Expected: 6
    val result2 = HW.q7_find(Vector(1, 3, 5, 7, 9))(x => x % 2 == 0)
    println(s"Test 2: $result2") // Expected: -1 (no even numbers)
    // Add more test cases as needed
  }



  def main(args: Array[String]): Unit = {
    // Call testing functions 
    test_q1_countedsorted()
    test_q2_interpolation()
    test_q3_polynomial()
    test_q4_application()
    test_q5_stringy()
    test_q6_modab()
    test_q7_find()
  }
}
