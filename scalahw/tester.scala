
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
