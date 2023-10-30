case class Neumaier(sum: Double, c: Double)

// scalahw.scala

object HW {
  // Question 1
  def q1_countedsorted(x: Int, y: Int, z: Int): Int = {
    var count = 0
    if (x < y) count += 1
    if (y < z) count += 1
    if (x < z) count += 1
    count
  }

  // Question 2
  def q2_interpolation(name: String, age: Int): String = {
    val greeting = if (age >= 21) "hello" else "howdy"
    s"$greeting, ${name.toLowerCase()}"
  }

  // Question 3
  def q3_polynomial(arr: Seq[Double]): Double = {
    arr.zipWithIndex.foldLeft(0.0) { (sum, element) =>
      sum + element._1 * math.pow(2, element._2)
    }
  }

  // Question 4
  def q4_application(x: Int, y: Int, z: Int, f: (Int, Int) => Int): Int = {
    f(f(x, y), f(y, z))
  }

  // Question 5
  def q5_stringy(start: Int, n: Int): Vector[String] = {
    Vector.tabulate(n)(i => s"($start)")
  }

  // Question 6
  def q6_modab(a: Int, b: Int, c: Vector[Int]): Vector[Int] = {
    c.filter(x => (x >= a) && (x % b != 0))
  }

  // Question 7
  def q7_find(input: Vector[Int])(f: Int => Boolean): Int = {
    input.reverse.indexWhere(f) match {
      case -1 => -1
      case idx => input.length - idx - 1
    }
  }

  // Question 8
  import scala.annotation.tailrec
  @tailrec
  def q8_find_tail(input: Vector[Int], f: Int => Boolean, currentIndex: Int = 0, lastIndex: Int = -1): Int = {
    if (currentIndex >= input.length) lastIndex
    else {
      if (f(input(currentIndex))) q8_find_tail(input, f, currentIndex + 1, currentIndex)
      else q8_find_tail(input, f, currentIndex + 1, lastIndex)
    }
  }

  // Question 9
  case class Neumaier(sum: Double, c: Double)

  def q9_neumaier(input: Seq[Double]): Double = {
    input.foldLeft(Neumaier(0.0, 0.0)) { (acc, elem) =>
      val sum = acc.sum + elem
      val t = if (Math.abs(acc.sum) >= Math.abs(elem)) acc.c + (acc.sum - sum) + elem
      else acc.c + (elem - sum) + acc.sum
      Neumaier(sum, t)
    }.sum + Neumaier(0.0, 0.0).c
  }


}

