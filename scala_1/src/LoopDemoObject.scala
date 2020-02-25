object LoopDemoObject {
  def main(args: Array[String]): Unit = {
    for (i <- 1 to 9; j <- 1 to i) {
      print(j + "*" + i + "=" + i * j + "\t")
      if (i == j) {
        println()
      }
    }
  }
}
