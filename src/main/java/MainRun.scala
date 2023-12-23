import tool.DataLoading

object MainRun {

  def main(args: Array[String]): Unit = {

    DataLoading
      .loadCsv("src/main/resources/dummy.csv")
      .show(10)

  }

}