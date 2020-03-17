object MainRun {

  def main(args: Array[String]): Unit = {
    DataLoading
      .loadCsv("src/main/resources/dataset1.csv")
      .show(10)
  }

}
