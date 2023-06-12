import tool.DataLoading

object MainRun {

  def main(args: Array[String]): Unit = {

    DataLoading
      .loadCsv("src/main/resources/dummy.csv")
      .show(10)


    //val df = DataLoading.loadCsv("s3a://storecampus/data/dummy.csv")
    //df.write.csv("s3a://storecampus/data/out3.csv")

  }

}
