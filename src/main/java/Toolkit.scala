import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf


object Toolkit {

  val exampleUdf:UserDefinedFunction = udf { (lib: String) => Option(lib).isDefined && lib.startsWith("amIaLib") }

}
