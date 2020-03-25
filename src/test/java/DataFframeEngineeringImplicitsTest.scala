import org.apache.spark.sql.functions._
import org.scalactic.TolerantNumerics
import org.scalatest.funsuite.AnyFunSuite
import tool.implicits.DataFrameEngineeringImplicits.DataFrameEngineeringImprovements

class DataFframeEngineeringImplicitsTest extends AnyFunSuite {

  val eps = 1e-4f
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(eps)

  test("test_getCheckPivotFeatures") {
    val df = DataLoading.loadCsv("src/test/resources/data-engineering-dataset.csv")

    val result = df
      .withColumn("basedt", lit("2019-07-01"))
      .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
      .getCheckPivotFeatures("timestamp_interaction", "basedt", "id_client", "pivot", "", Seq(1, 3, 6))
      .na.fill(0)

    val expectColumns = Seq("id_client",
      "GAB_MAROC_consultation_solde_nb_1_month",
      "GAB_MAROC_consultation_solde_nb_3_month",
      "GAB_MAROC_consultation_solde_nb_6_month",
      "GAB_MAROC_retrait_gab_nb_1_month",
      "GAB_MAROC_retrait_gab_nb_3_month",
      "GAB_MAROC_retrait_gab_nb_6_month",
      "INTERNATIONAL_paiement_tpe_nb_1_month",
      "INTERNATIONAL_paiement_tpe_nb_3_month",
      "INTERNATIONAL_paiement_tpe_nb_6_month",
      "MAROC_paiement_tpe_nb_1_month",
      "MAROC_paiement_tpe_nb_3_month",
      "MAROC_paiement_tpe_nb_6_month",
      "MAROC_pin_errone_nb_1_month",
      "MAROC_pin_errone_nb_3_month",
      "MAROC_pin_errone_nb_6_month")


    assert(df.count() == 7, "Input CSV seems correct")
    assert(result.count() == 2, "Output dataframe should contain 2 entries")
    expectColumns.foreach(c => assert(result.columns.contains(c), f"Pivot behavior for column ${c} is correct"))
    assert(result.select("GAB_MAROC_retrait_gab_nb_6_month").filter(col("id_client") === "sgjuL0CYJf")
      .collect().apply(0).getLong(0) == 2, "Correct number of retrait_gab for client sgjuL0CYJf")
    assert(result.select("GAB_MAROC_retrait_gab_nb_6_month").filter(col("id_client") === "17AbPYC2M7")
      .collect().apply(0).getLong(0) == 1, "Correct number of retrait_gab for client 17AbPYC2M7")
  }

  test("test_getCheckValuePivotFeatures") {
    val df = DataLoading.loadCsv("src/test/resources/data-engineering-dataset.csv")

    val result = df
      .withColumn("basedt", lit("2019-07-01"))
      .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
      .getCheckValuePivotFeatures("timestamp_interaction", "basedt", "montant_dhs", "id_client", "pivot", "mt", Seq(1, 3, 6))
      .na.fill(0)

    val expectColumns = Seq("id_client",
      "GAB_MAROC_consultation_solde_sum_mt_1_month",
      "GAB_MAROC_consultation_solde_sum_mt_3_month",
      "GAB_MAROC_consultation_solde_sum_mt_6_month",
      "GAB_MAROC_retrait_gab_sum_mt_1_month",
      "GAB_MAROC_retrait_gab_sum_mt_3_month",
      "GAB_MAROC_retrait_gab_sum_mt_6_month",
      "INTERNATIONAL_paiement_tpe_sum_mt_1_month",
      "INTERNATIONAL_paiement_tpe_sum_mt_3_month",
      "INTERNATIONAL_paiement_tpe_sum_mt_6_month",
      "MAROC_paiement_tpe_sum_mt_1_month",
      "MAROC_paiement_tpe_sum_mt_3_month",
      "MAROC_paiement_tpe_sum_mt_6_month",
      "MAROC_pin_errone_sum_mt_1_month",
      "MAROC_pin_errone_sum_mt_3_month",
      "MAROC_pin_errone_sum_mt_6_month")


    assert(df.count() == 7, "Input CSV seems correct")
    assert(result.count() == 2, "Output dataframe should contain 2 entries")
    expectColumns.foreach(c => assert(result.columns.contains(c), f"Pivot behavior for column ${c} is correct"))
    assert(result.select("GAB_MAROC_retrait_gab_sum_mt_6_month").filter(col("id_client") === "sgjuL0CYJf")
      .collect().apply(0).getDouble(0) == 1700.0d, "Correct montant of retrait_gab for client sgjuL0CYJf")
    assert(result.select("GAB_MAROC_retrait_gab_sum_mt_6_month").filter(col("id_client") === "17AbPYC2M7")
      .collect().apply(0).getDouble(0) == 500.0d, "Correct montant of retrait_gab for client 17AbPYC2M7")
  }

  test("test_getRatioPivotFeatures") {
    val df = DataLoading.loadCsv("src/test/resources/data-engineering-dataset.csv")

    val result = df
      .withColumn("basedt", lit("2019-07-01"))
      .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
      .getRatioPivotFeatures("timestamp_interaction", "basedt", "montant_dhs", "id_client", "pivot", Seq((1, 6, 6, 12)))
      .na.fill(0)

    val expectColumns = Seq("id_client",
      "GAB_MAROC_consultation_solde_ratio_1_6_6_12",
      "GAB_MAROC_retrait_gab_ratio_1_6_6_12",
      "INTERNATIONAL_paiement_tpe_ratio_1_6_6_12",
      "MAROC_paiement_tpe_ratio_1_6_6_12",
      "MAROC_pin_errone_ratio_1_6_6_12")

    assert(df.count() == 7, "Input CSV seems correct")
    assert(result.count() == 2, "Output dataframe should contain 2 entries")
    expectColumns.foreach(c => assert(result.columns.contains(c), f"Pivot behavior for column ${c} is correct"))
    assert(result.select("GAB_MAROC_retrait_gab_ratio_1_6_6_12").filter(col("id_client") === "sgjuL0CYJf")
      .collect().apply(0).getDouble(0) === 5.3033d, "Correct ratio of retrait_gab for client sgjuL0CYJf")
    assert(result.select("GAB_MAROC_retrait_gab_ratio_1_6_6_12").filter(col("id_client") === "17AbPYC2M7")
      .collect().apply(0).getDouble(0) === 6.2166d, "Correct ratio of retrait_gab for client 17AbPYC2M7")
  }

  test("readme") {
    val df = DataLoading.loadCsv("src/test/resources/data-engineering-dataset.csv")

    // First example in Readme
    df.withColumn("basedt", lit("2019-07-01"))
      .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
      .getCheckFeatures("timestamp_interaction", "basedt", "id_client", "pivot", "", Seq(1, 3, 6))
      .na.fill(0).show(truncate = false)

    // Second example in Readme
    df
      .withColumn("basedt", lit("2019-07-01"))
      .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
      .getCheckValueFeatures("timestamp_interaction", "basedt", "montant_dhs", "id_client", "pivot", "mt", Seq(1, 3, 6))
      .na.fill(0).show(truncate = false)

    // Third example in Readme
    df
      .withColumn("basedt", lit("2019-07-01"))
      .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
      .getRatioFeatures("timestamp_interaction", "basedt", "montant_dhs", "id_client", "pivot", Seq((1, 6, 6, 12)))
      .na.fill(0).show(truncate = false)
  }

}
