# spark-feature-engineering-toolkit
Snippets of spark/scala code used to do some handy feature engineering.

Many seems simple but they save a lot of LoC and make the code more readable.

Having a dataset :

| id_client  | timestamp_interaction   | montant_dhs | lieu_interaction | type_interaction |
| ---------- |:------------------------| -------:|------------------|----------------------|
| sgjuL0CYJf | 2019-01-01T09:01:25.000 | 1500    |   GAB MAROC      | retrait_gab          |
| 17AbPYC2M7 | 2019-02-01T01:08:23.000 | 1207.10 |   MAROC          | paiement_tpe         |
| sgjuL0CYJf | 2019-01-03T07:01:21.000 | 200     |   GAB MAROC      | retrait_gab          |
| sgjuL0CYJf | 2019-04-01T01:06:19.000 | 0       |   GAB MAROC      | consultation_solde   |
| 17AbPYC2M7 | 2019-01-05T05:01:17.000 | 500     |   GAB MAROC      | retrait_gab          |
| 17AbPYC2M7 | 2019-06-01T01:04:15.000 | 299     |   INTERNATIONAL  | paiement_tpe         |
| 17AbPYC2M7 | 2019-01-07T03:01:13.000 | 0       |   MAROC          | pin_errone           |

We apply basic transformations 

``` scala
  df
    .withColumn("basedt", lit("2019-07-01"))
    .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
```

This spark/scala code builds features (that could be pivoted) :

## Does an event exist ?

``` scala
val result = df.getCheckFeatures("timestamp_interaction", "basedt", "id_client", 
                                 "pivot", "", Seq(1, 3, 6))
```

Result is

|id_client |pivot                       |nb_1_month|nb_3_month|nb_6_month|
|----------|----------------------------|----------|----------|----------|
|17AbPYC2M7|MAROC_pin_errone            |0         |0         |1         |
|17AbPYC2M7|MAROC_paiement_tpe          |0         |0         |1         |
|17AbPYC2M7|INTERNATIONAL_paiement_tpe  |1         |1         |1         |
|17AbPYC2M7|GAB_MAROC_retrait_gab       |0         |0         |1         |
|sgjuL0CYJf|GAB_MAROC_retrait_gab       |0         |0         |2         |
|sgjuL0CYJf|GAB_MAROC_consultation_solde|0         |1         |1         |

## What is the value of an event ?

``` scala
val result = df.getCheckValueFeatures("timestamp_interaction", "basedt", "montant_dhs", "id_client", 
                                      "pivot", "mt", Seq(1, 3, 6))
```

Result is

|id_client |pivot                       |sum_mt_1_month|sum_mt_3_month|sum_mt_6_month|
|----------|----------------------------|--------------|--------------|--------------|
|17AbPYC2M7|MAROC_pin_errone            |0.0           |0.0           |0.0           |
|17AbPYC2M7|MAROC_paiement_tpe          |0.0           |0.0           |1207.1        |
|17AbPYC2M7|INTERNATIONAL_paiement_tpe  |299.0         |299.0         |299.0         |
|17AbPYC2M7|GAB_MAROC_retrait_gab       |0.0           |0.0           |500.0         |
|sgjuL0CYJf|GAB_MAROC_retrait_gab       |0.0           |0.0           |1700.0        |
|sgjuL0CYJf|GAB_MAROC_consultation_solde|0.0           |0.0           |0.0           |

## How is the event evolving between the interval 1-6 months and the interval 6-12 months ?

``` scala
val result = df.getRatioFeatures("timestamp_interaction", "basedt", "montant_dhs", "id_client", 
                                 "pivot", Seq((1, 6, 6, 12)))
```

Result is

|id_client |pivot                       |ratio_1_6_6_12    |
|----------|----------------------------|------------------|
|17AbPYC2M7|MAROC_pin_errone            |0.0               |
|17AbPYC2M7|MAROC_paiement_tpe          |7.096804156525518 |
|17AbPYC2M7|INTERNATIONAL_paiement_tpe  |0.0               |
|17AbPYC2M7|GAB_MAROC_retrait_gab       |6.2166061010848646|
|sgjuL0CYJf|GAB_MAROC_retrait_gab       |5.303304908059076 |
|sgjuL0CYJf|GAB_MAROC_consultation_solde|0.0               |

More information can be found on https://issam.ma/jekyll/update/2020/03/23/datacience-feature-engineering.html (French)