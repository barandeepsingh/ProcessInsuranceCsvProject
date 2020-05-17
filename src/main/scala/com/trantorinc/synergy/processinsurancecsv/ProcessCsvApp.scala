package com.trantorinc.synergy.processinsurancecsv

object ProcessCsvApp extends App {

  println("CSV demo")
  val spark = SparkSession.builder().appName("PostgresApp").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("FATAL")

  val summerSlamRDD = spark.sparkContext.textFile("/Users/baran/Documents/spark-data/summerslam.csv")
  val hellInACellRDD = spark.sparkContext.textFile("/Users/baran/Documents/spark-data/hell_in_a_cell.csv")
  case class PPV(challenger: String, champion: String, title: String, ppv:String)

  val summerSlamMappedRDD = summerSlamRDD.map { entry =>
    val cols = entry.split(",")
    PPV(challenger = cols(0), champion = cols(1), title = cols(2), "Summerslam")
  }

  val hellInACellMappedRDD = hellInACellRDD.map { entry =>
    val cols = entry.split(",")
    PPV(challenger = cols(0), champion = cols(1), title = cols(2), "Hell In A Cell")
  }

  val summerSlamDF = summerSlamMappedRDD.toDF.as[PPV]
  val HellInACellDF = hellInACellMappedRDD.toDF.as[PPV]

  val WweDF = summerSlamDF.union(HellInACellDF)

  WweDF.show
  //summerSlamDF.select("champion","challenger").where("title like '%Universal%'").show



  //summerSlamDF.printSchema()

  //summerSlamDF.createOrReplaceTempView("summerSlam")

  //spark.sql("select * from summerSlam").select("challenger").where("champion='Kofi'").show


}
