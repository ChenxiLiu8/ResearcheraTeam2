package com.flight

import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.{SparkConf, sql}

object FlightDataEtlApp {

    /**
        * Pre-processing flight data.
     * 1. Read the raw data
     * 2. Remove the null and "" data rows
     * 3. convert Year, Month, DayofMonth to Date(YYYY-MM-DD), and delete Year, Month, DayofMonth three columns, add Date at the end
     * 4. Replace the data value of NA with 0.
     * 5. output as a csv file
     */
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("app").setMaster("local")
        val session = new sql.SparkSession.Builder().config(sparkConf).getOrCreate()

         *//1. Read the raw data
        val df = session.
          read.option("header", "true")
          .csv("D:\\flight_data\\input\\")

        //println("1. 显示原始5条数据：")
        println(df.show(5))
        //println(s"显示原始数据总条数：${df.count()}")

        //2. Remove the null and "" data rows
        val nullFilterDf = df.filter(row => {
            var res = true
            for (i <- 0 until row.length) {
                //保留CancellationCode为null的
                if (i != 22) {
                    if (row.get(i) == null || row.get(i).equals("")) {
                        res = false
                    }
                }
            }
            res
        })
        //println("2. 显示去除空值之后的5条数据：")
        nullFilterDf.show(5)
        //println(s"去除空值后条数：${nullFilterDf.count()}")

        //3. convert Year, Month, DayofMonth to Date(YYYY-MM-DD), and delete Year, Month, DayofMonth three columns, add Date at the end
        val dateDf = nullFilterDf.withColumn("Date",
            concat_ws("-", nullFilterDf("Year"), nullFilterDf("Month"), nullFilterDf("DayofMonth")))
          .drop("Year")
          .drop("Month")
          .drop("DayofMonth")
          .toDF()

        //println("3. 显示日期转换之后的5条数据：")
        dateDf.show(5)
        //println(s"日期转换之后数据条数:${dateDf.count()}")

        //4.Replace the data value of NA with 0.
        val replaceNADf = dateDf.na.replace(List("DepTime", "ArrTime", "ActualElapsedTime", "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay"),
            Map("NA" -> "0"))
        //println("4. 显示替换NA为0之后的5条数据：")
        replaceNADf.show(5)
        //println(s"最终数据条数:${replaceNADf.count()}")

        //5.5. output as a csv file
        replaceNADf.repartition(1)
          .write.option("header", "true")
          .csv("D:\\flight_data\\output")
    }

}
