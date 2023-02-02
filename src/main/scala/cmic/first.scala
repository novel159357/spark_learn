package cmic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object first {
  def main(args: Array[String]): Unit = {
    class Person{
      var age : Int = 18
      val name : String = "Duyongliang"
    }
    var varity = new Person()
    varity.age = 19
    varity = null

    val constant = new Person()
    constant.age = 10


    var age: Int = 18
    //配置运行对象（本地模式）
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits
    val rdd1 = sparkContext.parallelize(List(1,1,2,3,2,4),3)
    val rdd2 = sparkContext.makeRDD(List(1,2,3,4),2)
    val dataRdd1 : RDD[Int] = rdd1.map(
      num => {
        num * 2
      }
    )
    val dataRdd2 : RDD[Int] = dataRdd1.mapPartitions(
      datas => {
        datas.filter(_==2)
      }
    )
    val fileRDD : RDD[String] = sparkContext.textFile("input/word.txt",5)
    val idRDD = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    import spark.implicits._
    idRDD.toDF("id", "name", "age")
    rdd1.collect().foreach(println)
    dataRdd1.collect().foreach(println)
    dataRdd2.collect().foreach(println)
    rdd2.collect().foreach(println)
    fileRDD.collect().foreach(println)

    sparkContext.stop()
  }
}
