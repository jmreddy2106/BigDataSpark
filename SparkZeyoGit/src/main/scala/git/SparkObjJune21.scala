package git

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object SparkObjJune21 {
  
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("ClassJune20").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    
    val arrayDF = spark.read.format("json").option("multiLine","true").load("D:/BigData_Packs/MultiArrays.json")
    arrayDF.printSchema()
    
    val studentdf = arrayDF.select(
        
                        col("Students"),
                        col("address.temporary_address"),
                        col("address.Permanent_address"),
                        col("first_name"),
                        col("second_name")
    )
    studentdf.printSchema()
    
    val flattendf = studentdf.withColumn("Students", explode($"Students")).withColumn("components", explode($"Students.user.components"))
    
    flattendf.printSchema()
    
    val finaldf = flattendf.select(
        
                               col("components"),
                               col("Students.user.gender"),
                               col("Students.user.name.title"),
                               col("Students.user.name.first"),
                               col("Students.user.name.last"),
                               col("Students.user.address.temporary_address").alias("Stemp_add"),
                               col("Students.user.address.Permanent_address").alias("Sperm_add"),
                               col("Permanent_address"),
                               col("temporary_address"),
                               col("first_name"),
                               col("second_name")              
                  
    )
    
    finaldf.printSchema()
    
    val complexdf = finaldf.groupBy("Permanent_address","temporary_address","second_name","first_name"
        
                    ).agg(collect_list(                        
                                       // struct(
                                              struct(
                                                     struct(
                                                         col("Stemp_add"),
                                                         col("Sperm_add")
                                                     ).alias("address"),   
                                                    array("components"),
                                                    col("gender"),
                                                    struct(
                                                          col("title"),
                                                          col("first"),
                                                          col("last")
                                                     ).alias("name")                                              
                                              ).alias("user")    
                                        //)
                                      ).alias("Students")
                    )
                    
      complexdf.printSchema()
      
      val finalcomplexdf = complexdf.select(
                                        col("Students"),
                                        struct(
                                            col("temporary_address"),
                                            col("Permanent_address")
                                        ).alias("address"),
                                        col("first_name"),
                                        col("second_name")

                                        
                        )
    finalcomplexdf.printSchema()
    finalcomplexdf.show()
  }
  
}