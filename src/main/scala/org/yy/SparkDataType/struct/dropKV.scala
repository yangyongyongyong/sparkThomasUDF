package org.yy.SparkDataType.struct

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, lit, to_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * desc: struct删除kv
 */
object dropKV {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName(this.getClass.getName)
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()
        val sc = spark.sparkContext
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._

        val df1 = Seq(
            "{'country':'usa','peopleNum':'1'}"
            , "{'country':'china','peopleNum':'14'}"
        ).toDF()

        val st = StructType(
            StructField("country", StringType)
              :: StructField("peopleNum", StringType)
              :: Nil
        )
        // 构造struct 生成struct
        val df2 = df1.select(from_json('value, st).as("c1"))
        df2.show()
        /*
        +-----------+
        |         c1|
        +-----------+
        |   {usa, 1}|
        |{china, 14}|
        +-----------+
         */
        df2.printSchema()
        /*
        root
         |-- c1: struct (nullable = true)
         |    |-- country: string (nullable = true)
         |    |-- peopleNum: string (nullable = true)
         */

        //通过withField来增加struct的kv
        val df3 = df2
          .withColumn(
              "new_struct"
              ,'c1.dropFields("peopleNum")
          )
          df3.show(false)
        /*
        +-----------+----------+
        |c1         |new_struct|
        +-----------+----------+
        |{usa, 1}   |{usa}     |
        |{china, 14}|{china}   |
        +-----------+----------+
         */

        df3.select(to_json('new_struct)).show(false)
        /*
        +-------------------+
        |to_json(new_struct)|
        +-------------------+
        |{"country":"usa"}  |
        |{"country":"china"}|
        +-------------------+
         */

        spark.stop()
    }

}
