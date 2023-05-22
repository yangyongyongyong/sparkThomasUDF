package org.yy.SparkDataType.struct

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{expr, struct, to_json, udf}

object read2Json {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName(this.getClass.getName)
            .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        val sc = spark.sparkContext
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._

        val df = Seq(
            ("a", 1)
            , (null, 2)
            , ("b", 3)
        ).toDF("c1", "c2")

        val run1 = udf(
            (row: Row) => {
                // 某个字段名在row中的index, 从0开始
                row.fieldIndex("c1")
            }
        )
        val run2 = udf(
            (row: Row) => {
                row.json
            }
        )
        val run3 = udf(
            (row: Row) => {
                // 泛型不写会报错. 获取某列的值
                row.getAs[String]("c1")
            }
        )

        val run4 = udf(
            (row: Row) => {
                // row 新增 kv
                val seq = row.toSeq
                /*
                WrappedArray(a, 1)
                WrappedArray(null, 2)
                WrappedArray(b, 3)
                 */
                println(seq)
                ""
            }
        )

        df.select(
            run1(struct('c1, 'c2))
            , run2(struct('c1, 'c2))
            , run3(struct('c1, 'c2))
//            , run4(struct('c1, 'c2))
        ).show()
        /*
        +---------------------------+---------------------------+---------------------------+
        |UDF(struct(c1, c1, c2, c2))|UDF(struct(c1, c1, c2, c2))|UDF(struct(c1, c1, c2, c2))|
        +---------------------------+---------------------------+---------------------------+
        |                          0|          {"c1":"a","c2":1}|                          a|
        |                          0|         {"c1":null,"c2":2}|                       null|
        |                          0|          {"c1":"b","c2":3}|                          b|
        +---------------------------+---------------------------+---------------------------+
         */

        // key使用df的列名, value使用df的列值
        df.select(to_json(struct('c1,'c2)).as("col1")).show()
        /*
        +-----------------+
        |             col1|
        +-----------------+
        |{"c1":"a","c2":1}|
        |         {"c2":2}|
        |{"c1":"b","c2":3}|
        +-----------------+
         */

        // key使用自定义的列名, value使用df的列值
        df.select(to_json(expr(""" named_struct("name",c1,"age",c2) """)).as("col2")).show()
        /*
        +--------------------+
        |                col2|
        +--------------------+
        |{"name":"a","age":1}|
        |           {"age":2}|
        |{"name":"b","age":3}|
        +--------------------+
         */

        spark.stop()
    }
}
