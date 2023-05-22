package org.yy.SparkDataType.struct

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{element_at, from_json, lit, to_json, when}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


/**
 * desc: struct增加/更新 kv
 * 注意:
 *      withFiled有新增和更新的功能
 *      put: add or update
 *      支持多层字段的值修改
 */
object putKV2 {
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
              , 'c1.withField("kk1", col = lit("vv1")) //value可以选择join其他表后的列
          )
        df3.show(false)
        /*
        +-----------+----------------+
        |c1         |new_struct      |
        +-----------+----------------+
        |{usa, 1}   |{usa, 1, vv1}   |
        |{china, 14}|{china, 14, vv1}|
        +-----------+----------------+
         */

        //注意:withFiled有新增和更新的功能
        df2
          .withColumn(
              "new_struct"
              , 'c1.withField("peopleNum", col = lit(999)) //value可以选择join其他表后的列
          ).show()
        /*
        +-----------+------------+
        |         c1|  new_struct|
        +-----------+------------+
        |   {usa, 1}|  {usa, 999}|
        |{china, 14}|{china, 999}|
        +-----------+------------+
         */

        df3
          .select(to_json('new_struct).as("json"))
          .show(false)
        /*
        +------------------------------------------------+
        |json                                            |
        +------------------------------------------------+
        |{"country":"usa","peopleNum":"1","kk1":"vv1"}   |
        |{"country":"china","peopleNum":"14","kk1":"vv1"}|
        +------------------------------------------------+
         */

        // 报错 只有struct类型的列调用 withField 才不会报错,其他类型列会导致报错
        // 报错内容: data type mismatch: struct argument should be struct type, got: string
        //        val df = Seq(
        //            ("a",1)
        //            ,("b",2)
        //        ).toDF("c1","c2")
        //          .select('c1.withField("num",lit(999)))
        //        df.printSchema()
        //        df.show()


        // 多层级修改
        val df11 = Seq(
            "{'country':'usa','peopleNum':{'mannum':11,'womannum':22} }"
            , "{'country':'china','peopleNum':{'mannum':44 } }"
        ).toDF()

        val st1 = StructType(
            StructField("country", StringType)
              :: StructField("peopleNum"
                , StructType(
                    StructField("mannum", LongType)
                      :: StructField("womannum", LongType)
                      :: Nil
                )
            )
              :: Nil
        )
        // 构造struct 生成struct
        val df22 = df11.select(from_json('value, st1).as("c1"))
        df22.show()
        /*
        +-------------------+
        |                 c1|
        +-------------------+
        |    {usa, {11, 22}}|
        |{china, {44, null}}|
        +-------------------+
         */
        df22.printSchema()

        df22.select(
            when($"c1.peopleNum.womannum".isNull,$"c1".withField("peopleNum.womannum",lit(99999)))
              .otherwise($"c1").as("field22")
        ).show()
        /*
        +--------------------+
        |             field22|
        +--------------------+
        |     {usa, {11, 22}}|
        |{china, {44, 99999}}|
        +--------------------+
         */

        df22.createOrReplaceTempView("view_1")
        spark.sql("""
          select
            case when c1.peopleNum.womannum is null  then c1 else c1 end as field1
          from view_1
          """)
          .show()
        /*
        +-------------------+
        |             field1|
        +-------------------+
        |    {usa, {11, 22}}|
        |{china, {44, null}}|
        +-------------------+
         */





        spark.stop()
    }

}
