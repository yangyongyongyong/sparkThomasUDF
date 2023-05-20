import com.alibaba.fastjson2.{JSON, JSONPath, JSONReader}

object fastjsonT1 {
    def main(args: Array[String]): Unit = {
        val array = JSON.parse(JSONPath.of("$[?(@.id==0)]").extract(JSONReader.of("{'des':'i am 0','id':0}")).toString())
        println(array)
    }

}
