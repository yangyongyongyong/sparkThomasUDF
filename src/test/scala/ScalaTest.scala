import com.alibaba.fastjson2.{JSON, JSONPath, JSONReader}

object ScalaTest {
    def main(args: Array[String]): Unit = {
        JSON.parse("i am s")
//        val js = """[{"des":"i am 1","id":1},{"des":"i am 2","id":2},{"des":"i am 0","id":0}]"""
//        val m = JSONPath.of("$[?(@.id = 0)].des[0]").extract(JSONReader.of(js)).toString()
//        val jsarr = JSON.parse(m);
//        println(jsarr)
    }

}
