import org.apache.spark.sql.SparkSession
import util.Graph

object GraphPartition {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._
    val df = sparkSession.read.csv("./test/test2.csv")

    val df_rdd = df.rdd
//    val vertex_partition_1  = Array[Any]("1", "2")
//    val vertex_partition_2  = Array[Any]("3", "4")
    val vertex_partition_1  = Array[Any]("1", "2", "3")
    val vertex_partition_2  = Array[Any]("4", "5", "6")
    val init_vertex_partition = List[Array[Any]](vertex_partition_1, vertex_partition_2)

    var edgeRDD = df_rdd.map(x=>(x(0), x(1), 1.0))
    edgeRDD = edgeRDD.union(edgeRDD.map(x=>(x._2, x._1, 1.0)))


    val graph = new Graph(edgeRDD)//构建图
//    graph.KernighanLin(1234)//运行算法
    graph.KernighanLin(init_vertex_partition)
    graph.Print()//打印图
    val performance_KL = graph.graphPartitionEvaluation//评价图分割结果
    printf("图分割的performance为：%f", performance_KL)

  }

}