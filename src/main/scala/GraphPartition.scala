import org.apache.spark.sql.SparkSession
import util.Graph
import partition.KernighanLin

object GraphPartition {

    def main(args: Array[String]): Unit = {

        // Initial Spark session.
        val sparkSession = SparkSession
        .builder()
        .appName("Graph Partition")
        .master("local")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()


        val df = sparkSession.read.csv(args(0)).persist()

        val df_rdd = df.rdd
        var edgeRDD = df_rdd.map(x=>(x(0), x(1), 1.0))

        edgeRDD = edgeRDD.union(edgeRDD.map(x=>(x._2, x._1, 1.0))).persist()

        var graph = new Graph(edgeRDD)//构建图
        //234 12 1234 34
        KernighanLin.partition(graph, 234)//运行算法
        val performance_KL = graph.graphPartitionEvaluation//评价图分割结果
        printf("图分割的performance为：%f", performance_KL)

    }

}