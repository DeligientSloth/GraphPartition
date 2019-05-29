import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util.Date

import util.Graph

import partition.HashGraphPartition
import partition.KernighanLin
import partition.SpectralClustering

object GraphPartition {

  def readGraphCSV(filePath:String,isDirected:Boolean):RDD[(String,String,Double)]={
    val sparkSession = SparkSession
      .builder()
      .appName("Graph Partition")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df_rdd = sparkSession.read.csv(filePath).rdd //dataframe to rdd
    val edgeRDD = df_rdd.map(x=>(x(0).toString, x(1).toString, 1.0))

    if(isDirected) edgeRDD
    else edgeRDD.union(edgeRDD.map(x=>(x._2, x._1, 1.0)))

  }
  def main(args: Array[String]): Unit = {


    val edgeRDD = readGraphCSV(args(0),false).persist()
    var graph = new Graph(edgeRDD)//构建图

    val startTime = new Date().getTime
    // seed=324,12324,2324
    //KernighanLin.partition(graph, 324)//运行算法
    graph = SpectralClustering.partition(graph,2,40)
//    graph = HashGraphPartition.partition(graph,2)
    val endTime = new Date().getTime
    println("运行时间="+(endTime-startTime)/1000.0)

    val performance = graph.graphPartitionEvaluation//评价图分割结果
    println("图分割的performance为: " + performance)
    println("划分得到的子图")
    graph.Print()
  }

}