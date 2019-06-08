//import org.apache.spark.sql.SparkSession
//import org.apache.spark.rdd.RDD
//
//
//
//class GraphPartMetis{
//  val file = ""
//
//  /*
//   * 实现对大图的粗化
//   * 输入是大图RDD，采用pairRDD储存图的连接信息,(("nodei","nodej"),weight)表示nodei与nodej的连接权重为weight
//   * 不断的调用_biCoarsen进行二分粗化，直到粗化后的图的规模小于thresh为止
//   * 输出为粗化的RDD
//   */
//  def coarsen(linksRDD: RDD[((String,String),Int)],thresh : Int):RDD[((String,String),Int)]={
//
//    var tempRDD :RDD[((String,String),Int)]= linksRDD
//    var count :Long =0
//    while(count < thresh){
//      tempRDD = _biCoarsen(tempRDD)
//      count = tempRDD.count()
//    }
//    return tempRDD
//  }
//
//  /*
//   * 实现二分粗化功能，选择2个节点i和j，其邻接节点集合为N(i)和N(j)，i和j合并为新节点k，调整k与N(i)，N(j)的连接关系，不断
//   * 执行上述操作。
//   * 输入是RDD，采用pairRDD储存图的连接信息,(("nodei","nodej"),weight)表示nodei与nodej的连接权重为weight
//   * 输出是粗化后的图RDD
//   */
//  def _biCoarsen(linksRDD: RDD[((String,String),Int)]):RDD[((String,String),Int)]={
//    //分析图的连接关系
//    val adjNodesRDD = linksRDD.map{case(k,v) => (k._1,k._2)}.groupByKey()
//    val nodes = adjNodesRDD.keys.collect()
//    val adjNodes = adjNodesRDD.values.collect()
//    val nbnodes = nodes.size
//
//    //按照节点的度排序
//    //val counts = linksRDD.mapValues(v => v.size)
//
//    /*粗化的算法：
//     * 0.对每个节点初始化标识符为true
//     * 1.针对标识符为true的节点，选择度最大的节点i，选择其邻接节点中度最大的j
//     * 2.合并i与j为k，其邻接节点集合为N(i)和N(j)，设置i的标识符为false，j的标识符为false，设置N(i)和N(j)中的节点的标识符为false
//     * 3.调整k与V(i)和V(j)连接权重
//     * 4.返回步骤1循环执行，直到不能找到合法的i与j为止
//     */
//    //随机选择i，j节点，合并后新节点为k，计算并记录(i -> k) 和 (j -> k)的关系到mergehash表
//    var mergehash:Map[String,String] = Map()
//    var name2idx : Map[String,Int] = Map()
//    var flags:Array[Boolean] = new Array[Boolean](nbnodes)
//    var idx :Int = 0;var jdx :Int =0;
//    idx=0;while(idx < nbnodes){name2idx += (nodes(idx) -> idx);idx=idx+1;}
//    idx=0;while(idx < nbnodes){flags(idx) = true;idx=idx+1;}
//    var iNode:String =""
//    var jNode:String = ""
//    var _n :String =""
//    var ijNode:String = ""
//    var tag: Boolean = true
//    idx = 0;jdx = 0;
//    while(idx < nbnodes){
//      if(flags(idx)){
//        iNode = nodes(idx)
//        val iter = adjNodes(idx).iterator
//        tag = true
//        while(iter.hasNext && tag){
//          jNode = iter.next
//          jdx = name2idx(jNode)
//          if(flags(jdx))
//              tag = false
//        }
//        if(flags(jdx)){
//          mergehash += (iNode-> ijNode)
//          mergehash += (jNode-> ijNode)
//          flags(jdx) = false
//
//          val iterj = adjNodes(jdx).iterator
//          while(iterj.hasNext){
//            _n = iterj.next
//            flags(name2idx(_n)) = false
//        }
//        flags(idx) = false
//        val iteri = adjNodes(idx).iterator
//        while(iteri.hasNext){
//            _n = iteri.next
//            flags(name2idx(_n)) = false
//        }
//        idx = idx + 1
//      }else{
//        idx = idx + 1
//      }
//    }
//
//    //根据mergehash表, 更新连接关系和权重，得到粗化后的图 tempRDD
//    var tempRDD :RDD[((String,String),Int)]= linksRDD
//    tempRDD = tempRDD.map{case(k,v) =>
//      ((mergehash(k._1),mergehash(k._2)),v)
//    }
//    tempRDD = tempRDD.map{case(k,v) =>
//      if(k._1==k._2)
//        (k,0)
//      else
//        (k,v)
//    }
//    tempRDD = tempRDD.reduceByKey((x,y)=>x+y)
//    return tempRDD
//  }
//
//  def partitonKernighanLin(){
//
//  }
//
//  def refine(){
//
//  }
//}