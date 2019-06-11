package util

class Node(__idx: String,
           __neighbour: Map[String,Double]) extends Serializable {

    private val idx: String = __idx//node的id
    private var neighbour: Map[String,Double] = __neighbour//node的邻居，表示node指向的节点
    private var E: Double = 0.0//node的外部权重，跟不在一个子图的node的连接权重之和
    private var I: Double = 0.0//node的内部权重，跟在一个子图的node的连接权重之和
    private var partition: Int = 0 //node所在子图的id，从0开始
    private var chosen: Boolean = false//这个node是否被交换过
    private var composition: List[Node] = List()//node由原来哪些node组成
    private var composLevel = 0 //each node's compos Level
    private var isMark:Boolean = false//node是否是匹配点
    private var weight:Double = 1.0//node的权重

    def this(idx: String, neighbour: Map[String,Double],
             E: Double, I: Double = 0,
             partition: Int = 0,
             chosen: Boolean = false,
             isMark:Boolean,
             weight:Double) {
        this(idx, neighbour) //主构造函数
        this.E = E
        this.I = I
        this.partition = partition
        this.chosen = chosen
        this.isMark=isMark
        this.weight=weight
    }

    def setE(E: Double): Node = {
        this.E = E
        this
    }

    def setI(I: Double): Node = {
        this.I = I
        this
    }

    def setNeighbour(neighbour:Map[String,Double]): Node = {
        this.neighbour = neighbour
        this
    }

    def popNeighbour(popNode: Node): Node = {
        this.neighbour-=popNode.getIdx
        this
    }

    def pushNeighbour(neighbourNode:(String,Double)): Node = {
        this.neighbour+=neighbourNode
        this
    }

    def setChosen(chosen: Boolean): Node = {
        this.chosen = chosen
        this
    }

    def setPartition(partition: Int): Node = {
        this.partition = partition
        this
    }

    def setCompositionPartition(): Node = {
        this.composition.map(_.setPartition(this.partition))
        this
    }

    def setIsMark(isMark: Boolean): Node = {
        this.isMark = isMark
        this
    }

    def setComposition(composition:List[Node],composLevel:Int): Node = {
        this.composition = composition
        this.composLevel = composLevel
        this
    }

    def setWeight(weight:Double): Node = {
        this.weight = weight
        this
    }

    def getIdx: String = this.idx

    def getNeighbour: Map[String,Double] = this.neighbour

    def getE: Double = this.E

    def getI: Double = this.I

    def getChosen: Boolean = this.chosen

    def getPartition: Int = this.partition

    def getIsMark: Boolean = this.isMark

    def getComposition: List[Node] = this.composition

    def getWeight: Double = this.weight

    def getComposLevel:Int = this.composLevel

    def isNeighbour(otherNode:Node):Boolean= {
        this.neighbour.contains(otherNode.getIdx)
    }

    def edgeWeight(otherNode: Node): Double = {
        if (!isNeighbour(otherNode)) return 0.0
        this.neighbour(otherNode.getIdx)
    }
    def edgeWeight(otherNodeIdx: String): Double = {
        if(!this.neighbour.contains(otherNodeIdx)) return 0.0
        this.neighbour(otherNodeIdx)
    }

    def swapGain(otherNode: Node): Double =
        this.getE - this.getI + otherNode.getE -
                otherNode.getI - 2 * this.edgeWeight(otherNode)

    def Print(): Unit = {
        println("=================================")
        this.neighbour.foreach(x=>print(x+" "))
        println()
        this.composition.foreach(x=>print(x.getIdx+"-"+x.getPartition+" "))
        println()
        println(this.idx + " E=" + this.E + " I=" + this.I
                + " partition=" + this.partition + " is chosen=" + this.chosen+
                " is mark=" + this.isMark+" weight"+this.weight)
        println("=================================")
    }

    def swapUpdate(swap_node_a: Node,
                   swap_node_b: Node): Node = {
        if (swap_node_a.getPartition == swap_node_b.getPartition) {
            println("输入有错误")
            return this
        }

        val is_node_a = this.getIdx == swap_node_a.getIdx
        val is_node_b = this.getIdx == swap_node_b.getIdx
        val in_a_graph = this.getPartition == swap_node_a.getPartition

        val E_a = swap_node_a.getE
        val I_a = swap_node_a.getI
        val E_b = swap_node_b.getE
        val I_b = swap_node_b.getI

        val weight_ab = swap_node_a.edgeWeight(swap_node_b)

        if (is_node_a)
            return this.setE(I_a + weight_ab).setI(E_a - weight_ab).
                    setChosen(true).setPartition(swap_node_b.getPartition)
        if (is_node_b)
            return this.setE(I_b + weight_ab).setI(E_b - weight_ab).
                    setChosen(true).setPartition(swap_node_a.getPartition)

        val weight_a = this.edgeWeight(swap_node_a)
        val weight_b = this.edgeWeight(swap_node_b)

        if (weight_a == 0.0 && weight_b == 0.0) return this

        if (in_a_graph)
            this.setI(
                this.getI - weight_a + weight_b
            ).setE(
                //这些点的E增加了与a连接的权重
                this.getE + weight_a - weight_b
            )
        else
            this.setI(
                this.getI + weight_a - weight_b
            ).setE(
                //这些点的E增加了与a连接的权重
                this.getE - weight_a + weight_b
            )
    }
}
