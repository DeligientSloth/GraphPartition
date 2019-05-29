package util

class Node(__idx:Any,
           __neighbour:List[(Any, Double)]) extends Serializable {

    private val idx:Any=__idx
    private val neighbour:List[(Any, Double)]=__neighbour
    private var E:Double=0.0
    private var I:Double=0.0
    private var partition:Int=0//表示这个点在第几个图里面，从0开始
    private var chosen:Boolean=false

    def this(idx:Any, neighbour:List[(Any,Double)],
            E:Double, I:Double=0,
            partition:Int=0,
            chosen:Boolean=false){
        this(idx, neighbour)//主构造函数
        this.E=E
        this.I=I
        this.partition=partition
        this.chosen=chosen
    }
    def setE(E:Double):Node={
        this.E = E
        this
    }
    def setI(I:Double):Node={
        this.I = I
        this
    }
    def setChosen(chosen:Boolean):Node={
        this.chosen = chosen
        this
    }
    def setPartition(partition:Int):Node={
        this.partition = partition
        this
    }

    def getIdx:Any=this.idx
    def getNeighbour:List[(Any,Double)]=this.neighbour
    def getE:Double=this.E
    def getI:Double= this.I
    def getChosen:Boolean= this.chosen
    def getPartition:Int= this.partition


    def weight(otherNode:Node): Double ={
        val weight = this.getNeighbour.filter(x=>x._1==otherNode.getIdx)
        if(weight.isEmpty) return 0.0
        weight.head._2
    }

    def swapGain(otherNode:Node):Double=
        this.getE-this.getI+otherNode.getE-
        otherNode.getI-2*this.weight(otherNode)

    def Print(): Unit ={
        println(this.idx+" E="+this.E+" I="+this.I
        +" partition="+this.partition+" is chosen="+this.chosen)
    }

    def swapUpdate(swap_node_a:Node,
                    swap_node_b:Node):Node={
        if(swap_node_a.getPartition==swap_node_b.getPartition){
            println("输入有错误")
            return this
        }

        val is_node_a = this.getIdx==swap_node_a.getIdx
        val is_node_b = this.getIdx==swap_node_b.getIdx
        val in_a_graph = this.getPartition==swap_node_a.getPartition

        val E_a = swap_node_a.getE
        val I_a = swap_node_a.getI
        val E_b = swap_node_b.getE
        val I_b = swap_node_b.getI

        val weight_ab = swap_node_a.weight(swap_node_b)
        def inversePartition(node:Node):Int =
        if(node.getPartition==1) 0 else 1

        if(is_node_a)
            return this.setE(I_a+weight_ab).setI(E_a-weight_ab).
                setChosen(true).setPartition(inversePartition(this))
        if(is_node_b)
            return this.setE(I_b+weight_ab).setI(E_b-weight_ab).
                setChosen(true).setPartition(inversePartition(this))

        val weight_a = this.weight(swap_node_a)
        val weight_b = this.weight(swap_node_b)

        if(weight_a==0.0&&weight_b==0.0) return this

        if(in_a_graph)
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
