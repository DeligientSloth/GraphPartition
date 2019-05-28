
` sbt package "&&" spark-submit --class "KernighanLin" --master local[4] .\target\scala-2.12\sparkgraph_2.12-0.1.jar `