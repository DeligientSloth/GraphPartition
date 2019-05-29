# -*- coding: utf-8 -*-

import os

r"""
sbt package "&&" \
spark-submit \
    --class "GraphPartition" \
    --master local[4] \
    .\target\scala-2.12\sparkgraph_2.12-0.1.jar \
    .\test\network.csv
"""

sbt_compile_command = "sbt package"

spark_submit_command = "spark-submit --class \"{scala_class}\" \
    --master {master} {jar} {args} ".format(
        scala_class = "GraphPartition",
        master = "local[4]",
        jar = ".\\target\\scala-2.12\\sparkgraph_2.12-0.1.jar",
        args = ".\\data\\network.csv"
    )

os.system(sbt_compile_command + "&&" + spark_submit_command)