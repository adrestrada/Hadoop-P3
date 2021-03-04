package ca.mcit.bigdata.hdfs

import org.apache.hadoop.fs.Path

object Hadoop03Directories extends App with HadoopClient {
  fs.listStatus(new Path("/user/bdsf2001/adriest/course3")).map(_.getPath).foreach(println)
}
