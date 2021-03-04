package ca.mcit.bigdata.hdfs

import org.apache.hadoop.fs.{FSDataOutputStream, Path}

object Hadoop04Writer extends App with HadoopClient {

  val filePath = new Path("/user/bdsf2001/adriest/course3/Hadoop04Writer.txt")
  val tutorialList = List("Hadoop01Init", "Hadoop02CopyFromLocal", "Hadoop03Directories", "Hadoop04Writer")
  val writer: FSDataOutputStream = fs.create(filePath)
  tutorialList
    .map(tutorial => s"$tutorial\n")
    .foreach(tutorial => writer.writeChars(tutorial))
  writer.flush()
  writer.close()
  fs.close()
}
