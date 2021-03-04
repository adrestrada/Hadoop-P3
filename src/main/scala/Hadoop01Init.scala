package ca.mcit.bigdata.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

object Hadoop01Init extends App {

  val conf = new Configuration()
  val hadoopConfDir = sys.env("HADOOP_CONF_DIR")
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))

  val fs = FileSystem.get(conf)
  val contentOfRoot: Array[FileStatus] = fs.listStatus(new Path("/"))
  contentOfRoot.map(_.getPath).foreach(println)
  fs.close()
}
