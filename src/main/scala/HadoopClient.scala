package ca.mcit.bigdata.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait HadoopClient {

  val conf = new Configuration()
  val hadoopConfDir = "/Users/Valeria/opt/hadoop-2.7.3/etc/cloudera"
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))
  val fs: FileSystem = FileSystem.get(conf)
}
