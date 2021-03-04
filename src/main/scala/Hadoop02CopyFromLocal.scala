package ca.mcit.bigdata.hdfs

import org.apache.hadoop.fs.Path

object Hadoop02CopyFromLocal extends App with HadoopClient {

  fs.copyFromLocalFile(new Path("datosSTM/calendar_dates.csv"), new Path("/user/bdsf2001/adriest/stm"))
  fs.copyFromLocalFile(new Path("datosSTM/routes.csv"), new Path("/user/bdsf2001/adriest/stm"))
  fs.copyFromLocalFile(new Path("datosSTM/trips.csv"), new Path("/user/bdsf2001/adriest/stm"))
  fs.close()
}
