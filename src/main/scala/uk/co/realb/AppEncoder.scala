package uk.co.realb

import java.util.Properties

import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._
import org.apache.orc.TypeDescription
import uk.co.realb.flink.orc.OrcWriters

object AppEncoder {

  def main(args: Array[String]): Unit = {
     val (in, out) = try {
       (
         ParameterTool.fromArgs(args).getRequired("in"),
         ParameterTool.fromArgs(args).getRequired("out")
       )
    } catch {
      case e: Exception =>
        System.err.println("No paths specified. Please run 'App --in <path> --out <path>'")
        return
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val checkpoints = env.getCheckpointConfig
    checkpoints.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    checkpoints.setCheckpointInterval(60000)

    val format = new TextInputFormat(new Path(in))
    format.setFilesFilter(FilePathFilter.createDefaultFilter)
    format.setCharsetName("UTF-8")

    val stream = env
      .readFile(format, in, FileProcessingMode.PROCESS_CONTINUOUSLY, 60)
      .map(r => r.split(","))
      .rebalance
      .map(a => (a.lift(0).getOrElse(""), a.lift(1).getOrElse(""), a.lift(2).getOrElse("")))

    val config = new Properties()
    config.setProperty("orc.compress", "SNAPPY")

    val schemaString = """struct<dispatching_base_num:string,pickup_datetime:string,dropoff_datetime:string>""".stripMargin

    val schema = TypeDescription.fromString(schemaString)

    stream
      .addSink(StreamingFileSink
        .forBulkFormat(
          new Path(out),
          OrcWriters
            .withCustomEncoder[(String, String, String)](new Encoder, schema, config)
        )
        .withBucketAssigner(new BucketAssigner[(String, String, String), String] {
          override def getBucketId(element: (String, String, String), context: BucketAssigner.Context): String = "part"
          override def getSerializer: SimpleVersionedSerializer[String] = SimpleVersionedStringSerializer.INSTANCE
        })
        .build())

    env.execute("Encoded ORC writer")
  }
}
