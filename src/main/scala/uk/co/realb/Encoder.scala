package uk.co.realb

import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, VectorizedRowBatch}
import uk.co.realb.flink.orc.encoder.OrcRowEncoder

class Encoder extends OrcRowEncoder[(String, String, String)]() with Serializable {
  override def encodeAndAdd(
                             datum: (String, String, String),
                             batch: VectorizedRowBatch
                           ): Unit = {
    val row = nextIndex(batch)
    batch
      .cols(0)
      .asInstanceOf[BytesColumnVector]
      .setVal(row, datum._1.getBytes)
    batch
      .cols(1)
      .asInstanceOf[BytesColumnVector]
      .setVal(row, datum._2.getBytes)
    batch
      .cols(2)
      .asInstanceOf[BytesColumnVector]
      .setVal(row, datum._3.getBytes)
    incrementBatchSize(batch)
  }
}
