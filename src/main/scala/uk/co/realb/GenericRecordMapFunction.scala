package uk.co.realb

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.flink.api.common.functions.RichMapFunction

class GenericRecordMapFunction(schema: String) extends RichMapFunction[Array[String], GenericRecord]{
  @transient lazy val schemaAvro = new Schema.Parser().parse(schema)
  override def map(value: Array[String]): GenericRecord = {
    val r: GenericRecord = new GenericData.Record(schemaAvro)
    r.put("dispatching_base_num", value.lift(0).getOrElse(""))
    r.put("pickup_datetime", value.lift(1).getOrElse(""))
    r.put("dropoff_datetime", value.lift(2).getOrElse(""))
    r
  }
}
