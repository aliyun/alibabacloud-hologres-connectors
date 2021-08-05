package com.alibaba.hologres.spark.sink

import com.alibaba.hologres.client.HoloClient
import org.apache.spark.sql.streaming.OutputMode

/** HoloClientInstance is use for stream writer and outputMode is complete .*/
object HoloClientInstance extends Serializable {
  private var holoOptions: Map[String, String] = _
  private var outputMode: OutputMode = _

  def setHoloOptions(holoOptions: Map[String, String]): Unit = {
    this.holoOptions = holoOptions
  }

  def getHoloOptions: Map[String, String] = {
    holoOptions
  }

  def setOutputMode(outputMode: OutputMode): Unit = {
    this.outputMode = outputMode
  }

  def getOutputMode: OutputMode = {
    outputMode
  }

  lazy val client: HoloClient = new BaseSourceProvider().getOrCreateHoloClient(holoOptions)
}
