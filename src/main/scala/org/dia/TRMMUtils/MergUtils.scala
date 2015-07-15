package org.dia.TRMMUtils

import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.api.linalg.DSL._
import ucar.nc2.dataset.NetcdfDataset
import scala.io.Source
import java.io.File
/**
 * Created by rahulsp on 7/13/15.
 */
object MergUtils {
  
  def ReadMergtoJavaArray(file : String) : Array[Double] = {
    val Sourcefile = Source.fromFile(file, "ISO8859-1")
    val byteArray = Sourcefile.map(_.toInt).toArray
    Sourcefile.close
    val SourceArray = byteArray.map(floatByte => floatByte.asInstanceOf[Float].toDouble)
    SourceArray
  }
  
  def ReadMergtoINDArray(file : String, shape : Array[Int], offset : Double) : INDArray = {
    val java1dArray = ReadMergtoJavaArray(file)
    Nd4j.create(java1dArray, shape) += offset
  }

  def ReadMergtoNetCDFDataset(file : String, shape : Array[Int]) : NetcdfDataset = {
    null
  }
}
