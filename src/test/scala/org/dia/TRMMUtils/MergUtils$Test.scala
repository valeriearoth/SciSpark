package org.dia.TRMMUtils

import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.NDArrayIndex
import org.scalatest.FunSuite
import org.nd4j.api.Implicits._
/**
 * Created by rahulsp on 7/13/15.
 */
class MergUtils$Test extends FunSuite {
  val fileName = "merg_2015070623_4km-pixel"
  val shape = Array(2, 9896, 3298)
  val offset = 75.0
  test("testReadMergtoJavaArray") {

  }

  test("testReadMergtoINDArray") {
    val indArray = MergUtils.ReadMergtoINDArray(fileName, shape, offset)
    val shape1 = indArray.tensorAlongDimension(1, 1, 2)
    val shape2 = indArray.slice(1, 0)
    val multi1 = Nd4j.create((1f to 8f by 1).toArray, Array(2,2,2))
    val multi = (1f to 8f by 1).asNDArray(2,2,2)
    println(multi)
    //println(multi(0, ->, ->))
    assert(shape1 == shape2)
    //assert(shape1 == shape3)
  }

  test("testReadMergtoNetCDFDataset") {

  }

}
