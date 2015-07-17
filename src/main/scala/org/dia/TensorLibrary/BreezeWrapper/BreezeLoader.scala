package org.dia.TensorLibrary.BreezeWrapper

import breeze.linalg.DenseMatrix
import org.dia.TRMMUtils.Constants._
import org.dia.TRMMUtils.NetCDFUtils
import org.slf4j.Logger
import ucar.nc2.dataset.NetcdfDataset

import scala.collection.mutable

/**
 * Created by rahulpalamuttam on 7/17/15.
 */
object BreezeLoader {
  val LOG : Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  /**
   * Creates a 2D array from a list of dimensions using a variable
   * @param dimensionSizes hashmap of (dimension, size) pairs
   * @param netcdfFile the NetcdfDataset to read
   * @param variable the variable array to extract
   * @return DenseMatrix
   */
  implicit def create2dArray (dimensionSizes: mutable.HashMap[Int, Int], netcdfFile: NetcdfDataset, variable: String): BreezeLib = {
    //TODO make sure that the dimensions are always in the order we want them to be
    try {
      val x = dimensionSizes.get(1).get
      val y = dimensionSizes.get(2).get
      val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
      new BreezeLib(new DenseMatrix[Double](x, y, coordinateArray))
    } catch {
      case e :
        java.util.NoSuchElementException => LOG.error("Required dimensions not found. Found:%s".format(dimensionSizes.toString()))
        null
    }
  }


  /**
   * Breeze implementation for loading TRMM data
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  implicit def loadNetCDFTRMMVars (url : String, variable : String) : DenseMatrix[Double] = {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)

    val rowDim = NetCDFUtils.getDimensionSize(netcdfFile, X_AXIS_NAMES(0))
    val columnDim = NetCDFUtils.getDimensionSize(netcdfFile, Y_AXIS_NAMES(0))

    val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
    new DenseMatrix[Double](rowDim, columnDim, coordinateArray, 0)
  }

  /**
   * Gets an NDimensional array of Breeze's DenseMatrices from a NetCDF file
   * TODO :: How do we return nested DenseMatrices - given the function return type has to match T
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   *
   */
  implicit def loadNetCDFNDVars (url : String, variable : String) : BreezeLib = {
    //    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)
    //    val SearchVariable: ma2.Array = NetCDFUtils.getNetCDFVariableArray(netcdfFile, variable)
    //    val ArrayClass = Array.ofDim[Float](240, 1, 201 ,194)
    //    val NDArray = SearchVariable.copyToNDJavaArray().asInstanceOf[ArrayClass.type]
    //     we can only do this because the height dimension is 1
    //    val j = NDArray(0)(0).flatMap(f => f)
    //    val any = NDArray.map(p => new DenseMatrix[Double](201, 194, p(0).flatMap(f => f).map(d => d.toDouble), 0))
    //    denseMatrix = any
    null.asInstanceOf[BreezeLib]
  }
}
