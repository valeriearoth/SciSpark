package org.dia.TensorLibrary.Nd4jWrapper

import org.dia.TRMMUtils.Constants._
import org.dia.TRMMUtils.NetCDFUtils
import org.nd4j.linalg.factory.Nd4j
import org.slf4j.Logger
import ucar.nc2.dataset.NetcdfDataset

import scala.collection.mutable

/**
 * Created by rahulpalamuttam on 7/17/15.
 */
object Nd4jLoader {
  val LOG : Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  // TODO :: Opportunity to refactor loaders
  /**
   * Gets an NDimensional Array of ND4j from a TRMM dataset
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  implicit def loadNetCDFTRMMVars(url: String, variable: String): Nd4jLib = {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)

    val rowDim = NetCDFUtils.getDimensionSize(netcdfFile, X_AXIS_NAMES(0))
    val columnDim = NetCDFUtils.getDimensionSize(netcdfFile, Y_AXIS_NAMES(0))

    val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
    new Nd4jLib(Nd4j.create(coordinateArray, Array(rowDim, columnDim)))
  }

  /**
   * Gets an NDimensional array of INDArray from a NetCDF url
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  implicit def loadNetCDFNDVars(url: String, variable: String): Nd4jLib =  {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)
    loadNetCDFNDVars(netcdfFile, variable)
  }

  /**
   * Gets an NDimensional array of INDArray from a NetCDF file
   * @param netcdfFile where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  implicit def loadNetCDFNDVars(netcdfFile: NetcdfDataset, variable: String): Nd4jLib = {
    val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
    val dims = NetCDFUtils.getDimensionSizes(netcdfFile, variable)
    val shape = dims.toArray.sortBy(_._1).map(_._2)
    new Nd4jLib(Nd4j.create(coordinateArray, shape))
  }

  /**
   * Creates a 2D array from a list of dimensions using a variable
   * @param dimensionSizes hashmap of (dimension, size) pairs
   * @param netcdfFile the NetcdfDataset to read
   * @param variable the variable array to extract
   * @return DenseMatrix
   */
  def create2dArray(dimensionSizes: mutable.HashMap[Int, Int], netcdfFile: NetcdfDataset, variable: String): Nd4jLib = {

    val x = dimensionSizes.get(1).get
    val y = dimensionSizes.get(2).get

    val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)

    new Nd4jLib(Nd4j.create(coordinateArray, Array(x, y)))
  }
}
