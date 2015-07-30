/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dia

import org.dia.Constants._
import org.dia.core.{SciSparkContext, sciTensor}
import org.dia.loaders.NetCDFLoader
import org.dia.tensors.BreezeTensor

import scala.language.implicitConversions

/**
  */
object Main {

  /**
   * NetCDF variables to use
   * TODO:: Make the netcdf variables global - however this may need broadcasting
   */
  val rowDim = 180
  val columnDim = 360
  val TextFile = "TestLinks"


  def main(args: Array[String]): Unit = {
    val variables = NetCDFLoader.loadNetCDFVariables(args(0))
    println(variables)

    val loadStart = System.currentTimeMillis() / 1000.0
    val tuples = NetCDFLoader.loadNetCDFNDVars(args(0), "tasmax")
    val breezeArrays = new BreezeTensor((tuples._1, tuples._2))
    val loadEnd = System.currentTimeMillis() / 1000.0
    println(breezeArrays.tensor.rows + " " + breezeArrays.tensor.cols)
    println("Loaded all variables")
    println((loadEnd - loadStart))
    println(breezeArrays)
    //val breezeTense = new BreezeTensor(arrayTuple._1, arrayTuple._2)
  }
}


