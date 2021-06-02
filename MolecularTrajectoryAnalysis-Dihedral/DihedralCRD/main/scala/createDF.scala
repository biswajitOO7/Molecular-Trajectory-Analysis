import MainCall._
import java.io.BufferedOutputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import ucar.ma2.ArrayFloat
import ucar.nc2.NetcdfFile
import org.apache.spark.sql.functions._


object createDF {

  val fs = FileSystem.get(sc.hadoopConfiguration)

  def getCoordDF (coordFileNameRef1 : String): DataFrame = {

    import sqlContext.implicits._

    val coordFileNameRef = sc.textFile(coordFileNameRef1)
    val coordFileNameArray = coordFileNameRef.map(x => x.mkString("")).collect()

    var fileNumber : Int = 0
    var xCoord : Float = 0.0f
    var yCoord : Float = 0.0f
    var zCoord : Float = 0.0f
    var frameNumber : Int = 0
    var atomIt: Int = 0
    var i = 0
    val coordArray = new Array[String](NATOM * coordFileNameArray.size * 10)

    while (fileNumber < coordFileNameArray.size) {
      val coordFileName: String = coordFileNameArray(fileNumber)
      val coordFile: NetcdfFile = NetcdfFile.open(coordFileName)
      var coordData : ArrayFloat.D3 = coordFile.findVariable("coordinates").read().asInstanceOf[ArrayFloat.D3]

      frameNumber = 0
      while (frameNumber < 10) {
        atomIt = 0
        while (atomIt < NATOM) {

          xCoord = coordData.get(frameNumber, atomIt, 0)
          yCoord = coordData.get(frameNumber, atomIt, 1)
          zCoord = coordData.get(frameNumber, atomIt, 2)

          var frame = frameNumber + 1 + (fileNumber * 10)
          var atomNo = atomIt + 1
          coordArray(i) = f"$frame $atomNo $xCoord%8.3f $yCoord%8.3f $zCoord%8.3f\n"

          i += 1
          atomIt += 1
        }
        frameNumber += 1
      }
      coordFile.close()
      fileNumber += 1
    }

    var headerCoord =  Array[String]("FrameNum", "AtomNum", "XCoord", "YCoord", "ZCoord")
    var coordSchema = StructType(headerCoord.map(col => StructField(col, StringType, true )))

    var coordRDD = sc.parallelize(coordArray).map(_.split("\\s+")).map(x => Row(x(0), x(1), x(2), x(3), x(4)))
    var coordDF = sqlContext.createDataFrame(coordRDD, coordSchema)

    coordDF =  coordDF
      .withColumn("FrameNum", 'FrameNum.cast("Int"))
      .withColumn("AtomNum", 'AtomNum.cast("Int"))
      .withColumn("XCoord", 'XCoord.cast("Float"))
      .withColumn("YCoord", 'YCoord.cast("Float"))
      .withColumn("ZCoord", 'ZCoord.cast("Float"))

    coordDF.persist()

    return coordDF
  }

  def getTopologyDF (topoList : Array[String]) : DataFrame = {

    import sqlContext.implicits._

    var atomCount: Int = 0
    var resCount: Int = 0
    val ATOM_STRING: String = "ATOM"

    val topoArray = new Array[String](NATOM)

    var atomLine: Int = 0
    var atomIndex: Int = 0
    var atomName: String = ""

    var resNumLine: Int = 0
    var resNumIndex: Int = 0
    var resNum: Long = 0

    var resNameLine: Int = 0
    var resNameIndex: Int = 0
    var resName: String = ""

    var i: Int = 0

    var terSeq: Int = 0

    while (atomCount < NATOM) {
      atomLine = atomCount / 20
      atomIndex = /*atomCount % 20*/ atomCount - (atomCount / 20) * 20
      atomName = topoList(ATOM_NAME + atomLine).toString.substring(4 * atomIndex, 4 * atomIndex + 4)
      atomCount += 1

      var topoString = ""

      if (resCount < 54572) {
        resNum = topoList(RESIDUE_POINTER + resNumLine).toString.grouped(8).toList(resNumIndex).trim.toInt
        if (atomCount == resNum) {
          resCount += 1
          resNumLine = resCount / 10
          resNumIndex = /*resCount % 10*/ resCount - (resCount / 10) * 10
          resName = topoList(RESIDUE_LABEL + resNameLine).toString.substring(4 * resNameIndex, 4 * resNameIndex + 4) //.grouped(4).toList(resNameIndex)
          resNameLine = resCount / 20
          resNameIndex = /*resCount % 20*/ resCount - (resCount / 20) * 20
        }
      }

      topoString = f"$ATOM_STRING  $atomCount%5d $atomName%s $resName%s $resCount%4d \n"
      topoArray(i) = topoString
      i += 1
    }

    var headerTopo = Array[String]("Type","AtomNum","AtomName","ResidueName","ResidueNum")  // ATOM ANum AName ResName ResNum XCoord YCoord ZCoord
    var schema = StructType(headerTopo.map(col => StructField(col, StringType,true )))
    var rd = sc.parallelize(topoArray,10).map(_.split("\\s+")).map(x => Row(x(0), x(1), x(2), x(3), x(4)))
    var topoDF = sqlContext.createDataFrame(rd, schema)

    topoDF =  topoDF.withColumn("AtomNum", 'AtomNum.cast("Int"))
      .withColumn("ResidueNum", 'ResidueNum.cast("Int"))
    topoDF.persist()

    return topoDF
  }

}
