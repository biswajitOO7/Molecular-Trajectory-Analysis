import java.io.BufferedOutputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import ucar.ma2.ArrayFloat
import ucar.ma2.ArrayObject.D3
import ucar.nc2.NetcdfFile
import ucar.nc2.NetcdfFile.open

object MainCall {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("SuperGroup3")
  val sc = new SparkContext(conf)
  val sqlContext = SparkSession.builder().getOrCreate()
  import sqlContext.implicits._

  var POINTERS : Int = 0
  var ATOM_NAME : Int = 0
  var RESIDUE_LABEL : Int = 0
  var RESIDUE_POINTER : Int = 0
  var SOLVENT_POINTERS : Int = 0
  var BOX_DIMENSIONS : Int = 0
  var ATOMS_PER_MOLECULE : Int = 0
  var NATOM : Int = 0
  var NSPM : Int = 0
  var topoList : Array[String] = _

  val fs = FileSystem.get(sc.hadoopConfiguration)

  def main(args: Array[String]): Unit = {

//    val topologyFileString : String = args(0)
    val topoFile = sc.textFile(args(0))
    val topoList = topoFile.map(x => x.mkString("")).collect()

    POINTERS = topoList.indexOf("%FLAG POINTERS".padTo(80, " ").mkString) + 2
    ATOM_NAME = topoList.indexOf("%FLAG ATOM_NAME".padTo(80, " ").mkString) + 2
    RESIDUE_LABEL = topoList.indexOf("%FLAG RESIDUE_LABEL".padTo(80, " ").mkString) + 2
    RESIDUE_POINTER = topoList.indexOf("%FLAG RESIDUE_POINTER".padTo(80, " ").mkString) + 2
    SOLVENT_POINTERS = topoList.indexOf("%FLAG SOLVENT_POINTERS".padTo(80, " ").mkString) + 2
    BOX_DIMENSIONS = topoList.indexOf("%FLAG BOX_DIMENSIONS".padTo(80, " ").mkString) + 2
    ATOMS_PER_MOLECULE = topoList.indexOf("%FLAG ATOMS_PER_MOLECULE".padTo(80, " ").mkString) + 2

    NATOM = topoList(POINTERS).toString.substring(0, 8).trim.toInt
    NSPM = topoList(SOLVENT_POINTERS).toString.substring(9, 16).trim.toInt

    val topologyDF : DataFrame = createDF.getTopologyDF(topoList)
    var coordDF : DataFrame = createDF.getCoordDF(args(1))

    val t1 = System.nanoTime

    var dihedral = Dihedral.getBackboneDihedralAngle(topologyDF, coordDF)
    WriteDihedralAngles.writeDihedralAngle(dihedral)

    var os1 = new BufferedOutputStream( fs.create( (new Path("out_file_CRDDihedral_lines") )))
    os1.write(dihedral.size.toString.getBytes("UTF-8"))
    os1.close()

    val duration = (System.nanoTime - t1) / 1e9d
    var os = new BufferedOutputStream( fs.create( (new Path("out_file_CRDDihedral_time") )))
    os.write(duration.toString.getBytes("UTF-8"))
    os.close()
  }
}