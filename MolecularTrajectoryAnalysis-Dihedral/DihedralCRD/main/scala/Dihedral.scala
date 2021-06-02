import breeze.numerics._
import breeze.linalg._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import java.io.{File, PrintWriter}

import MainCall._

import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal

object Dihedral {

  //MaskData.maskNETCDF(pdbDF, @N,CA,C)
  private def calculateAngle (atom1: Array[Double], atom2: Array[Double], atom3: Array[Double], atom4: Array[Double]) : Double = {

    //Praxeolitic Formula
    val vector0 = new DenseVector(atom1)
    val vector1 = new DenseVector(atom2)
    val vector2 = new DenseVector(atom3)
    val vector3 = new DenseVector(atom4)

    val b0 = vector0 - vector1
    val b1 = vector2 - vector1
    val b2 = vector3 - vector2

    b1 /= norm(b1)

    val scaVal = (b0.dot(b1)) * b1
    val scaVal2 = (b2.dot(b1)) * b1

    val v = b0 - scaVal
    val w = b2 - scaVal2

    val x = v.dot(w)

    // Cross Product of b1 and v Vector
    val i = b1(1) * v(2) - v(1) * b1(2)
    val j = v(0) * b1(2) - b1(0) * v(2)
    val k = b1(0) * v(1) - v(0) * b1(1)

    val crossVal = new DenseVector(Array(i , j , k))

    val y = crossVal.dot(w)
    val dihedralAngle = toDegrees(atan2(y,x))
    val dihedralAngleFormatted = BigDecimal(dihedralAngle).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    dihedralAngleFormatted
  }


  def getBackboneDihedralAngle(topoDF: DataFrame, coordDF: DataFrame): ArrayBuffer[String] = {

    val someDF = MaskData.maskNETCDF(topoDF, coordDF, "@N,CA,C")


    val dihedralSchema = List(
      StructField("FrameNum", IntegerType, true),

      StructField("AtomName", StringType, true),
      StructField("ResidueNum", IntegerType, true),
      StructField("ResidueName", StringType, true),

      StructField("AtomName", StringType, true),
      StructField("ResidueNum", IntegerType, true),
      StructField("ResidueName", StringType, true),

      StructField("AtomName", StringType, true),
      StructField("ResidueNum", IntegerType, true),
      StructField("ResidueName", StringType, true),

      StructField("AtomName", StringType, true),
      StructField("ResidueNum", IntegerType, true),
      StructField("ResidueName", StringType, true),

      StructField("DihedralAngle", DoubleType, true),
      StructField("AngleType", StringType, true)
    )

    val list = someDF.collectAsList()
    val listSize = list.size()

    if (listSize < 6) {
      return ArrayBuffer[String]()
    }

    var dihedralArray = new ArrayBuffer[String]()

    var dihedralPhiString = " "
    var dihedralPsiString = " "
    var dihedralOmegaString = " "

    var NCordRes1 = new Array[Double](3)
    var CACordRes1 = new Array[Double](3)
    var CCordRes1 = new Array[Double](3)
    var NCordRes2 = new Array[Double](3)
    var CACordRes2 = new Array[Double](3)
    var CCordRes2 = new Array[Double](3)
    var atom1 = list.get(0)
    var atom2 = list.get(1)
    var atom3 = list.get(2)
    var atom4 = list.get(3)
    var atom5 = list.get(4)
    var atom6 = list.get(5)
    var res1 = Array(NCordRes1, CACordRes1, CCordRes1)
    var res2 = Array(NCordRes2, CACordRes2, CCordRes2)
    var phiAngle: Double = 0
    var psiAngle: Double = 0
    var omegaAngle: Double = 0
    var frameNumRes1: Int = 1
    var frameNumRes2: Int = 2
    var resNumRes1: Int = 1
    var resNumRes2: Int = 2
    var resNameRes1: String = ""
    var resNameRes2: String = ""

    for (i <- 0 until listSize - 5 by 3) {
      atom1 = list.get(i + 0)
      atom2 = list.get(i + 1)
      atom3 = list.get(i + 2)
      atom4 = list.get(i + 3)
      atom5 = list.get(i + 4)
      atom6 = list.get(i + 5)
      frameNumRes1 = atom1.get(0).toString.toInt
      frameNumRes2 = atom4.get(0).toString.toInt
      resNumRes1 = atom1.get(5).toString.toInt
      resNumRes2 = atom4.get(5).toString.toInt
      resNameRes1 = atom1.get(4).toString
      resNameRes2 = atom4.get(4).toString

      if (frameNumRes1 == frameNumRes2) {
        NCordRes1 = Array(atom1.get(6).toString.toDouble, atom1.get(7).toString.toDouble, atom1.get(8).toString.toDouble)
        CACordRes1 = Array(atom2.get(6).toString.toDouble, atom2.get(7).toString.toDouble, atom2.get(8).toString.toDouble)
        CCordRes1 = Array(atom3.get(6).toString.toDouble, atom3.get(7).toString.toDouble, atom3.get(8).toString.toDouble)
        NCordRes2 = Array(atom4.get(6).toString.toDouble, atom4.get(7).toString.toDouble, atom4.get(8).toString.toDouble)
        CACordRes2 = Array(atom5.get(6).toString.toDouble, atom5.get(7).toString.toDouble, atom5.get(8).toString.toDouble)
        CCordRes2 = Array(atom6.get(6).toString.toDouble, atom6.get(7).toString.toDouble, atom6.get(8).toString.toDouble)

        res1 = Array(NCordRes1, CACordRes1, CCordRes1)
        res2 = Array(NCordRes2, CACordRes2, CCordRes2)

        phiAngle = getPhiAngle(res1 = res1, res2 = res2)
        psiAngle = getPsiAngle(res1 = res1, res2 = res2)
        omegaAngle = getOmegaAngle(res1 = res1, res2 = res2)

        dihedralPhiString = f"$frameNumRes1%5d C  $resNumRes1%5d $resNameRes1%4s  N $resNumRes2%5d $resNameRes2%4s CA$resNumRes2%5d $resNameRes2%4s C  $resNumRes2%5d $resNameRes2%4s $phiAngle PHI\n"
        dihedralPsiString = f"$frameNumRes1%5d N  $resNumRes1%5d $resNameRes1%4s  CA$resNumRes1%5d $resNameRes1%4s C  $resNumRes1%5d $resNameRes1%4s N  $resNumRes2%5d $resNameRes2%4s $psiAngle PSI\n"
        dihedralOmegaString = f"$frameNumRes1%5d CA $resNumRes1%5d $resNameRes1%4s  C $resNumRes1%5d $resNameRes1%4s N  $resNumRes2%5d $resNameRes2%4s CA$resNumRes2%5d $resNameRes2%4s $omegaAngle OMEGA\n"

        dihedralArray += dihedralPhiString
        dihedralArray += dihedralPsiString
        dihedralArray += dihedralOmegaString
      }
    }

    val dihedralRdd = sc.parallelize(dihedralArray).map(_.split("\\s+")).map(x => Row(x(0).toInt, x(1), x(2).toInt, x(3), x(4), x(5).toInt,x(6), x(7), x(8).toInt, x(9), x(10), x(11).toInt, x(12), x(13).toDouble, x(14)))
    val dihedralDF = sqlContext.createDataFrame(dihedralRdd, StructType(dihedralSchema))

    dihedralArray
  }

  private def getPhiAngle(res1: Array[Array[Double]], res2: Array[Array[Double]]): Double = {
    // C-N-Cα-C // Residue wise - 1, 2, 2, 2
    calculateAngle(atom1 = res1(2), atom2 = res2(0), atom3 = res2(1), atom4 = res2(2))
  }

  private def getPsiAngle(res1: Array[Array[Double]], res2: Array[Array[Double]]): Double = {
    // N-Cα-C-N // Residue wise - 1, 1, 1, 2
    calculateAngle(atom1 = res1(0), atom2 = res1(1), atom3 = res1(2), atom4 = res2(0))
  }

  private def getOmegaAngle(res1: Array[Array[Double]], res2: Array[Array[Double]]): Double = {
    // Cα-C-N-Cα // Residue wise - 1, 1, 2, 2
    calculateAngle(atom1 = res1(1), atom2 = res1(2), atom3 = res2(0), atom4 = res2(1))
  }
}
