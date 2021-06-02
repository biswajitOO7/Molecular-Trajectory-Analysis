import java.io.BufferedOutputStream

import MainCall.NATOM
import createDF.fs
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

object WriteDihedralAngles {

  def writeDihedralAngle (dihedralArray : ArrayBuffer[String]) : Unit = {

//    var pdbdfArray = pdbDF.collect()
//    var newarray2 : Array[String] = new Array[String](NATOM)
//
//    var i = 0
//
//    var FrameNum : Int = 0
//
//    var AtomName1 : String = ""
//    var ResidueNum1 : Int = 0
//    var ResidueName1 : String = ""
//
//    var AtomName2 : String = ""
//    var ResidueNum2 : Int = 0
//    var ResidueName2 : String = ""
//
//    var AtomName3 : String = ""
//    var ResidueNum3 : Int = 0
//    var ResidueName3 : String = ""
//
//    var AtomName4 : String = ""
//    var ResidueNum4 : Int = 0
//    var ResidueName4 : String = ""
//
//    var dihedralAngle : Double = 0.0
//    var AngleType : String = ""
//
//    while (i < pdbdfArray.size) {
//
//      FrameNum = pdbdfArray(i)(0).toString.trim.toInt
//
//      AtomName1 = pdbdfArray(i)(1).toString
//      ResidueNum1 = pdbdfArray(i)(2).toString.trim.toInt
//      ResidueName1 = pdbdfArray(i)(3).toString
//
//      AtomName2 = pdbdfArray(i)(4).toString
//      ResidueNum2 = pdbdfArray(i)(5).toString.trim.toInt
//      ResidueName2 = pdbdfArray(i)(6).toString
//
//      AtomName3 = pdbdfArray(i)(7).toString
//      ResidueNum3 = pdbdfArray(i)(8).toString.trim.toInt
//      ResidueName3 = pdbdfArray(i)(9).toString
//
//      AtomName4 = pdbdfArray(i)(10).toString
//      ResidueNum4 = pdbdfArray(i)(11).toString.trim.toInt
//      ResidueName4 = pdbdfArray(i)(12).toString
//
//      dihedralAngle = pdbdfArray(i)(13).toString.trim.toDouble
//      AngleType = pdbdfArray(i)(14).toString
//
//
//      newarray2(i) = f"\n"
//      i += 1
//    }

    val os2 = new BufferedOutputStream( fs.create( (new Path("out_file_CRDDihedral_Angles") )))
    dihedralArray.foreach(line => os2.write(line.toString().getBytes("UTF-8")))
    os2.close()
  }

}
