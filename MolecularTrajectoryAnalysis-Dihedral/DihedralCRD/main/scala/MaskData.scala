import java.io.BufferedOutputStream

import MainCall._
import createDF.fs
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.control.Breaks._
import org.apache.spark.sql._

object MaskData {

  def getQuery (query : String) : String = {
    var str = query  //":1-50!@H2O,Cl-,CA&:1-10@H5'',E,F"
    var size1 = str.size
    var Flag = ""
    var result = ""
    var i = 0
    while(i < size1){
      if(str(i) == ':'){
        result += "("
        i += 1
        val value = str(i)
        if(value >= 'A' && value <= 'Z' ){
          Flag = "ResidueName = "
          result += Flag
          breakable{
            while(i < size1 ){
              if(str(i) == '@'  || str(i) == '!' || str(i) == ':'){
                result = result +  ")and "
                break
              }
              else if(str(i) == '&'){
                result += ")"
                break;
              }
              else if(str(i) == ','){
                result += "or "
                result += Flag
                i += 1
              }
              else{
                // result += str(i)
                var name = ""
                while(i<size1 && str(i) >= 'A' && str(i) <= 'Z'){
                  name += str(i)
                  i += 1
                }
                result = result + "'"+name+"'"
              }

            }
          }
        }
        else{
          Flag = "ResidueNum "
          result += Flag
          breakable{
            while(i < size1 ){
              if( str(i) == '@'  || str(i) == '!' || str(i) == ':'){
                result += ")and "
                break
              }
              else if(str(i) == '&'){
                result += ")"
                break;
              }
              else if(str(i) == ','){
                result += "or "
                result += Flag
                i += 1
              }
              else{
                // result += str(i)
                var num = ""
                while(i < size1  && (str(i) >= '0' && str(i) <=  '9' || str(i) == '-')){
                  num += str(i)
                  i += 1
                }
                var arr  = num.split("-")
                var size2 = arr.size
                if(size2 == 1){
                  result = result + " = " +arr(0)
                }
                else{
                  result = result + ">=" + arr(0)+" and "+ Flag + "<=" + arr(1)
                }
              }
            }
          }
        }
      }
      else if(str(i) == '@'){

        result += "("
        i += 1
        val value = str(i)
        if(value >= 'A' && value <= 'Z' ){
          Flag = "AtomName = "
          result += Flag
          breakable{
            while(i < size1 ){
              if(str(i) == '@'  || str(i) == '!' || str(i) == ':'){
                result = result +  ")and "
                break
              }
              else if(str(i) == '&'){
                result += ")"
                break;
              }else if(str(i) == ','){
                result += "or "
                result += Flag
                i += 1
              }
              else{
                // result += str(i)
                var name = ""
                while(i<size1 && str(i) != '@' && str(i) != ':' && str(i) != '&' && str(i) != '!' && str(i) != ','){
                  name += str(i)
                  i += 1
                }
                result = result + "'"+name+"'"
              }

            }
          }
        }
        else{
          Flag = "AtomNum "
          result += Flag
          breakable{
            while(i < size1 ){
              if( str(i) == '@'  || str(i) == '!' || str(i) == ':'){
                result += ")and "
                break
              }
              else if(str(i) == '&'){
                result += ")"
                break;
              } else if(str(i) == ','){
                result += "or "
                result += Flag
                i += 1
              }
              else{
                // result += str(i)
                var num = ""
                while(i < size1  && (str(i) >= '0' && str(i) <=  '9' || str(i) == '-')){
                  num += str(i)
                  i += 1
                }
                var arr  = num.split("-")
                var size2 = arr.size
                if(size2 == 1){
                  result = result + " = " +arr(0)
                }
                else{
                  result = result + ">=" + arr(0)+" and "+ Flag + "<=" + arr(1)
                }
              }
            }
          }
        }
      }
      else if(str(i) == '&'){
        result += " or "
        i += 1
      }
      else if(str(i) == '!'){
        result += "!"
        i += 1
      }
    }
    result += ")"

    return result
  }

  def maskNETCDF (topoDF : DataFrame, coordDF : DataFrame, Query : String): DataFrame = {

    coordDF.createOrReplaceTempView("coor")
    var maskedTopoDF = topoDF.filter(getQuery(Query))
    maskedTopoDF.createOrReplaceTempView("topo")

    val query : String = "select c.FrameNum, t.Type, t.AtomNum, t.AtomName, t.ResidueName, t.ResidueNum, c.XCoord, c.YCoord, c.ZCoord from topo as t, coor as c where t.AtomNum = c.AtomNum"
    //FrameNum Type AtomNum AtomName ResidueName ResidueNum XCoord YCoord ZCoord

    var maskedDF = sqlContext.sql(query)

    var os1 = new BufferedOutputStream( fs.create( (new Path("out_file_getpdbdf") )))
    os1.write(maskedDF.count().toString.getBytes("UTF-8"))
    os1.close()

    return maskedDF
  }
}
