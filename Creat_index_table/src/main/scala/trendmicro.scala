import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage._

object Hamburger {
  def main(args: Array[String]){
    val conf=new SparkConf().setAppName("Creat_table")
    val sc = new SparkContext(conf)
    
    var path=""
    if(args(0)=="itri")
      path="hdfs:///user/conan/"
    else
      path="hdfs:///"
    
    /**
     * Load malware files
     */
    val malware_data = sc.textFile(path+"malware/trendmicro1")
                        .union(sc.textFile(path+"malware/trendmicro2"))
                        .union(sc.textFile(path+"malware/trendmicro3"))

    val malware_info = malware_data.map{line=>
        var fields=line.split("\t")
        if (fields(2)=="" ) fields(2)="XXX"
        (fields(0).toString,MD_info(fields(1).toLong ,fields(2),fields(3),fields(4)  ))
    }
     

    /**
     * build table_version2 , use two-level family
     */
    val malware_transform=malware_info.map{x =>
      val f=x._2.family.split("\\.")
      val small=f(0)
      val mini=small.split("_")
      if(mini.length>1)
        (x._1,MD_info(x._2.timestamp, x._2.country , mini(0)+"_"+mini(1) , x._2.industry ) )
      else
        (x._1,MD_info(x._2.timestamp, x._2.country , mini(0) , x._2.industry ) )
    }
    /**
     * save GUID and Malware index table to avoid re-calculate
     */
    val save_guid=malware_transform.map(x => x._1 ).distinct().zipWithIndex()
    val save_virus=malware_transform.map(x => x._2.family ).distinct().zipWithIndex()
    save_guid.repartition(1).saveAsTextFile(path+"data_set_use4week/index_guid")
    save_virus.repartition(1).saveAsTextFile(path+"data_set_use4week/index_virus")
    /**
     * Load GUID and Malware table
     */
    val index_guid=sc.textFile(path+"data_set_use4week/index_guid").map{ x=>
      val line=x.drop(1).dropRight(1)
      var f=line.split(",")
      (f(0),f(1).toLong)
    }
    val index_virus=sc.textFile(path+"data_set_use4week/index_virus").map{ x=>
      val line=x.drop(1).dropRight(1)
      var f=line.split(",")
      (f(0),f(1).toLong)
    }
    /**
     * do mutiple join to produce readable formats
     * EX: (123,555,15 )
     * 123 is guid code name, 555 is malware code name, 15 is infection times
     */
    val rating_info=malware_transform.map(x => (x._1 , x._2.family) )
    val rating_one=rating_info.map(x => (x,1L)).reduceByKey(_ + _).map( x => ( x._1._1,(x._1._2,x._2) ))
    val ratings=rating_one.join(index_guid).map(x => (x._2._1._1,(x._2._2,x._2._1._2,x._1))).join(index_virus).map(x => x._2).map(x => (x._1._1,x._2,x._1._2))
    
    val split_data=ratings.randomSplit(Array(0.8,0.2))
    val training1=split_data(0)
    val testing1=split_data(1)
    
    training1.saveAsTextFile(path+"data_set_use4week/training_data")
    testing1.saveAsTextFile(path+"data_set_use4week/testing_data")
    rating_one.repartition(1).saveAsTextFile(path+"data_set_use4week/original_malware")
    ratings.repartition(1).saveAsTextFile(path+"data_set_use4week/ratings")
    
    
    sc.stop()
  }
  
}

case class MD_info(timestamp:Long, country:String , family:String , industry:String )
