import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry,RowMatrix}
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage._
import java.util.Calendar
import scala.language.postfixOps

object Hamburger {
  def main(args: Array[String]){
    val conf=new SparkConf().setAppName("Final_analyze")
    val sc = new SparkContext(conf)
    
    /**
     * Check if it is on ITRI machine
     * if it is -> arg(0) is itri
     * else     -> arg(0) isn't itri
     */
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
    }.cache()
    
    /**
     * Load k-means cluster we just got
     */
    val temp_cluster=sc.textFile(path+"ALS_result/non_filter/binary/all_cluster_1600_pattern")
    
    /**
     * Use pattern match method to get malware name
     * {m1,m2... }
     */
    val cluster_malware=temp_cluster.map{ line =>
      val pattern="\\((\\w|_|-)*,(\\d)*\\)".r
      val field=(pattern findAllIn line).toArray
      val malware_list=field.map(x =>x.split(",").apply(0).drop(1))
      malware_list
    }
    /**
     * Transfer RDD to Array
     */
    val array_cluster_malware=cluster_malware.collect
    
    /**
     * For each cluster, we check if there are the same industries or the same countries
     */
    val bingo=for{
      pattern <- array_cluster_malware
      
      //Retrive original malware information
      same=malware_info.filter{x =>
        var check=false
        for(a <- pattern){
          check=checkPattern(x._2.family,a+"(.)*",true)||check
        }
        check
      }.cache()
      
      //check industry
      industry_num=same.map{ x =>
        if(x._2.industry=="-") "Not specified"
        else x._2.industry
      }.distinct.count
      
      //check country
      country_num=same.map(x => x._2.country).filter( x=> x!="XXX").distinct.count
      temp_same=same.unpersist(true)
      //guard
      if(industry_num==1||country_num==1)
    }yield (pattern,same,industry_num,country_num)
    
    /**
     * Save information to files 
     */
    for( x<-bingo){
      val filename=x._1.mkString("\t")
      x._2.repartition(1).saveAsTextFile(path+"ALS_result/non_filter/binary/k_1600_result/"+filename+"/raw")
      x._2.map(a => a._1 ).distinct.repartition(1).saveAsTextFile(path+"ALS_result/non_filter/binary/k_1600_result/"+filename+"/guid")
      
    }

     
    sc.stop()
  }
  /** 
   * check if the string "p" contains pattern "name"
   * take=true -> we want this
   * take=false -> we don't want this
   **/
  def checkPattern(name:String,p:String, take:Boolean):Boolean={
    val pattern=p.r
    if(take)
      pattern.findFirstIn(name).isDefined
    else
      pattern.findFirstIn(name).isEmpty
  }
  
}

case class MD_info(timestamp:Long, country:String , family:String , industry:String )
