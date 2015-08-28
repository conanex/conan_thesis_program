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

    val temp_cluster=sc.textFile(path+"new_k_means/k_2_16_ver"+i)
    
    val cluster_malware=temp_cluster.map{ line =>
      val pattern="\\((\\w|_|-)*,(\\d)*\\)".r
      val field=(pattern findAllIn line).toArray
      val malware_list=field.map(x =>x.split(",").apply(0).drop(1))
      malware_list
    }
    val array_cluster_malware=cluster_malware.collect
    val bingo=for{
      pattern <- array_cluster_malware
      //val a= pattern.mkString("\t")
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
    
    
    for( x<-bingo){
      val filename=x._1.mkString("\t")
      x._2.repartition(1).saveAsTextFile(path+"test/k_2000_result/"+filename+"/raw")
      x._2.map(a => a._1 ).distinct.repartition(1).saveAsTextFile(path+"test/k_2000_result/"+filename+"/guid")
      
    }

     
    sc.stop()
  }
  def checkPattern(name:String,p:String, take:Boolean):Boolean={
    val pattern=p.r
    if(take)
      pattern.findFirstIn(name).isDefined
    else
      pattern.findFirstIn(name).isEmpty
  }
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): (Double,Array[((Int,Int),Double,Double,Double)]) = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      
      val record=predictionsAndRatings.map(x => ( (x._1,x._2._2,x._2._1,( (x._2._1 - x._2._2) * (x._2._1 - x._2._2) ) )))
      val error=predictionsAndRatings.map(x => (x._1,( (x._2._1 - x._2._2) * (x._2._1 - x._2._2) )))

      val total_error=math.sqrt(error.values.reduce(_ + _) / n)
      val top10=record.top(10)(new Ordering[((Int,Int),Double,Double,Double)]() {
          override def compare(x:((Int, Int),Double,Double,Double), y: ((Int, Int),Double,Double,Double)): Int = 
                  Ordering[Double].compare(x._4, y._4)
      })
      (total_error,top10)

  }
  def SquaredDistance(v1:Vector,v2:Vector):Double={
    val size = v1.size
    val values1=v1.toArray
    val values2=v2.toArray
    
    var sum = 0.0 
    var i = 0 
    while (i < size) {
      sum += (values1(i) - values2(i)) *(values1(i) - values2(i)) 
      i += 1
    }   
    sum
  }

}

case class MD_info(timestamp:Long, country:String , family:String , industry:String )
