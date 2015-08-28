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
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, squaredDistance => breezeSquaredDistance}

object Hamburger {
  def main(args: Array[String]){
    val conf=new SparkConf().setAppName("Cluster_Kmeans")
    val sc = new SparkContext(conf)
    
    var path=""
    if(args(0)=="itri")
      path="hdfs:///user/conan/"
    else
      path="hdfs:///"
    

    /**
     * prepare data
     * index_virus= ( <malware_index> ,<malware_name> )
     */
    val index_virus=sc.textFile(path+"data_set_use4week/index_virus").map{ x=>
      val line=x.drop(1).dropRight(1)
      var f=line.split(",")
      (f(1).toLong,f(0))
    }
    /**
     * prepare data
     * malware_feature= ( <malware_index>,<malware_feature_vector> )
     */
    val malware_f= sc.textFile(path+"ALS_result/non_filter/binary/malware_feature")
    val malware_feature=malware_f.map{x =>
      val line=x.drop(1).dropRight(1).split(",")
      val v=line(1).split("\t").map(_.toDouble)
      (line(0).toLong,Vectors.dense(v))

    }
    /**
     * prepare data
     * rating = ( <malware_index>,<#infect_guid>  )
     */
    val rating_f= sc.textFile(path+"data_set_use4week/ratings")
    val rating=rating_f.map{x =>
      val line=x.drop(1).dropRight(1).split(",")
      (line(1).toLong,1L)
    }.reduceByKey(_+_)
    /**
     * prepare data
     * virus_info = ( <malware_index>, ( <malware_name>,<#infect_guid> ) )
     */
    val virus_info=index_virus.join(rating)

    
    /**
     * Do K-means and do some filtering
     */
    val parsedData=malware_feature.values.cache()
    val numClusters = 1600
    val numIterations = 200
    /**
     * decode =  ( ( <malware_name>,<#infect_guid> ),<malware_feature_vector> ) 
     */
    val decode=virus_info.join(malware_feature).values.cache()
    
    /**
     * K-means k=1600
     */
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    
    /**
     * 1600 centers
     */
    val centers:Array[Vector] =clusters.clusterCenters
    
    
    /**
     * Transform to friendly format and do some filtering
     * result = (  <err>,<#malware in a cluster>,<malware_name and #infect_guid> )
     */
    val result=decode.map{x =>
      val master=clusters.predict(x._2)
      val err=SquaredDistance(x._2,centers(master) )
      
      //Delete weird malware
      val pattern="TROJ_GEN(.)*"
      val check= !(x._1._1=="TROJ_GEN" || x._1._1== "TROJ_GE" || x._1._1=="Malware" 
        || x._1._1=="TROJ_GENERIC" || x._1._1=="HI" || x._1._1=="TROJ_SPNR" )
      
      // X means #(infect guid) < 3, 0/1 means if it is a nonactive malware
      if(check && x._1._2>2 )
        ( (master, (1L, err ,x._1,0)) )
      else
        ( (master, (1L, err ,"X" ,1)) )

    }.reduceByKey( (x,y)=> (x._1+y._1 , x._2+y._2 , x._3+"\t"+ y._3 , x._4+y._4) )
      .filter(x => x._2._4==0)//filter clusters including "X" 
      .map( x=> (x._2._2,x._2._1,x._2._3) )//Normal result
      
    /**
     * filter(x => x._2>1) means deleting clusters which includes only one malware
     */
    result.filter(x=>x._2>1 ).repartition(1).sortBy( x => x._1,true)
     .saveAsTextFile(path+"ALS_result/non_filter/binary/all_cluster_1600_pattern")

        

    sc.stop()
  }
  /**
   * check if the string "p" contains pattern "name"
   * take=1 -> we want this
   * take=0 -> we don't want this
   */
  def checkPattern(name:String,p:String, take:Boolean):Boolean={
    val pattern=p.r
    if(take)
      pattern.findFirstIn(name).isDefined
    else
      pattern.findFirstIn(name).isEmpty
  }
  /**
   * returns 2 vector's Euler distance
   */
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
