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
    val conf=new SparkConf().setAppName("ALS")
    val sc = new SparkContext(conf)
    
    var path=""
    if(args(0)=="itri")
      path="hdfs:///user/conan/"
    else
      path="hdfs:///"
    
    /**
     * Load training and testing data
     */
    val train_data = sc.textFile(path+"data_set_use4week/training_data")
    val test_data  = sc.textFile(path+"data_set_use4week/testing_data")
    val training = train_data.map{line=>
        var line2=line.drop(1).dropRight(1)
        var fields=line2.split(",")
        Rating(fields(0).toInt,fields(1).toInt ,/*fields(2).toDouble*/ 1 )
    }
    val testing = test_data.map{line=>
        var line2=line.drop(1).dropRight(1)
        var fields=line2.split(",")
        Rating(fields(0).toInt,fields(1).toInt ,/*fields(2).toDouble*/ 1 )
    }
    
    /**
     * Do ALS
     */
    val numTrain=training.count()
    val numTest=testing.count()
    
    val ranks =List(10,20)
    val lambdas =List(0.01,0.1)
    val numIters=List(15)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank =0
    var bestLambda = -1.0
    var bestNumIter = -1
  
    for(rank <- ranks ; lambda<-lambdas; numIter <- numIters){

      val model = ALS.train(training, rank, numIter, lambda)
    
       
      val result= computeRmse(model, testing, numTest)
      val validationRmse=result._1
      //Prediction error top10
      val top10=result._2
      
      println("Rank= "+rank+"\tLambda= "+lambda+"\tIter= "+numIter+"\tRMSE(test)= "+validationRmse)
      println(top10.deep.mkString("\n"))
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      } 


    }
    /**
     * Save GUID and malware features
     */
    bestModel match{
      case Some(s) => {
        s.productFeatures.map(x=>(x._1,x._2.mkString("\t"))).saveAsTextFile(path+"ALS_result/non_filter/binary/malware_feature")
        s.userFeatures.map(x=>(x._1,x._2.mkString("\t"))).saveAsTextFile(path+"ALS_result/non_filter/binary/guid_feature")     
      }
      case None => println("ERROR:model not found!")
    }

    println("=========================================================================================================")
    println("BEST\nRank= "+bestRank+"\tLambda= "+bestLambda+"\tIter= "+bestNumIter+"\tRMSE(test)= "+bestValidationRmse)
    

    sc.stop()
  }
  /**
   * Compute total RMSE and caculate top 10 prediction error 
   * return (RMSE,top10), top10 format is (matrix entry,original , predict  )
   */
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

}

case class MD_info(timestamp:Long, country:String , family:String , industry:String )
