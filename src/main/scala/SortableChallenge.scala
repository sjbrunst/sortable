import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object SortableChallenge {

  def main(args: Array[String]) {

    // TODO: pass these as command line arguments
    val listingsPath = "listings.txt"
    val productsPath = "products.txt"

    val conf = new SparkConf().setAppName("Sortable")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val listings = sqlContext.jsonFile(listingsPath)
    val products = sqlContext.jsonFile(productsPath)

    println("Number of listings: " + listings.count().toString)
    println("Number of products: " + products.count().toString)

    sc.stop()
  }
}

