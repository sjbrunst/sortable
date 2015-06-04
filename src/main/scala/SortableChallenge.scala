import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object SortableChallenge {

  // characters used to divide strings into arrays of words
  val splitCharacters = Array(' ','-',';','.',',','(',')')

  case class Product(
        product_name: String,   // A unique id for the product
        manufacturer: String,
        family: String,         // optional grouping of products
        model: String
        // the announced-date is not useful for this algorithm
    )

  // Product with the manufacturer and family split into an array of words:
  case class SplitProduct(
        product_name: String,   // A unique id for the product
        manufacturer: Array[String],
        family: Array[String],         // optional grouping of products
        model: String
    )

  case class Listing(
      title: String,        // description of product for sale
      manufacturer: String, // who manufactures the product for sale
      currency: String,     // currency code, e.g. USD, CAD, GBP, etc.
      price: String         // price, e.g. 19.99, 100.00
    )

  // Listing with the title and manufacturer split into an array of words:
  case class SplitListing(
      title: Array[String],        // description of product for sale
      manufacturer: Array[String]  // who manufactures the product for sale
    )

  case class Result(
      product_name: String,
      listings: Array[Listing]
    )

  def main(args: Array[String]) {

    // TODO: pass these as command line arguments
    val listingsPath = "listings.txt"
    val productsPath = "products.txt"
    val resultsPath = "results"

    val conf = new SparkConf().setAppName("Sortable")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val listings = sqlContext.jsonFile(listingsPath)
                             .select("title","manufacturer","currency","price")
                             .map(row => Listing(row.getString(0),
                                                 row.getString(1),
                                                 row.getString(2),
                                                 row.getString(3)))
    val splitListings = listings.distinct() // without ids, listings are not unique
                                            // save time later by removing duplicates now
                                // avoid "battery for" false positives:
                                .filter(sl => !(sl.title.contains("for") ||
                                                sl.title.contains("pour") ||
                                                sl.title.contains("für")))
                                .map(l => (SplitListing(l.title.toLowerCase().split(splitCharacters),
                                                        l.manufacturer.toLowerCase().split(splitCharacters)),
                                           l)) // keep the original listing for output at the end
                                .cache()
    val splitProducts = sqlContext.jsonFile(productsPath)
                                  .select("product_name","manufacturer","family","model")
                                  .map(row => SplitProduct(row.getString(0),
                                                 row.getString(1).toLowerCase().split(" "),
                                                 // "family" is optional:
                                                 if (row.isNullAt(2)) {Array()}
                                                 else {row.getString(2)
                                                          .toLowerCase()
                                                          .split(splitCharacters)},
                                                 row.getString(3)
                                                    .toLowerCase()
                                                    // remove any spaces, etc:
                                                    .split(splitCharacters)
                                                    .reduce(_ + _)))
                                  .cache()

    val allPairs = splitProducts.cartesian(splitListings)
    // now we have all possible (product,listing) pairs, and we want to reduce them

    val filtered = allPairs.filter{case (sp,(sl,l)) => sl.manufacturer
                                                         .intersect(sp.manufacturer).length > 0}
                           .filter{case (sp,(sl,l)) => (sp.family.length == 0) ||
                                                       (sl.title.intersect(sp.family).length == sp.family.length)}
                           .filter{case (sp,(sl,l)) => (sl.title.contains(sp.model) ||
                                                       (sl.title.sliding(2).map(_.reduce(_ + _)).contains(sp.model)) ||
                                                       (sl.title.sliding(3).map(_.reduce(_ + _)).contains(sp.model))) }

    // So far one listing could be matched to multiple products.
    // uniqueListings below matches each listing to the best product.

    // I use the entire Listing object as the key here
    // If listings had unique ids then I would use that instead
    val uniqueListings = filtered.map{case (sp,(sl,l)) => (l,(sl,sp))}
                                 .groupByKey()
                                 .mapValues(slspList => (slspList.head._1, slspList.map(_._2)))
                                 .mapValues{case (sl, spList) => {
                                     // if there are products with non-empty family fields:
                                     if (spList.filter(_.family.length > 0).size > 0) {
                                       // only use products that had to match a family too:
                                       spList.filter(_.family.length > 0) 
                                          .maxBy(sp => (sl.title.intersect(sp.manufacturer)).length)
                                     } else {
                                       spList.maxBy(sp => (sl.title.intersect(sp.manufacturer)).length)}
                                     }
                                   }

    val results = uniqueListings.map{case (l, sp) => (sp.product_name, l)}
                                .groupByKey()
                                .map{case (product_name,lList) => Result(product_name,lList.toArray)}

    results.toDF().toJSON.saveAsTextFile(resultsPath)

    // print the unmatched listings for debug
    //val unmatched = listings.distinct().subtract(results.flatMap(r => r.listings))
    //unmatched.toDF().toJSON.saveAsTextFile("unmatched")

    sc.stop()
  }
}

