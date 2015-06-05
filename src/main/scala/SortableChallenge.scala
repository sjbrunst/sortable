import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object SortableChallenge {

  // characters used to divide strings into arrays of words:
  val splitCharacters = Array(' ','-',';','.',',','(',')')

  // default values for command line arguments:
  var listingsPath = "listings.txt"
  var productsPath = "products.txt"
  var resultsPath = "results"

  /* This class is commented out since it is not actually necessary
  case class Product(
    product_name: String,   // A unique id for the product
    manufacturer: String,
    family: String,         // optional grouping of products
    model: String
    // the announced-date is not useful for this algorithm
  )
  */

  // Product with the manufacturer and family split into an array of words:
  case class SplitProduct(
    product_name: String,   // A unique id for the product
    manufacturer: Array[String],
    family: Array[String],  // optional grouping of products
    model: String
  )

  case class Listing(
    title: String,        // description of product for sale
    manufacturer: String, // who manufactures the product for sale
    currency: String,     // currency code, e.g. USD, CAD, GBP, etc.
    price: String         // price, e.g. 19.99, 100.00
  )

  // Listing's title and manufacturer split into an array of words:
  case class SplitListing(
    title: Array[String],        // description of product for sale
    manufacturer: Array[String]  // who manufactures the product for sale
  )

  case class Result(
    product_name: String,
    listings: Array[Listing]
  )

  // Read the command line arguments
  def processArgs(argList: List[String]): Unit = {
    argList match {
      case Nil => return
      case "--listings" :: value :: tail => {
        listingsPath = value
        processArgs(tail)
      }
      case "--products" :: value :: tail => {
        productsPath = value
        processArgs(tail)
      }
      case "--results" :: value :: tail => {
        resultsPath = value
        processArgs(tail)
      }
      case otherString :: tail => {
        println("Unknown option: " + otherString)
        System.exit(1)
      }
    }
  }

  def main(args: Array[String]) {

    processArgs(args.toList)

    val conf = new SparkConf().setAppName("Sortable")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // All Listings from the input file:
    val listings = sqlContext.jsonFile(listingsPath)
                             .select("title","manufacturer","currency","price")
                             .map(row => Listing(
                                           row.getString(0),  // title
                                           row.getString(1),  // manufacturer
                                           row.getString(2),  // currency
                                           row.getString(3))) // price

    // (SplitListing,Listing) pairs for listings we wish to match:
    val splitListings = listings.distinct() // without ids, listings are not unique
                                            // save time later by removing duplicates now
                                // avoid "battery for" or "lens for" false positives:
                                .filter(l => !(l.title.contains("for") ||
                                               l.title.contains("pour") ||
                                               l.title.contains("für")))
                                .map(l => (SplitListing(
                                             l.title
                                              .toLowerCase()
                                              .split(splitCharacters),
                                             l.manufacturer
                                              .toLowerCase()
                                              .split(splitCharacters)),
                                           l)) // keep the original listing for output at the end
                                .cache()

    // SplitProducts for all products in the input file:
    val splitProducts = sqlContext.jsonFile(productsPath)
                                  .select("product_name","manufacturer","family","model")
                                  .map(row => SplitProduct(
                                                row.getString(0),       // product_name
                                                row.getString(1)        // manufacturer
                                                   .toLowerCase()
                                                   .split(" "),
                                                if (row.isNullAt(2)) {  // family
                                                  Array()
                                                } else {
                                                  row.getString(2)
                                                     .toLowerCase()
                                                     .split(splitCharacters)
                                                },
                                                row.getString(3)        // model
                                                   .toLowerCase()
                                                   // next 2 lines remove spaces, etc:
                                                   .split(splitCharacters)
                                                   .reduce(_ + _)))
                                  .cache()

    // Possible matches are stored as (SplitProduct,(SplitListing,Listing)) pairs:
    val allPairs = splitProducts.cartesian(splitListings)

    // We now have all possible matches, and we want to filter them to ones that make sense.

    val filtered = allPairs.filter{case (sp,(sl,l)) => sl.manufacturer
                                                         .intersect(sp.manufacturer)
                                                         .length > 0}
                           .filter{case (sp,(sl,l)) => (sp.family.length == 0) ||
                                                       (sl.title
                                                          .intersect(sp.family)
                                                          .length == sp.family.length)}
                           .filter{case (sp,(sl,l)) => (sl.title.contains(sp.model) ||
                                                       (sl.title
                                                          .sliding(2)
                                                          .map(_.reduce(_ + _))
                                                          .contains(sp.model)) ||
                                                       (sl.title
                                                          .sliding(3)
                                                          .map(_.reduce(_ + _))
                                                          .contains(sp.model))) }

    // So far one listing could be matched to multiple products.
    // uniqueListings below matches each listing to the best product.

    val uniqueListings = filtered.map{case (sp,(sl,l)) => (l,(sl,sp))}
                                 // I use the entire Listing object as the key here
                                 // If listings had unique ids then I would use that instead
                                 .groupByKey()
                                 // Now we have (Listing,List((SplitListing,SplitProduct))).
                                 // The SplitListing is the same for every pair in the list, so
                                 // take the first one:
                                 .mapValues(slspList => (slspList.head._1, slspList.map(_._2)))
                                 // Now we have (Listing,(SplitListing,List(SplitProduct)))
                                 .mapValues{case (sl, spList) => {
                                     // if there are products with non-empty family fields:
                                     if (spList.filter(_.family.length > 0).size > 0) {
                                       // only use products that had to match a family too:
                                       spList.filter(_.family.length > 0)
                                             .maxBy(sp => (sl.title
                                                             .intersect(sp.manufacturer))
                                                             .length)
                                     } else {
                                       spList.maxBy(sp => (sl.title
                                                             .intersect(sp.manufacturer))
                                                             .length)}
                                     }
                                   }

    // uniqueListings now has (Listing,SplitProduct) pairs

    val results = uniqueListings.map{case (l, sp) => (sp.product_name, l)}
                                .groupByKey()
                                .map{case (product_name,lList) => Result(
                                                                    product_name,
                                                                    lList.toArray)}

    results.toDF().toJSON.saveAsTextFile(resultsPath)

    sc.stop()
  }
}

