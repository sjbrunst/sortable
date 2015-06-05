
This repository contains my submission for the
[Sortable coding challenge](http://sortable.com/challenge/).

# Quick Start Instructions (Linux)

1. Clone this repository with `git clone https://github.com/sjbrunst/sortable.git`
2. `cd sortable` to move into the project's root directory
3. Download a copy of Spark by executing
`wget http://d3kbcqa49mib13.cloudfront.net/spark-1.3.1-bin-hadoop2.6.tgz`
4. Exectue `tar -xf ./spark-1.3.1-bin-hadoop2.6.tgz`
5. Run `./run.sh`
6. Results will be saved in results.txt.

# More Detailed Run Instructions

## Compiling

I included a compiled jar of my program, but if you want to compile it yourself you need sbt. Since
Sortable's website says that they use Scala, I assume you already have sbt installed. If not, you
can get sbt [here](http://www.scala-sbt.org/download.html).

To compile my program execute `sbt package` from the root directory of this project. This will
update the jar file in target/scala-2.10.

## Running

Running this program requires Spark. If you do not have Spark already you can download a recent
copy [here](http://spark.apache.org/downloads.html). Set the `sparkPath` configuration option in
run.sh to be the path to Spark's home folder.

After `sparkPath` is set, you can run my program by executing run.sh. Other input and output
configuration options, such as changing the input files, can be modified in run.sh.

# Other Information

## Spark

I chose to complete this coding challenge using Apache Spark. This choice was made for a few
reasons:

* Spark will scale up nicely for problems with more input data. While the size of this coding
challenge may not require significant computing resources, it is still good to write code that is
scalable.
* The Spark SQL library makes it easy to read and write the JSON files for this challenge.
* I am currently using Spark for my Masters research. My familiarity with Spark will minimize the
amount of time required to create a good submission for this challenge.

## Algorithm

My algorithm generates all possible `(Product,Listing)` pairs, then filters them down to pairs that
look like a valid match. Then, to ensure that each listing is matched to at most one product, all
remaining products are found for each listing and the best products are selected.

Listings can have inconsistent capitalization (CANON, Canon, etc.) so all strings that will be
compared as part of the algorithm are first converted to lower case.

Furthermore, products and listings have their strings split into words by splitting on spaces,
hypens, and several common punctuation marks. This ensures that a string such as `AB-123` would be
considered identical to `AB 123`.

### Filtering

The first step in the filtering is to check the manufacturer. `(Product,Listing)` pairs are only
retained if the product and the listing's manufacturers share at least one word. This ensures that
a manufacturer such as "Konica Minolta" can be matched to listings with just "Konica" or just
"Minolta". It also ensures that a manufacturer such as "Canon" can be matched to a listing from
"Canon Canada".

The next step is to check the family (if the product has that field). I ensure that all words in
the family field of the product appear in the listing.

Finally, I check the model. This is the trickiest part. For example, the
Canon\_PowerShot\_SX130\_IS may have "SX130IS," "SX130 IS," or "SX 130 IS" in the listing. I want
to capture all of these cases. Furthermore, for a product such as the Nikon\_Coolpix\_900 with
model number 900, I do not want to match listings with model numbers 5900, 7900, or 900S. Thus it
is necessary to look at groups of words, rather than looking for "900" as a substring. My algorithm
searches groups of up to three adjacent words for the model number.

### Finding the best product for a listing

After all the filtering above, one listing may be matched to multiple products. If any products
have a non-empty family field then I only keep those products. I then choose the product that had
the most words from the manufacturer in the title.

## Known Issues

The challenge says "A single price listing may match at most one product." Listings in the input
file are not unique (example:
`{"title":"SAMSUNG PL200 - rouge","manufacturer":"Samsung","currency":"EUR","price":"170.99"}`
appears twice), but my algorithm enforces the challenge's constraint by ensuring that each unique
listing is only used once. If it is desired to distiguish between identical listings in the input
file, then they can be given an additional field with a unique id.

The given list of products all appear to be cameras, but the listings include items such as "Canon
LP-E6 Battery for Canon EOS 5D Mark II 7D LPE6" To avoid false positives for these listings, I
remove all listings that contain the words "for," "pour," or "für". This results in some missed
matches, but this is outweighed by the number of false positives that are avoided. This technique 
only works for English, French, and German listings.
