#!/bin/bash

### Configuration ###

# program input:
listingsFile=./listings.txt
productsFile=./products.txt

# output folder from Spark program:
resultsFolder=./results

# final output file:
resultsFile=./results.txt

### End of Configuration ###

../spark-1.3.1-bin-hadoop2.6/bin/spark-submit --class SortableChallenge --master local[4] target/scala-2.10/sortable_2.10-1.0.jar \
    --listings $listingsFile \
    --products $productsFile \
    --results $resultsFolder

cat ${resultsFolder}/part-* > $resultsFile
rm -r $resultsFolder
