WriteMode.NO_OVERWRITE <-  as.integer(0)
WriteMode.OVERWRITE <-  as.integer(1)

#Gotta be kept in sync with java constants!
Identifier.SORT <- "sort"
Identifier.GROUP <- "groupby"
Identifier.COGROUP <- "cogroup"
Identifier.CROSS <- "cross"
Identifier.CROSSH <- "cross_h"
Identifier.CROSST <- "cross_t"
Identifier.FLATMAP <- "flatmap"
Identifier.FILTER <- "filter"
Identifier.MAPPARTITION <- "mappartition"
Identifier.GROUPREDUCE <- "groupreduce"
Identifier.JOIN <- "join"
Identifier.JOINH <- "join_h"
Identifier.JOINT <- "join_t"
Identifier.MAP <- "map"
Identifier.PROJECTION <- "projection"
Identifier.REDUCE <- "reduce"
Identifier.UNION <- "union"
Identifier.SOURCE_CSV <- "source_csv"
Identifier.SOURCE_TEXT <- "source_text"
Identifier.SOURCE_VALUE <- "source_value"
Identifier.SOURCE_SEQ <- "source_seq"
Identifier.SINK_CSV <- "sink_csv"
Identifier.SINK_TEXT <- "sink_text"
Identifier.SINK_PRINT <- "sink_print"
Identifier.BROADCAST <- "broadcast"
Identifier.FIRST <- "first"
Identifier.DISTINCT <- "distinct"
Identifier.PARTITION_HASH <- "partition_hash"
Identifier.REBALANCE <- "rebalance"

createArrayTypeInfo <- function() {
  return(array(charToRaw("byte")))
}

createKeyValueTypeInfo <- function(keyCount){
  key <- list(sapply(1:keyCount, function(x) createArrayTypeInfo()))
  class(key) <- "list"
  val <- createArrayTypeInfo()
  result <- list(key, val)
  class(result) <- "list"
  return(result)
}
