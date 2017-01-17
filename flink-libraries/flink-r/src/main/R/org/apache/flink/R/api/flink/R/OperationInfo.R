
OperationInfo <- setRefClass("OperationInfo",
 fields = list(
   #fields being transferred to the java side
   identifier = "ANY",#as.integer(-1),
   parent = "ANY",
   other = "ANY",
   field = "integer",#as.integer(-1),
   order =  "integer",#as.integer(0),
   keys = "list",#list(),
   key1 = "list",#list(),
   key2 = "list",#list(),
   types = "ANY",
   uses_udf = "logical",#FALSE,
   name = "ANY",
   delimiter_line = "ANY",# "\n",
   delimiter_field = "ANY",# ",",
   write_mode = "ANY",#WriteMode.NO_OVERWRITE,
   path = "ANY",#""
   frm =  "integer",#as.integer(0),
   to =  "integer",#as.integer(0),
   count =  "integer",#as.integer(0),
   values = "ANY",#list(),
   projections = "list",#list(),
   id =  "integer",#as.integer(-1),
   to_err = "logical",#FALSE,
   parallelism =  "integer",#as.integer(-1),
   #internally used
   parent_set = "ANY",
   other_set = "ANY",
   chained_info = "ANY",
   bcvars = "list",#list(),
   sinks = "list",#list(),
   children = "list",#list(),
   operator = "ANY"
 ),
 methods = list(
   children_append = function(child) {
     children[[length(children)+1]] <<- child
   },
   sinks_append = function(sink) {
     sinks[[length(sinks)+1]] <<- sink
   }
   )
)
newOperationInfo <- function() {
  library(methods)
  info <- OperationInfo$new()
  info$identifier <- as.integer(-1)
  info$parent <- NULL
  info$other <- NULL
  info$field <- as.integer(-1)
  info$order <- as.integer(0)
  info$keys <- list()
  info$key1 <- list()
  info$key2 <- list()
  info$types <- NULL
  info$uses_udf <- FALSE
  info$name <- NULL
  info$delimiter_line <- "\n"
  info$delimiter_field <- ","
  info$write_mode <- WriteMode.NO_OVERWRITE
  info$path <- ""
  info$frm <- as.integer(0)
  info$to <- as.integer(0)
  info$count <- as.integer(0)
  info$values <- list()
  info$projections <- list()
  info$id <- as.integer(-1)
  info$to_err <- FALSE
  info$parallelism <- as.integer(-1)
  info$parent_set <- NULL
  info$other_set <- NULL
  info$chained_info <- NULL
  info$bcvars <- list()
  info$sinks <- list()
  info$children <- list()
  info$operator <- NULL
  return(info)
}

