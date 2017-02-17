
OperationInfo <- function() {
 info <- new.env()
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
   
   info$children_append <- function(child) {
     info$children[[length(info$children)+1]] <- child
   }
   
   info$sinks_append <- function(sink) {
     info$sinks[[length(info$sinks)+1]] <- sink
   }
   
   class(c) <- "OperationInfo"
   return(info)
} 
 
newOperationInfo <- function() {
  return(OperationInfo())
}

