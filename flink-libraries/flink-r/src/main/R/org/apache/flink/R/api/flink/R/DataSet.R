DataSet <- function(info) {

  nc <- new.env()
  nc$info <- info
  
  nc$info$id <- as.integer(.flinkREnv$counter)
  increment_counter()

  nc$map <- function(operator) {
    # Applies a Map transformation on a DataSet.
    if (is.function(operator)) {
      f <- operator
      operator <- MapFunction()
      operator$map <- f
    }

    child <- newOperationInfo()
    child_set <- DataSet(child)
    child$identifier <- Identifier.MAP
    child$parent <- nc$info
    child$operator <- operator
    child$types <- createArrayTypeInfo()
    child$name <- "RMap"
    nc$info$children_append(child)
    .sets_append(child)
    return(child_set)
  }

  nc$reduce <- function(operator) {
    # Applies a Reduce transformation on a DataSet.
    if (is.function(operator)) {
      f <- operator
      operator <- ReduceFunction()
      operator$reduce <- f
    }

    child <- newOperationInfo()
    child_set <- DataSet(child)
    child$identifier <- Identifier.REDUCE
    child$parent <- nc$info
    child$operator <- operator
    child$types <- createArrayTypeInfo()
    child$name <- "RReduce"
    nc$info$children_append(child)
    .sets_append(child)
    return(child_set)
  }
  
  nc$filter <- function(operator) {
    if (is.function(operator)) {
      f <- operator
      operator <- FilterFunction()
      operator$filter <- f
    }
    
    child <- newOperationInfo()
    child_set <- DataSet(child)
    child$identifier <- Identifier.FILTER
    child$parent <- nc$info
    child$operator <- operator
    child$types <- createArrayTypeInfo()
    child$name <- "RFilter"
    nc$info$children_append(child)
    .sets_append(child)
    return(child_set)
  }
  
  nc$flatmap <- function(operator) {
    if (is.function(operator)) {
      f <- operator
      operator <- FlatmapFunction()
      operator$flatmap <- f
    }
    
    child <- newOperationInfo()
    child_set <- DataSet(child)
    child$identifier <- Identifier.FLATMAP
    child$parent <- nc$info
    child$operator <- operator
    child$types <- createArrayTypeInfo()
    child$name <- "RFlatMap"
    nc$info$children_append(child)
    .sets_append(child)
    return(child_set)
  }
  
  nc$groupBy <- function(key) {
    if (is.numeric(key) & !is.na(key)) {
      key <- list(key)
    }
    mapping <- function(x) {
      print(paste("MAPPINGin: ", x))
      return(x)
    }
    nc$map(mapping)$.group_by(key)
    #nc$.group_by(key)
  }
  
  nc$.group_by <- function(key) {
    #chprint(paste("DataSet groupby key",key))
    child <- newOperationInfo()
    child_chain <- list()
    child_set <- UnsortedGrouping(child, child_chain)
    child$identifier <- Identifier.GROUP
    child$parent <- nc$info
    child$keys <- key
    child_set$.child_chain[[1]] <- child
    nc$info$children_append(child)
    .sets_append(child)
    return(child_set)
  }
  
  nc$reduce_group <- function(operator, combinable=FALSE) {
    child <- nc$.reduce_group(operator, combinable)
    child_set <- OperatorSet(child)
    nc$info$children_append(child)
    .sets_append(child)
    return(child_set)
  }
  
  nc$.reduce_group <- function(operator, combinable=FALSE) {
    if (is.function(operator)) {
      f <- operator
      operator <- GroupReduceFunction()
      operator$reduce <- f
    }
    
    child <- newOperationInfo()
    child$identifier <- Identifier.GROUPREDUCE
    child$parent <- nc$info
    child$operator <- operator
    child$types <- createArrayTypeInfo()
    child$name <- "RGroupReduce"
    return(child)
  }

  nc$.sys_output <- function(to_error = FALSE) {
    child <- newOperationInfo()
    child_set <- DataSink(child)
    child$identifier <- Identifier.SINK_PRINT
    child$parent <- nc$info
    child$to_err <- to_error
    nc$info$parallelism <- child$parallelism
    nc$info$sinks_append(child)
    .sinks_append(child)
    return(child_set)
  }

  nc$sys_output <- function(to_error = FALSE) {
    return(nc$map(Stringify())$.sys_output(to_error))
  }
  
  nc$.csv_output <- function(path="./output.txt", line_delimiter="\n", field_delimiter=",", write_mode=WriteMode.OVERWRITE) {
    child <- newOperationInfo()
    child_set <- DataSink(child)
    child$identifier <- Identifier.SINK_CSV
    child$parent <- nc$info
    child$path <- path
    child$delimiter_line <- line_delimiter
    child$delimiter_field <- field_delimiter
    child$write_mode <- write_mode
    nc$info$parallelism <- child$parallelism
    nc$info$sinks_append(child)
    .sinks_append(child)
    return(child_set)
  }
  
  nc$csv_output <- function(path=.flinkREnv$output_path, line_delimiter="\n", field_delimiter=",", write_mode=WriteMode.OVERWRITE) {
    return(nc$map(Stringify())$.csv_output(path, line_delimiter, field_delimiter, write_mode))
  }

  class(nc) <- "DataSet"
  return(nc)
}

OperatorSet <- function(info) {
  dc <- DataSet(info)

  dc$with_broadcast_set <- function(name, set) {
    # TODO
    print("not yet implemented")
  }

  class(dc) <- c("OperatorSet", "DataSet")
  return(dc)
}

Stringify <- function() {
  #chprint("init Stringify")
  c <- MapFunction()

  c$map <- function(value) {
    return(toString(value))
  }

  class(c) <- c("Stringify", "MapFunction", "Function")
  return(c)
}
