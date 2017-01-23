DataSet <- function(info) {

  nc <- list(
    info = info
  )
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
  
  nc$csv_output <- function(path="./output.txt", line_delimiter="\n", field_delimiter=",", write_mode=WriteMode.OVERWRITE) {
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
  chprint("init Stringify")
  c <- MapFunction()

  c$map <- function(value) {
    return(toString(value))
  }

  class(c) <- c("Stringify", "MapFunction", "Function")
  return(c)
}
