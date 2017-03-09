Grouping <- function(info, child_chain) {
  
  c <- new.env()
  c$.child_chain <- child_chain
  c$.info <- info
  
  c$.info$id <- as.integer(.flinkREnv$counter)
  increment_counter()
  
  c$.finalize <- function() {}
  
  c$first <- function(count) {
    c$.finalize()
    child <- newOperationInfo()
    child_set <- DataSet(child)
    child$identifier <- Identifier.FIRST
    child$parent <- c$.info
    child$count <- count
    c$.info$children_append(child)
    .sets_append(child)
    return(child_set)
  }
  
  c$reduce_group <- function(operator, combinalble=FALSE) {
    #chprint("reduce_group")
    child <- c$.reduce_group(operator, combinalble)
    child_set <- OperatorSet(child)
    c$.info$parallelism <- child$parallelism
    c$.info$children_append(child)
    .sets_append(child)
    return(child_set)
  }
  
  c$.reduce_group <- function(operator, combinable=FALSE) {
    c$.finalize()
    if (is.function(operator)) {
      f <- operator
      operator <- GroupReduceFunction() 
      operator$reduce <- f
    }
    
    child <- newOperationInfo()
    child$identifier <- Identifier.GROUPREDUCE
    child$parent <- c$.info
    child$operator <- operator
    child$types <- createArrayTypeInfo()
    child$name <- "RGroupReduce"
    child$key1 <- c$.child_chain[[1]]$keys # TODO: check validity
    return(child)
  }
  
  # TODO: sort_group
  
  class(c) <- "Grouping"
  return(c)
}

UnsortedGrouping <- function(info, child_chain) {
  c <- Grouping(info, child_chain)
  
  c$reduce <- function(operator) {
    c$.finalize()
    if (is.function(operator)) {
      f <- operator
      operator <- ReduceFunction()
      operator$reduce <- f
    }
    
    child <- newOperationInfo()
    child_set <- OperatorSet(child)
    child$identifier <- Identifier.REDUCE
    child$parent <- c$.info
    child$operator <- operator
    child$types <- createArrayTypeInfo()
    child$name <- "RReduce"
    child$key1 <- c$.child_chain[[1]]$keys # TODO: check validity
    c$.info$children_append(child)
    .sets_append(child)
    return(child_set)
  }
  
  c$aggregate <- function(aggregation, field) {
    print("not yet implemented")
  }
  
  c$min <- function(field) {
    print("not yet implemented")
  }
  
  c$max <- function(field) {
    print("not yet implemented")
  }
  
  c$sum <- function(field) {
    print("not yet implemented")
  }
  
  c$.finalize <- function() {
    grouping <- c$.child_chain[[1]]
    keys <- grouping$keys
    f <- NULL
    if (is.function(keys[[1]])) {
      f <- function(x) c(keys[[1]])
    } else {
      f <- function(x) sapply(keys, function(key) x[[key+1]])
    }
    grouping$parent$operator$map <- function(x) {
      #chprint(paste("finalize input",x,"class",class(x)))
      list(f(x), as.list(x))
    }
    grouping$parent$types <- createKeyValueTypeInfo(length(keys))
    grouping$keys <- lapply(0:(length(grouping$keys)-1), function(i) i)
  }
  
  class(c) <- c("UnsortedGrouping", "Grouping")
  return(c)
}