ReduceFunction <- function()
{
  nc <- Function()

  nc$.configure <- function(input_file, output_file, port, info, subtask_index) {
    nc$.configure(input_file, output_file, port, info, subtask_index)
    if (length(info$key1) == 0) {
      nc$.run = nc$.run_all_reduce
    } else {
      nc$.run = nc$.run_grouped_reduce
      nc$.group_iterator <- GroupIterator(nc$.iterator, info$key1)
    }
  }

  nc$.run <- function() {}

  nc$.run_all_reduce <- function() {
    collector <- nc$.collector
    func <- nc$reduce
    iterator <- nc$.iterator
    if (iterator$has_next()) {
      base <- iterator$nxt()
      while (iterator$has_next()) {
        value <- iterator$nxt()
        base <- func(base, value)
      }
      collector$collect(base)
    }
    nc$.close()
  }

  nc$.run_grouped_reduce <- function() {
    collector <- nc$.collector
    func <- nc$reduce
    iterator <- nc$.group_iterator
    iterator$.init()
    while(iterator$has_group()) {
      iterator$next_group()
      if (iterator$has_next()) {
        base <- iterator$nxt()
        while (iterator$has_next()) {
          value <- iterator$nxt()
          base <- func(base, value)
        }
      }
      collector$collect(base)
    }
    nc$.close()
  }

  nc$reduce <- function(val1, val2) {
    print("ReduceFunction.reduce()")
  }

  nc$combine <- function(val1, val2) {
    nc$reduce(val1, val2)
  }
  ## Set the name for the class
  class(nc) <- c("ReduceFunction", "Function")
  return(nc)
}
