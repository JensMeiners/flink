GroupReduceFunction <- function()
{
  nc <- Function()
  
  nc$.configure <- function(input_file, output_file, port, info, subtask_index) {
    #nc$.configure(input_file, output_file, port, info, subtask_index)
    if (length(info$key1) == 0) {
      nc$.run = nc$.run_all_group_reduce
    } else {
      nc$.run = nc$.run_grouped_group_reduce
      nc$.group_iterator <- GroupIterator(nc$.iterator, info$key1)
    }
  }
  
  nc$.run <- function() {}
  
  nc$.run_all_group_reduce <- function() {
    chprint("run all group reduce")
    collector <- nc$.collector
    func <- nc$reduce
    iterator <- nc$.iterator
    result <- func(iterator, collector)
    if (is.null(result) == FALSE) {
      for (value in result) {
        collector$collect(value)
      }
    }
    nc$.close()
  }
  
  nc$.run_grouped_group_reduce <- function() {
    collector <- nc$.collector
    func <- nc$reduce
    iterator <- nc$.group_iterator
    iterator$.init()
    while(iterator$has_group()) {
      iterator$next_group()
      result <- func(iterator, collector)
      if (is.null(result) == FALSE) {
        for (value in result) {
          collector$collect(value)
        }
      }
    }
    nc$.close()
  }
  
  nc$reduce <- function(iterator, collector) {
    print("ReduceGroupFunction.reduce()")
  }
  
  nc$combine <- function(iterator, collector) {
    nc$reduce(iterator, collector)
  }
  ## Set the name for the class
  class(nc) <- c("GroupReduceFunction", "Function")
  return(nc)
}

run.GroupReduceFunction <- function(obj) {
  chprint("groupreducefunc .run")
  obj$.run()
}
