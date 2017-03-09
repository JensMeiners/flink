ReduceFunction <- function()
{
  nc <- Function()

  nc$.configure <- function(input_file, output_file, port, info, subtask_index) {
    #nc$.configure(input_file, output_file, port, info, subtask_index)
    if (length(info$key1) == 0) {
      nc$.run = nc$.run_all_reduce
    } else {
      nc$.run = nc$.run_grouped_reduce
      nc$.group_iterator <- GroupIterator(nc$.iterator, info$key1)
    }
  }

  nc$.run <- function() {}

  nc$.run_all_reduce <- function() {
    #chprint("run all reduce")
    collector <- nc$.collector
    func <- nc$reduce
    iterator <- nc$.iterator
    if (iterator$has_next()) {
      base <- iterator$nxt()
      while (iterator$has_next()) {
        value <- iterator$nxt()
        base <- func(base, value)
        #chprint(paste("base: ", base))
      }

      collector$collect(base)
    }
    nc$.close()
  }

  nc$.run_grouped_reduce <- function() {
    #chprint("run_grouped_reduce")
    collector <- nc$.collector
    func <- nc$reduce
    iterator <- nc$.group_iterator
    iterator$.init()
    while(iterator$has_group()) {
      iterator$next_group()
      if (iterator$has_next()) {
        base <- iterator$nxt()
        #chprint(paste("grouped_reduce start base", base))
        while (iterator$has_next()) {
          value <- iterator$nxt()
          base <- func(base, value)
          #chprint(paste("grouped_reduce",base,value))
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

run.ReduceFunction <- function(obj) {
  #chprint("redfunc .run")
  obj$.run()
}
