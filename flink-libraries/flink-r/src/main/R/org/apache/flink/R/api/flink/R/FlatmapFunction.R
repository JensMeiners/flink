FlatmapFunction <- function()
{
  #chprint("init FlatmapFunction")
  c <- Function()
  
  c$.configure <- function(input_file, output_file, port, info, subtask_index) {}
  
  c$.run <- function() {
    #chprint("flatmapfunc .run")
    #chprint(paste0("FlatmapFunction .run scope: ", class(c)))
    while (c$.iterator$has_next()) {
      val <- c$.iterator$nxt()
      result <- c$flatmap(val)
      if (is.null(result) == FALSE){
        for (r in result) {
          c$.collector$collect(r)
        }
      }
    }
    c$.collector$.close()
  }
  
  c$collect <- function(value) {
    result <- c$flatmap(val)
    if (is.null(result) == FALSE){
      for (r in result) {
        c$.collector$collect(r)
      }
    }
  }
  
  c$flatmap <- function(operator) {
    #print("FlatmapFunction.flatmap()")
    return(NULL)
  }
  
  class(c) <- c("FlatmapFunction", "Function")
  return(c)
}

run.FlatmapFunction <- function(obj) {
  obj$.run()
}
