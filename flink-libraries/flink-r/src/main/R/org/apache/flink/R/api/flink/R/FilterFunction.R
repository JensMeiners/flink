FilterFunction <- function()
{
  #chprint("init FilterFunction")
  d <- Function()
  
  d$.configure <- function(input_file, output_file, port, info, subtask_index) {}
  
  d$.run <- function() {
    #chprint("filterfunc .run")
    while (d$.iterator$has_next()) {
      val <- d$.iterator$nxt()
      if (d$filter(val)) {
        d$.collector$collect(val)
      }
    }
    d$.collector$.close()
  }
  
  d$collect <- function(value) {
    if (d$filter(value)) {
      d$.collector$collect(value)
    }
  }
  
  d$filter <- function(operator) {
    #print("FilterFunction.filter()")
  }
  
  class(d) <- c("FilterFunction", "Function")
  return(d)
}

run.FilterFunction <- function(obj) {
  #chprint("filterfunc .run")
  while (obj$.iterator$has_next()) {
    #start <- as.numeric(Sys.time())
    val <- obj$.iterator$nxt()
    #end <- as.numeric(Sys.time())
    #chprint(paste0("+,deser input,",toString(end-start),","))
    #chprint("got next val")
    #start <- as.numeric(Sys.time())
    if (obj$filter(val)) {
      #end <- as.numeric(Sys.time())
      #chprint(paste0("+,filter,",toString(end-start),","))
      #start <- as.numeric(Sys.time())
      obj$.collector$collect(val)
      #end <- as.numeric(Sys.time())
      #chprint(paste0("+,ser result,",toString(end-start),","))
      #chprint("collected val")
    }
  }
  obj$.close()
}
