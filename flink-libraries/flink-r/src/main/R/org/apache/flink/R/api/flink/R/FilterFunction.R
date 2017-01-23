FilterFunction <- function()
{
  chprint("init FilterFunction")
  c <- Function()
  
  c$.configure <- function(input_file, output_file, port, info, subtask_index) {}
  
  c$.run <- function() {
    chprint("filterfunc .run")
    chprint(paste0("FilterFunction .run scope: ", class(c)))
    while (c$.iterator$has_next()) {
      val <- c$.iterator$nxt()
      if (c$filter(val)) {
        c$.collector$collect(val)
      }
    }
    c$.collector$.close()
  }
  
  c$collect <- function(value) {
    if (c$filter(value)) {
      c$.collector$collect(value)
    }
  }
  
  c$filter <- function(operator) {
    print("FilterFunction.filter()")
  }
  
  class(c) <- c("FilterFunction", "Function")
  return(c)
}

run.FilterFunction <- function(obj) {
  chprint("filterfunc .run")
  while (obj$.iterator$has_next()) {
    val <- obj$.iterator$nxt()
    chprint("got next val")
    if (obj$filter(val)) {
      obj$.collector$collect(val)
      chprint("collected val")
    }
  }
  obj$.close()
}