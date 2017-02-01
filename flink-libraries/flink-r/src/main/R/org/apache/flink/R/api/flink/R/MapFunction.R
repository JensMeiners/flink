MapFunction <- function()
{
  chprint("init MapFunction")
  c <- Function()

  c$.configure <- function(input_file, output_file, port, info, subtask_index) {}

  c$.run <- function() {
    chprint("mapfunc .run")
    chprint(paste0("MapFunction .run scope: ", class(c)))
    while (c$.iterator$has_next()) {
      val <- c$.iterator$nxt()
      c$.collector$collect(c$map(val))
    }
    c$.collector$.close()
  }

  c$collect <- function(value) {
    c$.collector$collect(c$map(value))
  }

  c$map <- function(operator) {
    print("MapFunction.map()")
  }

  class(c) <- c("MapFunction", "Function")
  return(c)
}

run <- function(obj) {
  UseMethod("run", obj)
}

run.MapFunction <- function(obj) {
  chprint("mapfunc .run")
  while (obj$.iterator$has_next()) {
    val <- obj$.iterator$nxt()
    chprint(paste("got next val", val, "class", class(val)))
    obj$.collector$collect(obj$map(val))
    chprint("collected val")
  }
  obj$.close()
}
