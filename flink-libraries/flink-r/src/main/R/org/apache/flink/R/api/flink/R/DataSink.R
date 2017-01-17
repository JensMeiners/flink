DataSink <- function(info) {
  c = list(info = info)
  c$info$id <- as.integer(.flinkREnv$counter)
  increment_counter()

  c$name <- function(name) {
    c$info$name <- name
    return(c)
  }

  c$set_parallelism <- function(parallelism) {
    c$info$parallelism <- parallelism
    return(c)
  }

  class(c) <- "DataSink"
  return(c)
}
