RuntimeContext <- function(iter, coll, subtask_index) {
  c <- list(
    iterator = iter,
    collector = coll,
    broadcast_variables = list(),
    subtask_index = subtask_index
  )

  c$.add_broadcast_variable <- function(name, var) {
    c$broadcast_variables[name] <- var
  }

  c$get_broadcast_variable <- function(name) {
    return(c$broadcast_variables[name])
  }

  c$get_index_of_this_subtask <- function() {
    return(c$subtask_index)
  }

  class(c) <- "RuntimeContext"
  return(c)
}
