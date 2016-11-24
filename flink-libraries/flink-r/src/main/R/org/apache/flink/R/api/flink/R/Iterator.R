PlanIterator <- function(connection)
{
  nc <- list(
    connection = connection$get()
  )

  nc$nxt <- function() {
    print("iterate plan")
    deser <- .get_deserializer(nc$connection)
    deser(nc$connection)
  }

  ## Set the name for the class
  class(nc) <- "PlanIterator"
  return(nc)
}
