PlanCollector <- function(connection)
{
  nc <- list(
    connection = connection$get()
  )

  nc$collect <- function(value) {
    print("collect")
    print("write type info")
    serializer.write_type_info(value, nc$connection)
    print("write value")
    serializer.write_value(value, nc$connection)

  }

  ## Set the name for the class
  class(nc) <- "PlanCollector"
  return(nc)
}


