PlanCollector <- function(connection)
{
  nc <- list(
    connection = connection$get()
  )

  nc$collect <- function(value) {
    serializer.write_type_info(value, nc$connection)
    serializer.write_value(value, nc$connection)
  }

  ## Set the name for the class
  class(nc) <- "PlanCollector"
  return(nc)
}

Collector <- function(con, info) {

  c <- new.env()

  c$.con <- con
  c$serializer <- NULL
  c$.as_array <- class(info$types) == "array"
  
  c$.info <- info


  c$.close <- function() {
    c$.con$send_end_signal()
  }

  c$collect <- function(value) {
    #chprint(paste("collect identifier:",c$.info$identifier," - ", value))
    #chprint(paste("as type:",c$.info$types))
    if (c$.as_array) {
      #chprint("ArraySer")
      c$.serializer <- ArraySerializer(value)
    } else {
      #chprint("KeyValSer")
      c$.serializer <- KeyValuePairSerializer(value)
    }
    #chprint("initialized Serializer")
    c$collect <- c$.collect
    c$collect(value)
  }

  c$.collect <- function(value) {
    #chprint(paste(".collect ", value))
    #start <- as.numeric(Sys.time())
    serialized_value <- c$.serializer$serialize(value)
    #end <- as.numeric(Sys.time())
    #chprint(paste0("+,ser result -> ser value,",toString(end-start),","))
    
    #chprint(paste("serialized value ", serialized_value))
    #start <- as.numeric(Sys.time())
    c$.con$write(serialized_value)
    #end <- as.numeric(Sys.time())
    #chprint(paste0("+,ser result -> write ser,",toString(end-start),","))
  }

  class(c) <- "Collector"
  return(c)
}
