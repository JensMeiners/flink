Connection <- function(port)
{
  nc <- list(
    port = port,
    con = socketConnection(host="localhost", port = port, blocking=TRUE,
                            server=FALSE, open="wb")
  )

  nc$write <- function(msg) {
    writeChar(msg, nc$con)
  }

  nc$read <- function(size) {
    readBin(nc$con, "raw", n = size)
  }

  nc$close_con <- function() {
    print("close connection")
    close(nc$con)
  }

  nc$get <- function() {
    nc$con
  }

  print("connected")

  ## Set the name for the class
  class(nc) <- "Connection"
  return(nc)
}
