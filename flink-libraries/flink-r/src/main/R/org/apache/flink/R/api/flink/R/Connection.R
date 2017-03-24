Connection <- function(port)
{
  nc <- new.env()
  nc$port <- port
  nc$con  <- socketConnection(host="localhost", port = port, blocking=TRUE,
                            server=FALSE, open="wb")
  

  nc$write <- function(msg) {
    writeChar(msg, nc$con)
  }

  nc$read <- function(size) {
    readBin(nc$con, raw(), n = size)
  }

  nc$close_con <- function() {

    while(isOpen(nc$con)) {
      readBin(nc$con, raw(), n=1)
    }
    #print("close connection")
    close(nc$con)
  }

  nc$get <- function() {
    nc$con
  }

  #chprint("connected")

  ## Set the name for the class
  class(nc) <- "Connection"
  return(nc)
}

recv_all <- function(con, toread) {
  #chprint("begin recv")
  initial <- readBin(con, raw(), n = toread, endian="big", signed = FALSE)
  bytes_read = length(initial)
  if (bytes_read == toread) {
    #chprint("end initial recv")
    return(initial)
  } else {
    bits <- list(initial)
    toread <- toread - bytes_read
    while (toread > 0) {
      bit = readBin(con, raw(), n = toread)
      bits[[length(bits)+1]] <- bit
      toread = toread - length(bit)
    }
    #chprint("end recv")
    return(paste(bits, sep = '', collapse = ''))
  }
}

MAPPED_FILE_SIZE <- 1024 * 1024 * 64

SIGNAL_BUFFER_REQUEST <- 0
SIGNAL_BUFFER_REQUEST_G0 <- -3
SIGNAL_BUFFER_REQUEST_G1 <- -4
SIGNAL_FINISHED <- -1
SIGNAL_LAST <- 32


TCPMappedFileConnection <- function(input_file, output_file, port) {
  library(mmap)
  library(pack)

  c <- new.env()
  c$input <- input_file
  c$output <- output_file
  c$.file_input_buffer <- mmap(input_file, mode = char(), len = MAPPED_FILE_SIZE,
                              prot = mmapFlags("PROT_READ"),
                              flags= mmapFlags("MAP_SHARED"))
  c$.file_output_buffer <- mmap(output_file, mode = char(), len = MAPPED_FILE_SIZE,
                               prot = mmapFlags("PROT_WRITE"),
                               flags= mmapFlags("MAP_SHARED"))
  c$port <- port
  c$con <- Connection(port)

  c$.out <- vector(mode="raw", length=MAPPED_FILE_SIZE)
  c$.out_size <- 0

  c$.input <- ""
  c$.input_offset <- 0
  c$.input_size <- 0
  c$.was_last <- FALSE
  c$.part <- 0

  c$close <- function() {
    c$con$close_con()
  }

  c$.out_append <- function(o, size) {
    s <- c$.out_size+1
    e <- c$.out_size+size
    c$.out[s:e] <- o
  }

  c$write <- function(msg) {
    #chprint(paste("con write ",msg))
    len <- length(unlist(msg))
    if (len > MAPPED_FILE_SIZE) {
      stop("Serialized object does not fit into a single buffer.")
    }
    tmp <- c$.out_size + len
    if (tmp > MAPPED_FILE_SIZE) {
      #start <- as.numeric(Sys.time())
      c$.write_buffer()
      #end <- as.numeric(Sys.time())
      #chprint(paste0("+,ser result -> write ser -> .write_buffer,",toString(end-start),","))
      
      c$write(msg)
    } else {
      #start <- as.numeric(Sys.time())
      c$.out_append(msg, len)
      #end <- as.numeric(Sys.time())
      #chprint(paste0("+,ser result -> write ser -> .out_append,",toString(end-start),","))
      
      c$.out_size <- tmp
    }
    #chprint(paste0("out_size ",c$.out_size))
  }

  c$.write_buffer <- function() {
    #chprint("write buffer")
    #start <- as.numeric(Sys.time())
    #c$.file_output_buffer[1]
    #chprint(paste("mapped file size", MAPPED_FILE_SIZE))
    #chprint(paste("c.out class", class(c$.out)))
    if (class(c$.out) == "list") {
      c$.out <- unlist(c$.out)
    }
    
    #chprint("c.out ")
    #print(c$.out)
    #chprint("write to file")
    c$.file_output_buffer[1:c$.out_size] <- c$.out[1:c$.out_size]
    #chprint("wrote to file")
    #chprint(c$.out[1:c$.out_size+1])
    
    #chprint(paste(".out_size: ",c$.out_size))
    #writeBin(as.integer(c$.out_size), c$con$get(), size=4, endian="big")
    writeInt(c$con$get(), c$.out_size)
    #chprint("wrote size")
    c$.out <- vector(mode="raw", length=MAPPED_FILE_SIZE)
    c$.out_size <- 0
    #chprint("cleared out vec")
    recv_all(c$con$get(), 1)
    
    #end <- as.numeric(Sys.time())
    #chprint("end write buffer")
    #chprint(paste0("+,write buffer,",toString(end-start),","))
  }

  c$read <- function(des_size) {
    #chprint(paste0("des_size: ", des_size))
    if (c$.input_size == c$.input_offset) {
      c$.read_buffer()
    }
    old_offset <- c$.input_offset + 1
    c$.input_offset <- c$.input_offset + des_size
    #chprint(paste0("input offset:",c$.input_offset," !<= input size:",c$.input_size))
    if (c$.input_offset > c$.input_size) {
      #chprint("BufferUnderflowException")
      stop("BufferUnderFlowException")
    }
    #chprint("fin read")
    result <- c$.input[old_offset:c$.input_offset]
    #chprint(paste("result ",old_offset,":",c$.input_offset," :: ", result))
    return(result)
  }

  c$.read_buffer <- function() {
    #writeBin(SIGNAL_BUFFER_REQUEST, c$con$get(), size=4, endian="big")
    writeInt(c$con$get(), SIGNAL_BUFFER_REQUEST)
    c$.file_input_buffer[1]
    c$.input_offset <- 0
    
    if (c$.part == 0) {
      testin <- readBin(c$con$get(), raw(), n = 4)
      #chprint(paste0("testin: ", testin))
      test <- rawToNum(testin)
      #chprint(paste0("test: ", test))
    }
    c$.part <- c$.part + 1 
    meta_size <- recv_all(c$con$get(), 5)
    #chprint(meta_size)
    #chprint(meta_size[1:4])
    c$.input_size <- readBin(meta_size[1:4], integer(), n = 4, endian = "big", signed = FALSE)
    #chprint(paste0("in_size: ", c$.input_size))
    c$.was_last <- meta_size[5] == SIGNAL_LAST
    #chprint(paste0("was last: ", c$.was_last))
    if (c$.input_size > 0) {
      if (c$.input_size > MAPPED_FILE_SIZE) {
        c$.input <- c$.file_input_buffer[1:MAPPED_FILE_SIZE]
      } else {
        c$.input <- c$.file_input_buffer[1:c$.input_size]
      }
      #chprint(".input: ")
      #print(c$.input)
    }
    #chprint("fin read_buffer")
  }

  c$send_end_signal <- function() {
    if (c$.out_size > 0) {
      #chprint("write out buffer")
      c$.write_buffer()
    }
    #chprint("send signal finished")
    #writeBin(SIGNAL_FINISHED, c$con$get(), size=4, endian="big")
    writeInt(c$con$get(),SIGNAL_FINISHED)
  }

  c$has_next <- function(group = NA) {
    has <- c$.was_last == FALSE | c$.input_size != c$.input_offset
    #chprint(paste("has next: ", has))
    return(has)
  }

  c$reset <- function() {
    c$.was_last <- FALSE
    c$.input_size <- 0
    c$.input_offset <- 0
    c$.input = ""
  }

  c$read_secondary <- function(des_size) {
    return(recv_all(c$con, des_size))
  }

  c$write_secondary <- function(data) {
    writeBin(data, c$con, endian = "big")
  }

  c$get <- function() {
    return(c$con$get())
  }

  class(c) <- "TCPMappedFileConnection"
  return(c)
}
