PlanIterator <- function(connection)
{
  nc <- new.env()
  nc$connection <- connection

  nc$nxt <- function() {
    print("iterate plan")
    deser <- .get_deserializer(nc$connection$read)
    deser(nc$connection$read)
  }

  ## Set the name for the class
  class(nc) <- "PlanIterator"
  return(nc)
}

TYPE_ARRAY <- as.raw(63)
TYPE_KEY_VALUE <- as.raw(62)
TYPE_VALUE_VALUE <- as.raw(61)

Iterator <- function(con, group=0) {
  #c <- list(
  #  .con = con,
  #  .init = TRUE,
  #  .group = group,
  #  .deserializer = NULL,
  #  .size = 0
  #)
  c <- new.env()
  c$.con <- con
  c$.init <- TRUE
  c$.group <- group
  c$.deserializer <- NULL
  c$.size <- 0

  c$.read <- function(des_size) {
    chprint(paste0("c$.read ", des_size))
    return(c$.con$read(des_size))
  }

  c$nxt <- function() {
    chprint("iter nxt")
    if (c$has_next()) {
      read <- c$.read
      if (is.null(c$.deserializer)) {
        type <- read(1)
        chprint(paste0("got type ", type))
        if (type == TYPE_ARRAY) {
          chprint("iter type array")
          key_des <- .get_deserializer(read)
          chprint("got deserializer")
          c$.deserializer <- ArrayDeserializer(key_des)
          chprint("as array deserializer")
          val <- key_des(read)
          chprint(paste("got val: ",val))
          return(val)
        } else if (type == TYPE_KEY_VALUE){
          chprint("iter type key val")
          size <- read(1)
          key_des <- list()
          keys <- list()
          for (i in 1:size) {
            new_d <- .get_deserializer(read)
            key_des[[length(key_des)+1]] <- new_d
            keys[[length(keys)+1]] <- new_d(read)
          }
          val_des <- .get_deserializer(read)
          val <- val_des(read)
          c$.deserializer <- KeyValueDeserializer(key_des, val_des)
          return(c(keys, val))
        } else if (type == TYPE_VALUE_VALUE) {
          chprint("iter type val val")
          des1 <- .get_deserializer(read)
          field1 <- des1$deserialize(read)
          des2 <- .get_deserializer(read)
          field1 <- des2$deserialize(read)
          c$.deserializer <- ValueValueDeserializer(des1, des2)
          return(c(field1, field2))
        } else {
          chprint(paste0("invalid iter type: ", type))
          stop(paste0("Invalid type ID encountered: ",type))
        }
      }
      return(c$.deserializer$deserialize(c$.read))
    } else {
      stop("Stop Iteration")
    }
  }

  c$has_next <- function() {
    chprint("iterator has_next")
    return(c$.con$has_next(c$.group))
  }

  c$.reset <- function() {
    c$.deserializer = NULL
  }

  class(c) <- "Iterator"
  return(c)
}
