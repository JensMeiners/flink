PlanIterator <- function(connection)
{
  nc <- new.env()
  nc$connection <- connection$get()

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
          chprint(paste("got val: ",val,"class", class(val)))
          return(val)
        } else if (type == TYPE_KEY_VALUE){
          chprint("iter type key val")
          size <- rawToNum(read(1))
          chprint(paste("got size", size))
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
          return(list(keys, val))
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

GroupIterator <- function(iterator, keys=NULL) {
  c <- new.env()
  c$iterator <- iterator
  c$key <- NULL
  c$keys <- keys
  chprint(paste("GroupIter keys", keys))
  if (is.null(keys)) {
    c$.extract_keys <- c$.extract_keys_id
  }
  c$cur <- NULL
  c$empty <- FALSE
  
  c$.init <- function() {
    chprint("init group iterator")
    if (c$iterator$has_next()) {
      c$empty <- FALSE
      c$cur <- c$iterator$nxt()
      c$key <- c$.extract_keys(c$cur)
    } else {
      c$empty <- TRUE
    }
    chprint("finished init")
  }
  
  c$nxt <- function() {
    if (c$has_next()) {
      tmp <- c$cur
      chprint(paste("GroupIter_tmp", tmp, class(tmp)))
      if (c$iterator$has_next()) {
        chprint("GroupIter_has_next")
        c$cur <- c$iterator$nxt()
        if (c$key[[1]] != c$.extract_keys(c$cur)[[1]]) {
          chprint("GroupIter_isEmpty")
          c$empty <- TRUE
        }
      } else {
        chprint("GroupIter_not_has_next")
        c$cur <- NULL
        c$empty <- TRUE
      }
      return(tmp[[2]])
    } else {
      stop("GroupIterator is empty")
    }
  }
  
  c$has_next <- function() {
    if (c$empty) {
      return(FALSE)
    }
    chprint(paste("groupiter.has_next key", c$key, class(c$key)))
    chprint(paste("groupiter.has_next extract_key", c$.extract_keys(c$cur), class(c$.extract_keys(c$cur))))
    return(c$key[[1]] == c$.extract_keys(c$cur)[[1]])
  }
  
  c$has_group <- function() {
    return(is.null(c$cur) == FALSE)
  }
  
  c$next_group <- function() {
    c$key <- c$.extract_keys(c$cur)
    c$empty <- FALSE
  }
  
  c$.extract_keys <- function(x) {
    return(lapply(c$keys, function(k) x[[1]][[k+1]]))
  }
  
  c$.extract_keys_id <- function(x) {
    return(x)
  }
  
  class(c) <- "GroupIterator"
  return(c)
}
