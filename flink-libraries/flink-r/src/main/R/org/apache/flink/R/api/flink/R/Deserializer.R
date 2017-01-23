deserializer.desInt <- function(read) {
  chprint("des Int")
  val <- read(4)
  chprint(paste("desInt: ", val))
  readInt(val)
}
deserializer.desLong <- function(read) {
  chprint("des Long")
  val <- read(8)
  chprint(paste("desLong: ", val))
  readLong(val)
}

deserializer.desString <- function(read) {
  chprint("des String")
  size <- readInt(read(4))
  val <- enc2utf8(rawToChar(read(size)))
  return(val)
}


.get_deserializer <- function(read) {
  library(pack)
  chprint("get_deserializer")
  #type <- as.integer(readBin(con, "raw", n = 1L))
  type <- read(1)
  chprint(paste("des type: ", type))
  
  ord <- rawToNum(type)
  if (ord < 26) {
    return(deserializer.desList(ord, read)$deserialize)
  }
  
  switch (toString(type),
          "20" = deserializer.desInt,
          "28" = deserializer.desString,
          "1c" = deserializer.desString,
          "34" = desBoolean,
          "30" = desDouble,
          "33" = desRaw,
          "1f" = deserializer.desLong,
          #"t" = readTime,
          "3f" = .get_deserializer(read),
          #"l" = readList,
          #"e" = readEnv,
          #"s" = readStruct,
          "26" = NULL,
          #"j" = getJobj,
          stop(paste("Type not supported for deserialization: ", type)))

}

deserializer.desList <- function(num, read) {
  chprint("deserialize List")
  c <- new.env()
  c$deserializer <- list()
  for (v in 1:num){
    c$deserializer[[length(c$deserializer)+1]] <- .get_deserializer(read)
  }
  c$.skip <- 0
  c$deserialize <- function(read) {
    chprint(paste("listDeser"))
    if (c$.skip > 0) {
      read(c$.skip) # skip type info
    } else {
      c$.skip <- length(c$deserializer)
    }
    i <- 1
    result <- c()
    for (i in 1:length(c$deserializer)) {
      result <- append(result, c$deserializer[[i]](read))
      i <- i+1
    }
    chprint(paste("result", result))
    return(result)
  }
  return(c)
}

ArrayDeserializer <- function(deserializer) {
  c <- new.env()
  c$des <- deserializer
  c$deserialize <- function(read) {
    read(2) # array and element type
    return(c$des(read))
  }
  class(c) <- "ArrayDeserializer"
  return(c)
}

KeyValueDeserializer <- function(key_des, val_des) {
  c <- list(k_des = key_des, v_des = val_des)
  c$deserialize <- function(read) {
    fields <- list()
    read(2)
    for (des in c$k_des) {
      read(1)
      fields[[length(fields)+1]] <- des(read)
    }
    read(1)
    return(c(fields, c$v_des(read)))
  }
  class(c) <- "KeyValueDeserializer"
  return(c)
}



readInt <- function(con) {
  readBin(con, integer(), n = 4, endian = "big")
}

readLong <- function(con) {
  readBin(con, integer(), n = 8, endian = "big")[[2]]
}

readString <- function(con) {
  stringLen <- readInt(con)
  raw <- readBin(con, raw(), stringLen, endian = "big")
  string <- rawToChar(raw)
  Encoding(string) <- "UTF-8"
  string
}

readBoolean <- function(con) {
  as.logical(readInt(con))
}

readDouble <- function(con) {
  readBin(con, double(), n = 1, endian = "big")
}

readRaw <- function(con) {
  dataLen <- readInt(con)
  readBin(con, raw(), as.integer(dataLen), endian = "big")
}

readDate <- function(con) {
  as.Date(readString(con))
}

readTime <- function(con) {
  t <- readDouble(con)
  as.POSIXct(t, origin = "1970-01-01")
}

readArray <- function(con) {
  type <- readType(con)
  len <- readInt(con)
  if (len > 0) {
    l <- vector("list", len)
    for (i in 1:len) {
      l[[i]] <- readTypedObject(con, type)
    }
    l
  } else {
    list()
  }
}

# Read a list. Types of each element may be different.
# Null objects are read as NA.
readList <- function(con) {
  len <- readInt(con)
  if (len > 0) {
    l <- vector("list", len)
    for (i in 1:len) {
      elem <- readObject(con)
      if (is.null(elem)) {
        elem <- NA
      }
      l[[i]] <- elem
    }
    l
  } else {
    list()
  }
}

readEnv <- function(con) {
  env <- new.env()
  len <- readInt(con)
  if (len > 0) {
    for (i in 1:len) {
      key <- readString(con)
      value <- readObject(con)
      env[[key]] <- value
    }
  }
  env
}

# Read a field of StructType from SparkDataFrame
# into a named list in R whose class is "struct"
readStruct <- function(con) {
  names <- readObject(con)
  fields <- readObject(con)
  names(fields) <- names
  listToStruct(fields)
}
