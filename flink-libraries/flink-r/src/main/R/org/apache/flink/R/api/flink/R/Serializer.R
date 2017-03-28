serializer.write_type_info <- function(val, con) {
  type <- serializer.getType(val)
  #chprint(paste("type",type,"val",val))
  if (type == "list") {
    #chprint(paste("list len", length(val)))
    writeBin(as.raw(length(val)), con)
    for(v in val) {
      serializer.write_type_info(v, con)
    }
  } else if (type == "array") {
    serializer.writeType("array", con)
    #writeInt(con, length(val))
  } else {
    serializer.writeType(type, con)
  }
}

serializer.writeType <- function(class, con) {
  type <- switch(class,
                 NULL = 26,
                 integer = 32,
                 character = 28,
                 logical = 34,
                 double = 30,
                 numeric = 29,
                 raw = 33,
                 array = 27,
                 matrix = 27,
                 #list = "l",
                 #struct = "s",
                 #jobj = "j",
                 #environment = "e",
                 #Date = "D",
                 #POSIXlt = "t",
                 #POSIXct = "t",
                 stop(paste("Unsupported type for serialization", class)))
  writeBin(as.raw(type), con)

  #writeBin(charToRaw(type), con)
}

serializer.getType <- function(object) {
  type <- class(object)[[1]]
  if (type != "list") {
    type
  } else {
    # Check if all elements are of same type
    #elemType <- unique(sapply(object, function(elem) { serializer.getType(elem) }))
    #if (length(elemType) <= 1) {
    #  "array"
    #} else {
    #  "list"
    #}
    type
  }
}

serializer.write_value <- function(object, con) {
  type <- serializer.getType(object)
  #print(paste("type: ", type))
  switch(type,
         NULL = writeVoid(con),
         integer = writeInt(con, object),
         character = writeString(con, object),
         logical = writeBoolean(con, object),
         double = writeDouble(con, object),
         numeric = writeDouble(con, object),
         raw = writeRaw(con, object),
         array = writeArray(con, object),
         list = writeList(con, object),
         matrix = writeArray(con, object),
         #struct = writeList(con, object),
         #jobj = writeJobj(con, object),
         #environment = writeEnv(con, object),
         #Date = writeDate(con, object),
         #POSIXlt = writeTime(con, object),
         #POSIXct = writeTime(con, object),
         stop(paste("Unsupported type for serialization", type)))
}

serializer.get_serializer <- function(value, c_types) {
  type <- serializer.getType(value)
  #chprint(paste("get_serializer for ", value, "type",type))
  result <- switch(type,
                   NULL = desNull,
                   "integer" = serializer.serLong,
                   "character" = serializer.serChar,
                   "logical" = desLogic,
                   "double" = desDouble,
                   "numeric" = serializer.serLong,
                   "raw" = desRaw,
                   "list" = serializer.serList(value)$serialize,
                   stop(paste("Unsupported type for serialization", type)))
  ##chprint(paste("got serializer result",result))
  return(result)
}

serializer.get_type_info <- function(value, c_types) {
  type <- serializer.getType(value)
  result <- switch(type,
                 NULL = "1A",
                 integer = "1F",
                 character = "1C",
                 logical = "22",
                 double = "1E",
                 numeric = "1F",
                 raw = "1B",
                 array = "3F",
                 list = return(serializer.get_type_info_list(value)),
                 stop(paste("Unsupported type for serialization", type)))
  return(as.raw(as.hexmode(result)))
}

serializer.get_type_info_list <- function(value) {
  types <- c()
  for (v in value) {
    types <- append(types, serializer.get_type_info(v))
  }
  return(c(serializer.serInt(length(value))[4],types))
}

KeyValuePairSerializer <- function(value, c_types) {
  c <- new.env()
  c$.typeK <- list()
  c$.serializerK <- list()
  for (key in value[[1]]){
    c$.typeK[[length(c$.typeK)+1]] <- serializer.get_type_info(key, c_types)
    c$.serializerK[[length(c$.serializerK)+1]] <- serializer.get_serializer(key, c_types)
  }
  c$.typeV <- serializer.get_type_info(value[[2]], c_types)
  c$.serializerV <- serializer.get_serializer(value[[2]], c_types)
  c$.typeK_length <- list()
  for (type in c$.typeK) {
    c$.typeK_length[[length(c$.typeK_length)+1]] <- length(c$.typeK)
  }
  c$.typeV_length <- length(c$.typeV)

  c$serialize <- function(value) {
    #chprint("KeyVal Serializer")
    size <- length(value[[1]])
    bits <- list(rev(serializer.intToRaw(size))[4])
    for (i in 1:size) {
      x <- c$.serializerK[[i]](value[[1]][i])
      bits[[length(bits)+1]] <- rev(serializer.intToRaw(length(x) + c$.typeK_length[[i]]))
      bits[[length(bits)+1]] <- c$.typeK[[i]]
      bits[[length(bits)+1]] <- x
    }
    v <- c$.serializerV(value[[2]])
    bits[[length(bits)+1]] <- rev(serializer.intToRaw(length(v) + c$.typeV_length))
    bits[[length(bits)+1]] <- c$.typeV
    bits[[length(bits)+1]] <- v
    return(bits)
  }

  class(c) <- "KeyValuePairSerializer"
  return(c)
}

ArraySerializer <- function(value, c_types) {
  ##chprint("constr ArraySer")
  c <- new.env()
  c$serialize <- function(value) {
    ser_value <- c$.serializer(value)
    size <- length(ser_value) + c$.type_length
    return(c(rev(packBits(intToBits(size), type="raw")), c$.type, ser_value))
  }
  c$.type <- serializer.get_type_info(value, c_types)
  c$.type_length <- length(c$.type)
  c$.serializer <- serializer.get_serializer(value, c_types)

  ##chprint("fin constr ArraySer")

  class(c) <- "ArraySerializer"
  return(c)
}

serializer.serList <- function(value) {
  c <- new.env()
  c$serializer <- list()
  for (v in value){
    c$serializer[[length(c$serializer)+1]] <- serializer.get_serializer(v, NULL)
  }
  c$serialize <- function(value) {
    #chprint(paste("listSer:", value))
    i <- 1
    result <- c()
    for (v in value) {
      #chprint(paste("val:",v))
      result <- append(result, c$serializer[[i]](v))
      i <- i+1
    }
    #chprint(paste("result", result))
    return(result)
  }
  return(c)
}

serializer.serLong <- function(value) {
  bytes <- raw(8)
  bytes[1:4] <- serializer.intToRaw(value)
  return(rev(bytes))
}

serializer.serInt <- function(value) {
  return(rev(serializer.intToRaw(value)))
}

serializer.intToRaw <- function(value) {
  return(packBits(intToBits(value), type="raw"))
}

serializer.serChar <- function(value) {
  #chprint(paste("serChar:",value))
  utfVal <- enc2utf8(value)
  return(c(serializer.serInt(nchar(utfVal)), charToRaw(utfVal)))
}

writeVoid <- function(con) {
  # no value for NULL
}

writeString <- function(con, value) {
  utfVal <- enc2utf8(value)
  writeInt(con, as.integer(nchar(utfVal, type = "bytes") + 1))
  writeBin(utfVal, con, endian = "big", useBytes = TRUE)
}

writeInt <- function(con, value) {
  writeBin(as.integer(value), con, endian = "big")
}

writeDouble <- function(con, value) {
  writeBin(value, con, endian = "big")
}

writeBoolean <- function(con, value) {
  # TRUE becomes 1, FALSE becomes 0
  writeInt(con, as.integer(value))
}

writeRaw <- function(con, batch) {
  #writeInt(con, length(batch))
  writeBin(batch, con, endian = "big")
}

int_to_uint <- function (x, adjustment=2^32) {
  x <- as.numeric(x)
  signs <- sign(x)
  x[signs < 0] <- x[signs < 0] + adjustment
  x
}

writeArray <- function(con, arr) {
  #size <- length(arr)
  writeInt(con, length(arr))
  #writeBin(size, con, size=4, useBytes = TRUE, endian = "big")
  #chprint(paste("arr:",arr,"class",class(arr)))
  for (val in arr) {
    #chprint(paste("val:",class(val)))
    writeBin(val, con, size=1, useBytes = TRUE)
  }
}

writeList <- function(con, list) {
  for (value in list) {
    #serializer.write_type_info(value, con)
    serializer.write_value(value, con)
  }
}

# Used to pass in hash maps required on Java side.
writeEnv <- function(con, env) {
  len <- length(env)

  writeInt(con, len)
  if (len > 0) {
    writeArray(con, as.list(ls(env)))
    vals <- lapply(ls(env), function(x) { env[[x]] })
    writeList(con, as.list(vals))
  }
}

writeDate <- function(con, date) {
  writeString(con, as.character(date))
}

writeTime <- function(con, time) {
  writeDouble(con, as.double(time))
}