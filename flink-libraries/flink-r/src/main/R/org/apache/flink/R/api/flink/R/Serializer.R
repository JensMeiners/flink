serializer.write_type_info <- function(val, con) {
  type <- serializer.getType(val)
  if (type == "list") {
    writeBin(as.raw(length(val)), con)
    for(v in val) {
      serializer.write_type_info(v, con)
    }
  } else if (type == "array") {
    serializer.writeType("array", con)
    writeInt(con, length(val))
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
  chprint(paste("get_serializer for ", value))
  result <- switch(type,
                   NULL = desNull,
                   "integer" = serializer.serLong,
                   "character" = serializer.serChar,
                   "logical" = desLogic,
                   "double" = desDouble,
                   "numeric" = serializer.serLong,
                   "raw" = desRaw,
                   #array = "3F",
                   stop(paste("Unsupported type for serialization", type)))
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
                 stop(paste("Unsupported type for serialization", type)))
  return(as.raw(as.hexmode(result)))
}

KeyValuePairSerializer <- function(value, c_types) {
  c <- new.env()
  c$.typeK <- list()
  c$.serializerK <- list()
  for (key in value[[0]]){
    c$.typeK[[length(c$.typeK)+1]] <- serializer.get_type_info(key, c_types)
    c$.serializerK[[length(c$.serializerK)+1]] <- serializer.get_serializer(key, c_types)
  }
  c$.typeV <- serializer.get_type_info(value[[1]], c_types)
  c$.serializerV <- serializer.get_serializer(value[[1]], c_types)
  c$.typeK_length <- list()
  for (type in c$.typeK) {
    c$.typeK_length[[length(c$.typeK_length)+1]] <- length(c$.typeK)
  }
  c$.typeV_length <- length(c$.typeV)

  c$serialize <- function(value) {
    chprint("KeyVal Serializer")
    size <- length(value[[0]])
    bits <- list(rev(numToRaw(size, nBytes = 4))[4])
    for (i in 1:length(x)) {
      x <- c$.serializerK[[i]](value[0][i])
      bits[[length(bits)+1]] <- rev(numToRaw(length(x) + c$.typeK_length[[i]], nBytes=4))
      bits[[length(bits)+1]] <- c$.typeK[[i]]
      bits[[length(bits)+1]] <- x
    }
    v <- c$.serializerV(value[[1]])
    bits[[length(bits)+1]] <- rev(numToRaw(length(v) + c$.typeV_length, nBytes=4))
    bits[[length(bits)+1]] <- c$.typeV
    bits[[length(bits)+1]] <- v
    return(bits)
  }

  class(c) <- "KeyValuePairSerializer"
  return(c)
}

ArraySerializer <- function(value, c_types) {
  #chprint("constr ArraySer")
  c <- new.env()
  c$serialize <- function(value) {
    ser_value <- c$.serializer(value)
    size <- length(ser_value) + c$.type_length
    return(c(rev(numToRaw(size, nBytes = 4)), c$.type, ser_value))
  }
  c$.type <- serializer.get_type_info(value, c_types)
  c$.type_length <- length(c$.type)
  c$.serializer <- serializer.get_serializer(value, c_types)

  #chprint("fin constr ArraySer")

  class(c) <- "ArraySerializer"
  return(c)
}

serializer.serLong <- function(value) {
  return(rev(numToRaw(value, nBytes = 8)))
}

serializer.serChar <- function(value) {
  chprint(paste("serChar ",value))
  utfVal <- enc2utf8(paste("   ", value))
  return(charToRaw(utfVal))
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


writeArray <- function(con, arr) {
  writeList(con, as.list(arr))
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
