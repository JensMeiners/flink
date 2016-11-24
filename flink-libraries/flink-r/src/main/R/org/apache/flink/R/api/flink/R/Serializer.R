serializer.write_type_info <- function(val, con) {
  type <- serializer.getType(val)
  if (type == "list") {
    writeBin(as.raw(length(val)), con)
    for(v in val) {
      serializer.write_type_info(v, con)
    }
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
                 #array = "a",
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
    elemType <- unique(sapply(object, function(elem) { serializer.getType(elem) }))
    if (length(elemType) <= 1) {
      "array"
    } else {
      "list"
    }
  }
}

serializer.write_value <- function(object, con) {
  type <- serializer.getType(object)
  print(paste("type: ", type))
  switch(type,
         NULL = writeVoid(con),
         integer = writeInt(con, object),
         character = writeString(con, object),
         logical = writeBoolean(con, object),
         double = writeDouble(con, object),
         numeric = writeDouble(con, object),
         raw = writeRaw(con, object),
         #array = writeArray(con, object),
         list = writeList(con, object),
         #struct = writeList(con, object),
         #jobj = writeJobj(con, object),
         #environment = writeEnv(con, object),
         #Date = writeDate(con, object),
         #POSIXlt = writeTime(con, object),
         #POSIXct = writeTime(con, object),
         stop(paste("Unsupported type for serialization", type)))
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
  writeInt(con, length(batch))
  writeBin(batch, con, endian = "big")
}


writeArray <- function(con, arr) {
  writeList(con, as.list(arr))
}

# Used to pass arrays where the elements can be of different types
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
