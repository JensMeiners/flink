.get_deserializer <- function(con) {
  print("get_deserializer")
  #type <- as.integer(readBin(con, "raw", n = 1L))
  type <- readBin(con, integer(), n = 1L)
  print(paste("type: ", type))
  switch (type,
          32 = readInt,
          28 = readString,
          34 = readBoolean,
          30 = readDouble,
          33 = readRaw,
          #"D" = readDate,
          #"t" = readTime,
          #"a" = readArray,
          #"l" = readList,
          #"e" = readEnv,
          #"s" = readStruct,
          26 = NULL,
          #"j" = getJobj,
          stop(paste("Type not supported for deserialization: ", type)))
}

readInt <- function(con) {
  readBin(con, integer(), n = 1, endian = "big")
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
