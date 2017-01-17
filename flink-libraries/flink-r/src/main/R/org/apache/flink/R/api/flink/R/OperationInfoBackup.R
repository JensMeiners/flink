#OperationInfo <- function()
#{
#  nc <- list(
#    #fields being transferred to the java side
#    identifier = as.integer(-1),
#    parent = NULL,
#    other = NULL,
#    field = as.integer(-1),
#    order =  as.integer(0),
#    keys = list(),
#    key1 = list(),
#    key2 = list(),
#    types = NULL,
#    uses_udf = FALSE,
#    name = NULL,
#    delimiter_line = "\n",
#    delimiter_field = ",",
#    write_mode = WriteMode.NO_OVERWRITE,
#    path = "",
#    frm =  as.integer(0),
#    to =  as.integer(0),
#    count =  as.integer(0),
#    values = list(),
#    projections = list(),
#    id =  as.integer(-1),
#    to_err = FALSE,
#    parallelism =  as.integer(-1),
#    #internally used
#    parent_set = NULL,
#    other_set = NULL,
#    chained_info = NULL,
#    bcvars = list(),
#    sinks = list(),
#    children = list(),
#    operator = NULL
#  )

#  nc$children_append <- function(child) {
#    nc$children[[length(nc$children)+1]] <- child
#  }

#  nc$sinks_append <- function(sink) {
#    nc$sinks[[length(nc$sinks)+1]] <- sink
#  }

#  class(nc) <- "OperationInfo"
#  return(nc)
#}
