
.flinkREnv <- new.env()
options(showErrorCalls = TRUE, show.error.locations= TRUE)
# util
.flinkREnv$counter <- as.integer(0)
increment_counter <- function() {
  .flinkREnv$counter <- .flinkREnv$counter + 1
}

# parameters
.flinkREnv$dop <- as.integer(-1)
.flinkREnv$local_mode <- TRUE
.flinkREnv$retry <- as.integer(0)
.flinkREnv$output_path <- "./output"

# sets
.flinkREnv$sources <- list()
.flinkREnv$sets <- list()
.flinkREnv$sinks <- list()

# specials
.flinkREnv$broadcast <- list()

flink.setParallelism <- function(parallelism) {
  print(paste0("set parallelism: ", parallelism))
  .flinkREnv$dop <- as.integer(parallelism)
}
flink.parallelism <- function(parallelism) {
  flink.setParallelism(parallelism)
}
flink.getParallelism <- function() {
  .flinkREnv$dop
}

.sources_append <- function(source) {
  .flinkREnv$sources[[length(.flinkREnv$sources)+1]] <- source
}

.sets_append <- function(set) {
  .flinkREnv$sets[[length(.flinkREnv$sets)+1]] <- set
}

.sinks_append <- function(sink) {
  .flinkREnv$sinks[[length(.flinkREnv$sinks)+1]] <- sink
}

flink.from_elements <- function(input) {
  child <- newOperationInfo()
  child_set <- DataSet(child)
  child$identifier <- Identifier.SOURCE_VALUE
  child$values <- input
  .sources_append(child)
  return(child_set)
}

flink.read_csv <- function(path, types, line_delimiter="\n", field_delimiter=",") {
  child <- newOperationInfo()
  child_set <- DataSet(child)
  child$identifier <- Identifier.SOURCE_CSV
  child$delimiter_line <- line_delimiter
  child$delimiter_field <- field_delimiter
  child$path <- path
  child$types <- types
  .sources_append(child)
  return(child_set)
}

flink.read_text <- function(path) {
  child <- newOperationInfo()
  child_set <- DataSet(child)
  child$identifier <- Identifier.SOURCE_TEXT
  child$path <- path
  .sources_append(child)
  return(child_set)
}

flink.map <- function(input, mapFunction, local = TRUE) {
  .flinkREnv$local_mode <- local
  data <- flink.from_elements(input)
  data$map(mapFunction)
}

flink.reduce <- function(mapFunction, reduceFunction, composition = TRUE) {
  if (inherits(mapFunction, "DataSet") == FALSE) {
    stop("mapFunction must be an Operator or DataSet")
  }
  mapFunction$reduce(reduceFunction)
}

flink.filter <- function(input, filterFunction) {
  data <- flink.from_elements(input)
  data$filter(filterFunction)
}

flink.flatmap <- function(input, flatmapFunction) {
  data <- flink.from_elements(input)
  data$flatmap(flatmapFunction)
}

flink.groupBy <- function(dataset, key){
  #chprint(paste("groupBy key",key))
  dataset$groupBy(key)
}

#flink.group_reduce <- function(grouping, reduce_function) {
#  grouping$reduce_group(reduce_function, FALSE)
#}

.flinkREnv$runID <- sample(10:99, 1)

chprint <- function(s) {
  #print(paste0("[",.flinkREnv$runID,"] ",s))
}

# USED ONLY FOR DEBUGGING
flink.collect <- function(func, local=TRUE) {
  result <- tryCatch({
    .flink.collect(func, local)
  }, error = function(err) {
    #chprint(paste("ERROR:  ",err))
  })
  return(result)
}

.flink.collect <- function(func, local=TRUE) {
  .flinkREnv$runID <- sample(100:999, 1)

  #chprint("collect")
  if (length(.flinkREnv$sinks) == 0) {
    func$csv_output()
  }
  # Triggers program execution
  .flinkREnv$local_mode <- local
  # optimize plan
  .optimize_plan()
  #chprint("optimized plan:")
  for (set in .flinkREnv$sets) {
    #chprint(paste("operator", set$name, "identifier", set$identifier))
  }
  # start plan mode
  f <- file("stdin")
  open(f)
  # waiting for std. input
  if (length(line <- readLines(f,n=1)) > 0) {
    print(paste("read new line: ", line))
    # process std. input
    plan_mode <- line == "plan"

    if (plan_mode) {
      port <- readLines(f, n=1)
      print(paste0("received port: ", port))
      .flinkREnv$conn <- Connection(port)
      .flinkREnv$iter <- PlanIterator(.flinkREnv$conn)
      .flinkREnv$coll <- PlanCollector(.flinkREnv$conn)
      .send_plan()
      result <- .receive_result()
      #chprint(paste("net_runlength:", result$net_runtime))
      .flinkREnv$conn$close_con()
      return(result)
    } else {
      out <- tryCatch(
        {
          port <- readLines(f, n=1)

          id <- readLines(f, n=1)
          print(paste0("id: ",id))
          subtask_index <- readLines(f, n=1)
          input_path <- readLines(f, n=1)
          output_path <- readLines(f, n=1)

          used_set <- NA
          operator <- NA
          for (set in .flinkREnv$sets) {
            if (set$id == id) {
              used_set <- set
              operator <- set$operator
            }
          }
          #chprint(paste0("ACTIVE SET:", used_set$name, "(", used_set$id,")"))
          #chprint(paste0("operator .configure: ", class(operator)))
          #operator$.configure(input_path, output_path, port, used_set, subtask_index)
          operator <- configure(operator, input_path, output_path, port, used_set, subtask_index)

          #chprint("operator .run")
          #operator$.run()
          run(operator)

          #chprint("operator .close")
          operator$.close()
          #chprint("console .flush")
          flush.console()
        },
        error=function(cond) {
          stop(cond)
          flush.console()
          conn <- NA
          if (exists("port")) {
            if (exists("operator")) {
              conn <- operator$.get_connection()
            } else {
              conn <- Connection(port)
            }
            writeInt(conn$get(), -2)
          } else {
            stop("could not read port for operator communication with flink.")
          }
        })

    }
    #chprint("end line")
  }
  #chprint("closing script")
  close(f)
}

#############################################


.optimize_plan <- function(){
  .find_chains()
}

.find_chains <- function() {
  chainable <- list(Identifier.MAP, Identifier.FILTER, Identifier.FLATMAP)
  dual_input <- list(Identifier.JOIN, Identifier.JOINH, Identifier.JOINT, Identifier.CROSS, Identifier.CROSSH, Identifier.CROSST, Identifier.COGROUP, Identifier.UNION)
  x <- length(.flinkREnv$sets)
  while (x > 0) {
    # CHAIN(parent -> child) -> grand_child
    # for all intents and purposes the child set ceases to exist; it is merged into the parent
    child <- .flinkREnv$sets[[x]]
    child_type <- child$identifier
    #chprint(paste("optimize", child_type))
    if (child_type %in% chainable) {
      #chprint("is chainable")
      parent <- child$parent
      # we can only chain to an actual python udf (=> operator is not None)
      # we may only chain if the parent has only 1 child
      # we may only chain if the parent is not used as a broadcast variable
      # we may only chain if the parent does not use the child as a broadcast variable
      if (is.null(parent$operator) == FALSE 
          & length(parent$children) == 1 
          & length(parent$sinks) == 0 
          & (parent$id %in% sapply(.flinkREnv$broadcast, function(x) x$id)) == FALSE 
          & (child$id %in% sapply(parent$bcvars, function(x) x$id)) == FALSE) {
        parent$chained_info <- child
        parent$name <- paste(parent$name,"->",child$name)
        #chprint(paste("child.types:", child$types))
        parent$types <- child$types
        # grand_children now belong to the parent
        for (grand_child in child$children) {
          #chprint(paste("grand_child", grand_child$id))
          # dual_input operations have 2 parents; hence we have to change the correct one
          if (grand_child$identifier %in% dual_input) {
            if (grand_child$parent$id == child$id) {
              grand_child$parent <- parent
            } else {
              grand_child$other <- parent
            }
          } else {
            grand_child$parent <- parent
          }
          parent$children[[length(parent$children) + 1]] <- grand_child
        }
        # if child is used as a broadcast variable the parent must now be used instead
        #chprint("check broadcasts")
        for (s in .flinkREnv$sets) {
          if (child$id %in% sapply(s$bcvars, function(x) x$id)) {
            s$bcvars <- s$bcvars[sapply(s$bcvars, function(x) x$id) != child$id]
            s$bcvars[[length(s$bcvars) + 1]] <- parent
          }
        }
        for (bcvar in .flinkREnv$broadcast) {
          if (bcvar$other$id == child$id) {
            bcvar$other <- parent
          }
        }
        # child sinks now belong to the parent
        #chprint("check sinks")
        for (sink in child$sinks) {
          sink$parent <- parent
          parent$sinks[[length(parent$sinks) + 1]] <- sink
        }
        # child broadcast variables now belong to the parent
        #chprint("rewire bradcast vars")
        for (bcvar in child$bcvars) {
          bcvar$parent <- parent
          parent$bcvars[[length(parent$bcvars) + 1]] <- bcvar
        }
        # remove child set as it has been merged into the parent
        #chprint("remove child")
        parent$children <- parent$children[sapply(parent$children, function(x) x$id) != child$id]
        .remove_set(child)
      }
    }
  x <- x - 1
  }
}

.remove_set <- function(set){
  ### TODO
  .flinkREnv$sets <- .flinkREnv$sets[sapply(.flinkREnv$sets, function(x) x$id) != set$id]
}
#############################################
.send_plan <- function() {
  print("send plan")
  .send_parameters()
  .send_operations()
}

.send_parameters <- function() {
  print("send parameters")
  collect <- .flinkREnv$coll$collect
  print("send dop")
  collect(list("dop", .flinkREnv$dop))
  print("send mode")
  collect(list("mode", .flinkREnv$local_mode))
  print("send retry")
  collect(list("retry", .flinkREnv$retry))
}

.send_operations <- function() {
  print("send operations")
  collect <- .flinkREnv$coll$collect
  collect(length(.flinkREnv$sources)+length(.flinkREnv$sets)
          +length(.flinkREnv$sinks)+length(.flinkREnv$broadcast))
  for(source in .flinkREnv$sources) {
    #chprint("send source")
    .send_operation(source)
  }
  for(set in .flinkREnv$sets) {
    #chprint("send set")
    .send_operation(set)
  }
  for(sink in .flinkREnv$sinks) {
    #chprint("send sink")
    .send_operation(sink)
  }
  for(bc in .flinkREnv$broadcast) {
    #chprint("send broadcast")
    .send_operation(bc)
  }
}

.send_operation <- function(op) {
  collect <- .flinkREnv$coll$collect
  collect(op$identifier)
  #chprint(paste(">>> identifier:",op$identifier))
  if (is.null(op$parent)) {
    collect(as.integer(-1))
  } else {
    #chprint(paste("parentID",op$parent$id))
    collect(as.integer(op$parent$id))
  }
  if (is.null(op$other)) {
    collect(as.integer(-1))
  } else {
    #chprint(paste("otherID",op$other$id))
    collect(as.integer(op$other$id))
  }
  collect(op$field)
  collect(op$order)
  #chprint(paste("keys",op$keys))
  collect(op$keys)
  #chprint(paste("key1",op$key1))
  collect(op$key1)
  #chprint(paste("key2",op$key2))
  collect(op$key2)
  #chprint(paste("types",op$types))
  collect(op$types)
  #chprint(paste("udf",op$uses_udf))
  collect(op$uses_udf)
  collect(op$name)
  collect(op$delimiter_line)
  collect(op$delimiter_field)
  collect(op$write_mode)
  collect(op$path)
  collect(op$frm)
  collect(op$to)
  #chprint(paste("id",op$id))
  collect(op$id)
  collect(op$to_err)
  collect(op$count)
  collect(length(op$values))
  for(value in op$values) {
    collect(value)
  }
  collect(op$parallelism)
}

.receive_result <- function() {
  print("receive result")
  JobExecutionResult(.flinkREnv$iter$nxt())
}
