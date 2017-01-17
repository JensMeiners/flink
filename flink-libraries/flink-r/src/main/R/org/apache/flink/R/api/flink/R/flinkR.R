
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

# sets
.flinkREnv$sources <- list()
.flinkREnv$sets <- list()
.flinkREnv$sinks <- list()

# specials
.flinkREnv$broadcast <- list()

flink.setParallelism <- function(parallalism) {
  print(paste0("set parallelism: ", parallalism))
  .flinkREnv$dop <- as.integer(parallalism)
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

.flinkREnv$runID <- sample(10:99, 1)

chprint <- function(s) {
  print(paste0("[",.flinkREnv$runID,"] ",s))
}

flink.collect <- function(func, local=TRUE) {
  .flinkREnv$runID <- sample(100:999, 1)

  chprint("collect")
  if (length(.flinkREnv$sinks) == 0) {
    func$output()
  }
  # Triggers program execution
  .flinkREnv$local_mode <- local
  # TODO: optimize plan
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
          chprint(paste0("operator .configure: ", class(operator)))
          #operator$.configure(input_path, output_path, port, used_set, subtask_index)
          operator <- configure(operator, input_path, output_path, port, used_set, subtask_index)

          chprint("operator .run")
          #operator$.run()
          run(operator)

          chprint("operator .close")
          operator$.close()
          chprint("console .flush")
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
    chprint("end line")
  }
  chprint("closing script")
  close(f)
}

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
    .send_operation(source)
  }
  for(set in .flinkREnv$sets) {
    .send_operation(set)
  }
  for(sink in .flinkREnv$sinks) {
    .send_operation(sink)
  }
  for(bc in .flinkREnv$broadcast) {
    .send_operation(bc)
  }
}

.send_operation <- function(op) {
  print("send operation")
  collect <- .flinkREnv$coll$collect
  collect(op$identifier)
  if (is.null(op$parent))
    collect(as.integer(-1))
  else
    collect(as.integer(op$parent$id))
  if (is.null(op$other))
    collect(as.integer(-1))
  else
    collect(as.integer(op$other$id))
  collect(op$field)
  collect(op$order)
  collect(op$keys)
  collect(op$key1)
  collect(op$key2)
  collect(op$types)
  collect(op$uses_udf)
  collect(op$name)
  collect(op$delimiter_line)
  collect(op$delimiter_field)
  collect(op$write_mode)
  collect(op$path)
  collect(op$frm)
  collect(op$to)
  #chprint(paste("send id: ", op$id))
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
