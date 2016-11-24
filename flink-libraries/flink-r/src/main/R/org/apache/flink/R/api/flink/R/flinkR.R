
.flinkREnv <- new.env()

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

# assign(".scStartTime", as.integer(Sys.time()), envir = .sparkREnv)
flink.getParallelism <- function() {
  .flinkREnv$dop
}


flink.map <- function(input, mapFunction, local) {
  MapFunction(input, mapFunction, local)
}

flink.reduce <- function(mapFunction, reduceFunction, composition) {
  ReduceFunction(mapFunction, reduceFunction, composition)
}


flink.collect <- function(f, local=TRUE) {
  print("collect")
  # Triggers program execution
  .flinkREnv$local_mode <- local
  # TODO: optimize plan
  f <- file("stdin")
  open(f)
  # waiting for std. input
  while(length(line <- readLines(f,n=1)) > 0) {
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
    }
  }
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
  collect <- .flinkREnv$coll$collect
  collect(op$identifier)
  if (is.null(op$parent))
    collect(-1)
  else
    collect(op$parent$id)
  if (is.null(op$other))
    collect(-1)
  else
    collect(op$other$id)
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
