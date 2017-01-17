Function <- function() {
  chprint("init Function")

  c <- list()

  c$.configure <- function(input_file, output_file, port, info, subtask_index) {
    chprint("Function .configure")
    chprint(paste0("context: ", class(c)))
    c$.conn <- TCPMappedFileConnection(input_file, output_file, port)
    c$.iterator <- Iterator(c$.conn)
    c$.collector <- Collector(c$.conn, info)
    c$context <- RuntimeContext(c$.iterator, c$.collector, subtask_index)
    if (is.null(info$chained_info) == FALSE) {
      info$chained_info$operator$.configure_chain(c$context, c$.collector, info$chained_info)
      c$.collector <- info$chained_info$operator
    }
    chprint(paste0("Function .configure scope: ",class(c)))
  }

  c$.configure_chain <- function(context, collector, info) {
    c$context <- context
    if (is.null(info$chained_info)) {
      c$.collector <- collector
    } else {
      c$.collector <- info$chained_info$operator
      info$chained_info$operator$.configure_chain(context, collector, info$chained_info)
    }
  }

  c$.close <- function() {
    if (is.null(c$.conn) == FALSE) {
      c$.conn$send_end_signal()
      c$.conn$close()
    }
  }

  c$.go <- function() {
    c$.run()
  }

  class(c) <- "Function"
  return(c)
}

configure <- function(obj, input_file, output_file, port, info, subtask_index) {
  UseMethod("configure", obj)
}

configure.Function <- function(obj, input_file, output_file, port, info, subtask_index) {
  chprint("Function .configure")
  obj$.conn <- TCPMappedFileConnection(input_file, output_file, port)
  obj$.iterator <- Iterator(obj$.conn)
  obj$.collector <- Collector(obj$.conn, info)
  obj$context <- RuntimeContext(obj$.iterator, obj$.collector, subtask_index)
  if (is.null(info$chained_info) == FALSE) {
    info$chained_info$operator$.configure_chain(obj$context, obj$.collector, info$chained_info)
    obj$.collector <- info$chained_info$operator
  }
  return(obj)
}
