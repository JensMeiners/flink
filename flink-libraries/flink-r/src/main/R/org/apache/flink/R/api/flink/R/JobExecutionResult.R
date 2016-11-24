JobExecutionResult <- function(net_runtime)
{
  nc <- list(
    net_runtime = net_runtime
  )

  ## Set the name for the class
  class(nc) <- "JobExecutionResult"
  return(nc)
}
