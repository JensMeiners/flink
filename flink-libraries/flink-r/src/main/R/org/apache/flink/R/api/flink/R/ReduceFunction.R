ReduceFunction <- function(mapFunction, reduceFunction, composition)
{
  c <- list(
    .mapFunction = mapFunction,
    .reduceFunction = reduceFunction,
    .composition = composition
  )

  c$reduce <- function(operator) {
    print("ReduceFunction.reduce()")
  }

  ## Set the name for the class
  class(c) <- append(class(c),"ReduceFunction")
  return(c)
}
