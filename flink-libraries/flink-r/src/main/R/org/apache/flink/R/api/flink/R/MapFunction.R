MapFunction <- function(input, mapFunction, local)
{
  nc <- list(
    .input = input,
    .mapFunction = mapFunction,
    .local = local
  )

  nc$map <- function(operator) {
    print("MapFunction.map()")
  }

  ## Set the name for the class
  #class(c) <- append(class(c),"MapFunction")
  #return(c)
  #nc <- list2env(nc)
  class(nc) <- "MapFunction"
  return(nc)
}

#map <- function(elObjeto, newValue)
#{
#  print("Calling the base map function")
#  UseMethod("map",elObjeto)
#}

#map.MapFunction <- function(elObjeto, newValue)
#{
#  print("In map.MapFunction and setting the value")
#  elObjeto$map <- newValue
#  return(elObjeto)
#}
