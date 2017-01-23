print("start R script")

library(flink)

print("found flink library")

## Flink connection - setup the number of threads
x <- flink.setParallelism(4)

if (4 == x) {
    print("OK")
} else {
    print("KO")
}



mapFunction <- function(x) {
    return(x + 100)
}

reduceFunction <- function(x, y) {
    x + y
}

input <- 1:100

start <- proc.time()

m <- flink.map(input, mapFunction, local=TRUE)
f <- flink.reduce(m, reduceFunction, composition=TRUE)
result <- flink.collect(f)

proc.time() - start

print(paste(">>>",result))
