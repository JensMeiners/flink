library(flink)

print("filter")

filterFunction <- function(elem) {
    if (elem < 5) {
        return(TRUE);
    } else {
        return(FALSE);
    }
}

input <- 1:10
f <- flink.filter(input, filterFunction)
res <- flink.collect(f)
print(res)
