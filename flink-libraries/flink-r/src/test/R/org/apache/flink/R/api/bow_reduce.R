# flink.init()
library(flink)

print("bow_reduce")

flink.parallelism(1)
input <- c("adf asfd asdf", "abc def", "asdf asdf asdf asdf asdf")
#input <- flink.distribute(text)

nGramCount <- function(text) {
    splits <- strsplit(text, " ")[[1]]
    numSplits <- length(splits)

    n <- 2

    srtIdx <- 1
    endIdx <- n
    ngrams = list()
    while (endIdx <= numSplits) {
        w <- splits[srtIdx]
        for (wi in (srtIdx + 1):endIdx) {
            w <- paste(w, splits[wi], collapse = " ")
        }
        ngrams[[srtIdx]] = list(w, 1)

        srtIdx <- srtIdx + 1
        endIdx <- endIdx + 1
    }
    return(ngrams)
}

countFunc <- function(a, b) {
    sum <- a[[2]] + b[[2]]
    return(list(a[[1]], sum))
}


ngrams <- flink.flatmap(input, nGramCount)
res <- flink.collect(ngrams, TRUE)

#groups <- flink.groupBy(ngrams, 0)

#counts <- flink.reduce(groups, countFunc)

#res <- flink.collect(counts, TRUE)
print(res)
