# flink.init()
library(flink)


wd <- getwd()
print(paste0("working directory: ", wd))
splits <- strsplit(wd, "/")[[1]]
path <- "/"
for (split in splits) {
    if (split != "flink") {
        path <- paste0(path, "/", split)
    } else {
        break
    }
}
print(paste0("flink path: ", path))

flink.parallelism(4)
input <- flink.read_text(paste0(path, "/flink/flink-libraries/flink-r/src/test/Resources/lorem_ipsum_ngram_10000.txt"))

nGramCount <- function(text) {
    chprint("ngram Count")
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
    list(a[[1]], a[[2]]+b[[2]])
}

ngrams <- input$flatmap(nGramCount)
#res <- flink.collect(ngrams, TRUE)

counts <- flink.groupBy(ngrams, 0)$reduce(countFunc)


res <- flink.collect(counts, TRUE)
print(res)
