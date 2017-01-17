library(flink)
# flink.init("cloud-7", 6002)
flink.parallelism(4)

input <- flink.readTextFile("hdfs://cloud-7:45000//tmp/input/rubbish.txt")

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
            w <- paste(w, splits[wi])
        }
        ngrams[srtIdx] = list(w, 1)

        srtIdx <- srtIdx + 1
        endIdx <- endIdx + 1
    }
    return(ngrams)
}

splitText <- flink.map(input, nGramCount)

flink.writeAsText(splitText, "hdfs://cloud-7:45000//tmp/input/output_fastr")
flink.execute()
