# flink.init()
library(flink)

print("bow_reduce")

flink.parallelism(1)

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
input <- flink.read_text(paste0(path, "/flink/flink-libraries/flink-r/src/test/Resources/lorem_ipsum.txt"))
#input <- flink.distribute(text)


wordCount <- function(word) {
    return(list(word, 1))
}

countFunc <- function(a, b) {
    list(a[[1]], a[[2]]+b[[2]])
}

ngrams <- input$map(wordCount)
#res <- flink.collect(ngrams, TRUE)

counts <- flink.groupBy(ngrams, 0)$reduce(countFunc)


res <- flink.collect(counts, TRUE)
print(res)
