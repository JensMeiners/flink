# flink.init()
library(flink)

print("bow_reduce")

flink.parallelism(1)

wd <- getwd()
print(paste0("working directory: ", wd))

input <- flink.read_text(paste0(wd, "/src/test/Resources/lorem_ipsum.txt"))
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
