library(flink)

print("count_local")

flink.parallelism(1)

text <- c("abc def", "adf asfd asdf", "asdf asdf asdf asdf asdf")
#input <- flink.distribute(text)

count <- function(text) {
    splits <- strsplit(text, " ")[[1]]
    return(length(splits))
}

splitText <- flink.map(text, count)

res <- flink.collect(splitText, TRUE)
print(res)
