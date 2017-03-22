library(flink)

print("filter")
flink.parallelism(1)

wd <- getwd()
print(paste0("working directory: ", wd))

splits <- strsplit(wd, "/")[[1]]
path <- "/"
for (split in splits) {
    if (split != "flink-libraries") {
        path <- paste0(path, "/", split)
    } else {
        break
    }
}
input <- flink.read_text(paste0(path, "/flink-libraries/flink-r/src/test/Resources/py_lorem.txt"))

filterFunction <- function(elem) {
    if (nchar(elem) < 100000000) {
        return(TRUE);
    } else {
        return(FALSE);
    }
}

f <- input$filter(filterFunction)
res <- flink.collect(f)
print(res)
