counter <- new.env(parent=emptyenv())

.onLoad <- function(libname, pkgname) {
    ## start counter at 0
    counter[["engine"]] <- 0
    counter[["record"]] <- 0
    counter[["enum"]] <- 0
    counter[["fixed"]] <- 0
    counter[["symbol"]] <- 0
}