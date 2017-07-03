# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
# 
# This file is part of Hadrian.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#' read_pfa
#'
#' Convert a JSON string in memory or a JSON file on disk into a list-of-lists structure.
#' 
#' @importFrom methods is
#' @param x A string, file or url connection
#' @return a \code{list} of lists structure in which null -> NULL, true -> TRUE, 
#' false -> FALSE, numbers -> numeric, strings -> character, array -> list, 
#' object -> named list
#' @examples
#' # literal JSON string  (useful for small examples)
#' toy_model <- read_pfa('{"input":"double","output":"double","action":[{"+":["input",10]}]}')
#' 
#' # from a local path, must be wrapped in "file" command to create a connection
#' local_model <- read_pfa(file(system.file("extdata", "my-model.pfa", package = "aurelius")))
#' 
#' # from a url (split on two lines so not to exceed 100 char wide during install)
#' url_model <- read_pfa(url(paste0('https://raw.githubusercontent.com/ReportMort/hadrian', 
#'                      '/feature/add-r-package-structure/aurelius/inst/extdata/my-model.pfa')))
#' @export
read_pfa <- function(x) {
  
  # keep just in case users forget to wrap urls in the url() function
  if (is.character(x) & length(x) == 1 & grepl("^(http|ftp)s?://", x)) {
    x <- url(x)
  }

  if (is.character(x)  &&  length(x) == 1) {
    x <- strsplit(x, "", useBytes = TRUE)[[1]]
    i <- 0
    getNext <- function() {
        if (i < length(x)) {
            out <- x[i : i+1]
            i <<- i + 1
            out
        }
        else
            character(0)
    }
    getIndex <- function() { i }
    rewind <- function() { i <<- i - 1 }
  }
  else if (is(x, "connection")) {
    if (!isOpen(x)){
      open(x)
      on.exit(close(x))
    }
    
    i <- 0
    useStorage <- FALSE
    storage <- NULL
    getNext <- function() {
        i <- i + 1
        if (useStorage) {
            useStorage <<- FALSE
            storage
        }
        else {
            storage <<- readChar(x, 1, useBytes = TRUE)
            storage
        }
    }
    getIndex <- function() { i }
    rewind <- function() {
        i <<- i - 1
        useStorage <<- TRUE
    }
  }
  else
    stop("read_pfa requires a file path, URL, literal JSON, or a connection (file)")

  out <- parse_value(getNext, getIndex, rewind)
  while (length(y <- getNext()) != 0)
      if (y != " "  &&  y != "\t"  &&  y != "\n"  &&  y != "\r")
          stop(paste("unexpected character", y, "after JSON terminates at index", getIndex()))
  out
}


#' parse_value
#' 
#' parse_value
#' 
#' @keywords internal
parse_value <- function(getNext, getIndex, rewind) {
    x <- getNext()
    if (length(x) == 0)
        stop("end of JSON file or string while expecting value")
    else if (x == "n") {
        if (getNext() != "u"  ||  getNext() != "l"  ||  getNext() != "l")
            stop(paste("token starting with \"n\" is not \"null\" at index", getIndex()))
        NULL
    }
    else if (x == "t") {
        if (getNext() != "r"  ||  getNext() != "u"  ||  getNext() != "e")
            stop(paste("token starting with \"t\" is not \"true\" at index", getIndex()))
        TRUE
    }
    else if (x == "f") {
        if (getNext() != "a"  ||  getNext() != "l"  ||  getNext() != "s"  ||  getNext() != "e")
            stop(paste("token starting with \"f\" is not \"false\" at index", getIndex()))
        FALSE
    }
    else if (x == "-"  ||  (x >= "0"  &&  x <= "9"))
        parse_number(x, getNext, getIndex, rewind)
    else if (x == "\"")
        parse_string(getNext, getIndex, rewind)
    else if (x == "{")
        parse_object(getNext, getIndex, rewind)
    else if (x == "[")
        parse_array(getNext, getIndex, rewind)
    else if (x == " "  ||  x == "\t"  ||  x == "\n"  ||  x == "\r")
        parse_value(getNext, getIndex, rewind)
    else
        stop(paste("unexpected character", x, "at index", getIndex()))
}


#' parse_number
#' 
#' parse_number
#' 
#' @keywords internal
parse_number <- function(x, getNext, getIndex, rewind) {
    y <- character(0)
    while (length(x) != 0  &&  (x == "-"  ||  x == "+"  ||  x == "."  ||  x == "e"  ||  x == "E"  ||  (x >= "0"  &&  x <= "9"))) {
        y <- c(y, x)
        x <- getNext()
    }
    rewind()
    y <- paste(y, collapse = "")
    out <- as.numeric(y)
    if (is.na(out))
        stop(paste("bad number", y, "at index", getIndex()))
    out
}


#' parse_string
#' 
#' parse_string
#' 
#' @keywords internal
parse_string <- function(getNext, getIndex, rewind) {
    y <- character(0)
    x <- getNext()
    while (length(x) != 0  &&  x != "\"") {
        if (x == "\\") {
            x <- getNext()
            if (x == "\"")
                y <- c(y, "\x22")               # turn \" into " (0x22)
            else if (x == "\\")
                y <- c(y, "\x5c")               # turn \\ into \ (0x5c)
            else if (x == "/")
                y <- c(y, "\x2f")               # turn \/ into / (0x2f)
            else if (x == "b")
                y <- c(y, "\x08")               # turn \b into backspace (0x08)
            else if (x == "f")
                y <- c(y, "\x0c")               # turn \f into formfeed (0x0c)
            else if (x == "n")
                y <- c(y, "\x0a")               # turn \n into newline (0x0a)
            else if (x == "r")
                y <- c(y, "\x0d")               # turn \r into carriage return (0x0d)
            else if (x == "t")
                y <- c(y, "\x09")               # turn \t into horizontal tab (0x09)
            else if (x == "u")
                y <- c(y, eval(parse(text = paste(c("\"", "\\", "u", getNext(), getNext(), getNext(), getNext(), "\""), collapse = ""))))
            else
                stop(paste("unrecognized backslash sequence at index", getIndex()))
        }
        else
            y <- c(y, x)
        x <- getNext()
    }
    paste(y, collapse = "")
}


#' parse_object
#' 
#' parse_object
#' 
#' @keywords internal
parse_object <- function(getNext, getIndex, rewind) {
    out <- json_map()
    repeat {
        commas <- 0
        while (length(x <- getNext()) != 0  &&  x != "\""  &&  x != "}") {
            if (x == ",") {
                if (length(out) == 0)
                    stop(paste("comma before first object key at index", getIndex()))
                commas <- commas + 1
            }
            else if (x != " "  &&  x != "\t"  &&  x != "\n"  &&  x != "\r")
                stop(paste("unrecognized character", x, "while expecting object key at index", getIndex()))
        }

        if (length(x) == 0)
            stop("end of JSON file or string while expecting object key")

        else if (x == "}") {
            if (commas != 0)
                stop(paste("trailing comma at end of object at index", getIndex()))
            break
        }

        else if (x == "\"") {
            if (commas >= 2)
                stop(paste("too many commas before key at index", getIndex()))
            else if (commas != 1  &&  length(out) > 0)
                stop(paste("missing comma between key-value pairs at index", getIndex()))

            key <- parse_string(getNext, getIndex, rewind)

            while (length(x <- getNext()) != 0  &&  x != ":")
                if (x != " "  &&  x != "\t"  &&  x != "\n"  &&  x != "\r")
                    stop(paste("unrecognized character", x, "while expecting colon at index", getIndex()))

            if (length(x) == 0)
                stop("end of JSON file or string while expecting object value")

            out[[key]] <- parse_value(getNext, getIndex, rewind)
        }
    }
    out
}


#' parse_array
#' 
#' parse_array
#' 
#' @keywords internal
parse_array <- function(getNext, getIndex, rewind) {
    out <- json_array()

    while (length(x <- getNext()) != 0  &&  x != "]")
        if (x != " "  &&  x != "\t"  &&  x != "\n"  &&  x != "\r") {
            rewind()
            break
        }

    if (x != "]")
        repeat {
            out[[length(out) + 1]] <- parse_value(getNext, getIndex, rewind)

            while (length(x <- getNext()) != 0  &&  x != ","  &&  x != "]")
                if (x != " "  &&  x != "\t"  &&  x != "\n"  &&  x != "\r")
                    stop(paste("unrecognized character", x, "while expecting array delimiter at index", getIndex()))

            if (length(x) == 0)
                stop("end of JSON file or string while expecting array delimiter")

            else if (x == "]")
                break
        }
    out
}