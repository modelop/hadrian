# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
# 
# This file is part of Hadrian.
# 
# Licensed under the Hadrian Personal Use and Evaluation License (PUEL);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://raw.githubusercontent.com/opendatagroup/hadrian/master/LICENSE
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' json
#'
#' Convert a list-of-lists structure into a JSON string in memory or a JSON file on disk.
#' @param x The structure to convert.
#' @param fileName If NULL (default), return an in-memory JSON string; if a fileName string, write to a file without incurring in-memory overhead.
#' @param newline If TRUE (default), end the string/file with a newline.
#' @param spaces If TRUE (default), include spaces after commas and colons for readability.
#' @param sigfigs If NULL (default), represent numbers with full precision; if an integer, represent numbers with that many significant digits.
#' @param stringsAsIs If FALSE (default), process strings to escape characters that need to be escaped in JSON; if TRUE, pass strings as-is for speed, possibly creating invalid JSON.
#' @return describeme
#' @export json
#' @examples
#' cat(json(pfaDocument))
#' json(pfaDocument, fileName = "myModel.pfa")

json <- function(x, fileName = NULL, newline = TRUE, spaces = TRUE, sigfigs = NULL, stringsAsIs = FALSE) {
    if (!is.null(fileName)) {
        outFile <- file(fileName)
        open(outFile, open = "wt")
        json.write(x, outFile, spaces, sigfigs, stringsAsIs)
        if (newline)
            writeLines("", con = outFile, sep = "\n")
        close(outFile)
    }
    else {
        out <- paste(json.string(x, spaces, sigfigs, stringsAsIs), collapse = "")
        if (newline)
            out <- paste(out, "", sep = "\n")
        out
    }
}

json.string <- function(x, spaces = TRUE, sigfigs = NULL, stringsAsIs = FALSE) {
    if (is.null(x))
        "null"
    else if (is.logical(x)  &&  x)
        "true"
    else if (is.logical(x)  &&  !x)
        "false"
    else if (is.numeric(x)  &&  length(x) == 1) {
        if (is.null(sigfigs))
            as.character(x)
        else
            as.character(signif(x, sigfigs))
    }
    else if (is.character(x)  &&  length(x) == 1) {
        if (stringsAsIs)
            paste("\"", x, "\"", sep = "")
        else
            paste("\"", gsub("\\x22", "\\\\\"",     # step 7: turn " (0x22) into \"
                        gsub("\\x08", "\\\\b",      # step 6: turn backspace (0x08) into \b
                        gsub("\\x0c", "\\\\f",      # step 5: turn formfeed (0x0c) into \f
                        gsub("\\x0a", "\\\\n",      # step 4: turn newline (0x0a) into \n
                        gsub("\\x0d", "\\\\r",      # step 3: turn carriage return (0x0d) into \r
                        gsub("\\x09", "\\\\t",      # step 2: turn horizontal tab (0x09) into \t
                        gsub("\\x5c", "\\\\\\\\",   # step 1: turn \ (0x5c) into \\
                             x))))))), "\"", sep = "")
    }
    else if (is.list(x)  &&  is.null(names(x))) {
        comma <- if (spaces) ", " else ","
        withcommas <- unlist(lapply(x, function(v) c(comma, json.string(v, spaces, sigfigs, stringsAsIs))))
        if (length(withcommas) > 0)
            withcommas <- withcommas[2:length(withcommas)]
        c("[", withcommas, "]")
    }
    else if (is.list(x)) {
        comma <- if (spaces) ", " else ","
        colon <- if (spaces) ": " else ":"
        withcommas <- unlist(lapply(names(x), function(n) c(comma, json.string(n), colon, json.string(x[[n]], spaces, sigfigs, stringsAsIs))))
        if (length(withcommas) > 0)
            withcommas <- withcommas[2:length(withcommas)]
        c("{", withcommas, "}")
    }
    else
        stop(paste("cannot convert object of class", class(x), "length", length(x), "to JSON", sep=" "))
}

json.write <- function(x, outFile, spaces = TRUE, sigfigs = NULL, stringsAsIs = FALSE) {
    if (is.list(x)  &&  is.null(names(x))) {
        writeLines("[", con = outFile, sep = "")
        first <- TRUE
        for (v in x) {
            if (!first) {
                if (spaces)
                    writeLines(", ", con = outFile, sep = "")
                else
                    writeLines(",", con = outFile, sep = "")
            }
            else
                first <- FALSE
            json.write(v, outFile, spaces, sigfigs, stringsAsIs)
        }
        writeLines("]", con = outFile, sep = "")
    }
    else if (is.list(x)) {
        writeLines("{", con = outFile, sep = "")
        first <- TRUE
        for (n in names(x)) {
            if (!first) {
                if (spaces)
                    writeLines(", ", con = outFile, sep = "")
                else
                    writeLines(",", con = outFile, sep = "")
            }
            else
                first <- FALSE
            json.write(n, outFile, spaces, sigfigs, stringsAsIs)
            if (spaces)
                writeLines(": ", con = outFile, sep = "")
            else
                writeLines(":", con = outFile, sep = "")
            json.write(x[[n]], outFile, spaces, sigfigs, stringsAsIs)
        }
        writeLines("}", con = outFile, sep = "")
    }
    else {
        writeLines(json.string(x, spaces, sigfigs, stringsAsIs), con = outFile, sep = "")
    }
}
