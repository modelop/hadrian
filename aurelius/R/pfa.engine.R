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

counter <- new.env()
counter$n <- 0

#' pfa.engine
#'
#' Create an executable PFA scoring engine in R by calling Titus through rPython. If this function is successful, then the PFA is valid (only way to check PFA validity in R).
#' @param config list-of-lists representing a complete PFA document
#' @param tempFile if NULL, generate the PFA as a string in memory and pass it without touching disk; if a string, save the PFA document in a temporary file and have Python load it from that file
#' @return an object with a $action(...) method that can be used to score data
#' @export pfa.engine
#' @examples
#' pfa.engine(pfaDocument)   # where pfaDocument is created by pfa.config

pfa.engine <- function(config, tempFile = NULL) {
    library(RJSONIO)
    library(rPython)

    counter$n <- counter$n + 1
    name <- paste("engine_", counter$n, sep = "")
    method <- config$method

    # load PFA into Titus and get an error message if it's malformed
    python.exec("import json")
    python.exec("from titus.genpy import PFAEngine")
    python.exec("def do(*args): return args[-1]")

    if (is.null(tempFile)) {
        python.exec(paste(name, " = PFAEngine.fromJson(json.loads(r\"\"\"", json(config), "\"\"\"))[0]", sep=""))
    }
    else {
        json(config, fileName = tempFile)
        python.exec(paste(name, " = PFAEngine.fromJson(json.load(open(\"", tempFile, "\")))[0]", sep=""))
    }

    # work-around because python.method.call doesn't seem to work for my auto-generated stuff
    python.exec(paste(name, "_begin = lambda: ", name, ".begin()", sep = ""))
    if (!is.null(method)  &&  method == "emit") {
        python.exec(paste(name, "_emitted = []", sep = ""))
        python.exec(paste(name, ".emit = lambda x: ", name, "_emitted.append(x)", sep = ""))
        python.exec(paste(name, "_action = lambda x: do(", name, ".action(x), ", name, "_emitted.reverse(), [", name, "_emitted.pop() for i in xrange(len(", name, "_emitted))])", sep = ""))
    }
    else
        python.exec(paste(name, "_action = lambda x: ", name, ".action(x)", sep = ""))
    python.exec(paste(name, "_end = lambda: ", name, ".end()", sep = ""))

    python.exec(paste(name, "_actionsStarted = lambda: ", name, ".actionsStarted", sep = ""))
    python.exec(paste(name, "_actionsFinished = lambda: ", name, ".actionsFinished", sep = ""))
    # python.exec(paste(name, "_instance = lambda: ", name, ".instance", sep = ""))
    python.exec(paste(name, "_config = lambda: ", name, ".config.jsonNode(False, set())", sep = ""))
    python.exec(paste(name, "_snapshot = lambda: ", name, ".snapshot().jsonNode(False, set())", sep = ""))

    # provide a thing like a class
    list(begin = function() python.call(paste(name, "_begin", sep="")),
         action = function(input) python.call(paste(name, "_action", sep=""), input, simplify = FALSE),
         end = function() python.call(paste(name, "_end", sep="")),
         actionsStarted = function() python.call(paste(name, "_actionsStarted", sep="")),
         actionsFinished = function() python.call(paste(name, "_actionsFinished", sep="")),
         # instance = function() python.call(paste(name, "_instance", sep="")),
         config = function() python.call(paste(name, "_config", sep=""), simplify = FALSE),
         snapshot = function() python.call(paste(name, "_snapshot", sep=""), simplify = FALSE))
}

