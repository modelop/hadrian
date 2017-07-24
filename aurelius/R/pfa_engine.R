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

#' pfa_engine
#'
#' Create an executable PFA scoring engine in R by calling Titus through rPython. 
#' If this function is successful, then the PFA is valid (only way to check 
#' PFA validity in R).
#' 
#' @importFrom utils capture.output
#' @source json.R
#' @param doc \code{list} of lists representing a complete PFA document
#' @examples
#' \dontrun{
#' my_pfa_doc <- pfa_document(avro_double, avro_double, expression(input + 10))
#' pfa_engine(my_pfa_doc)   # requres rPython and Titus to be installed
#' }
#' @export
pfa_engine <- function(doc) {
  
    name <- gen_unique_eng_name()
    method <- doc$method

    # load PFA into Titus and get an error message if it's malformed
    rPython::python.exec("import json")
    rPython::python.exec("from titus.genpy import PFAEngine")
    rPython::python.exec("def do(*args): return args[-1]")
    rPython::python.exec(paste(name, " = PFAEngine.fromJson(json.loads(r\"\"\"", paste(capture.output(write_pfa(doc)), collapse=''), "\"\"\"))[0]", sep=""))

    # work-around because python.method.call doesn't seem to work for my auto-generated stuff
    rPython::python.exec(paste(name, "_begin = lambda: ", name, ".begin()", sep = ""))
    
    if (!is.null(method)  &&  method == "emit") {
      rPython::python.exec(paste(name, "_emitted = []", sep = ""))
      rPython::python.exec(paste(name, ".emit = lambda x: ", name, "_emitted.append(x)", sep = ""))
      rPython::python.exec(paste(name, "_action = lambda x: do(", name, ".action(x), ", name, "_emitted.reverse(), [", name, "_emitted.pop() for i in xrange(len(", name, "_emitted))])", sep = ""))
    }
    else {
      rPython::python.exec(paste(name, "_action = lambda x: ", name, ".action(x)", sep = ""))
    }
    
    rPython::python.exec(paste(name, "_end = lambda: ", name, ".end()", sep = ""))
    rPython::python.exec(paste(name, "_actionsStarted = lambda: ", name, ".actionsStarted", sep = ""))
    rPython::python.exec(paste(name, "_actionsFinished = lambda: ", name, ".actionsFinished", sep = ""))
    rPython::python.exec(paste(name, "_config = lambda: ", name, ".config.jsonNode(False, set())", sep = ""))
    rPython::python.exec(paste(name, "_snapshot = lambda: ", name, ".snapshot().jsonNode(False, set())", sep = ""))

    # provide a thing like a class
    list(begin = function() rPython::python.call(paste(name, "_begin", sep="")),
         action = function(input) rPython::python.call(paste(name, "_action", sep=""), input, simplify = FALSE),
         end = function() rPython::python.call(paste(name, "_end", sep="")),
         actionsStarted = function() rPython::python.call(paste(name, "_actionsStarted", sep="")),
         actionsFinished = function() rPython::python.call(paste(name, "_actionsFinished", sep="")),
         config = function() rPython::python.call(paste(name, "_config", sep=""), simplify = FALSE),
         snapshot = function() rPython::python.call(paste(name, "_snapshot", sep=""), simplify = FALSE))
}