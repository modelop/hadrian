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


#' write_pfa
#'
#' Convert a PFA list of lists into a JSON string in memory or a 
#' JSON file on disk.
#' 
#' @param doc The document to convert.
#' @param file a string representing file path to write to. If '' then the 
#' string of JSON is returned
#' @param force a logical indicating to unclass/skip objects of classes with no defined JSON mapping
#' @param auto_unbox a logical indicating to automatically unbox all atomic vectors of length 1
#' @param pretty a logical indicating to add indentation whitespace to JSON output.
#' @param digits max number of decimal digits to print for numeric values. 
#' Use I() to specify significant digits. Use NA for max precision.
#' @param ... additional arguments passed to toJSON
#' @importFrom jsonlite toJSON
#' @examples
#' \dontrun{
#' my_pfa_doc <- pfa_document(avro_double, avro_double, expression(input + 10))
#' write_pfa(my_pfa_doc)
#' write_pfa(my_pfa_doc, file = "my-model.pfa")
#' }
#' @export
write_pfa <- function(doc, 
                      file='', 
                      force = TRUE, 
                      auto_unbox = TRUE, 
                      pretty = FALSE, 
                      digits = 8, ...){
  
  write(toJSON(doc, 
               force = force, 
               auto_unbox = auto_unbox, 
               pretty = pretty, 
               digits = digits, ...), 
        file=file)
  
}