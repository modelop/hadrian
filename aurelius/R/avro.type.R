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

# function to get PFA type names from R type names (FIXME: incomplete)

#' avro.type
#'
#' Inspects an R object and produces the corresponding Avro type name
#' @param obj object to inspect
#' @return list-of-lists Avro schema
#' @export avro.type
#' @examples
#' avro.type("hello")           # "string"
#' avro.type(factor("hello"))   # "string"
#' avro.type(3.14)              # "double"
#' avro.type(3)                 # "int"

avro.type <- function(obj) {
    if (is.factor(obj))
        "string"
    else {
        t <- typeof(obj)
        if (t == "double")
            "double"
        else if (t == "integer")
            "int"
        else if (t == "character")
            "string"
        else
            stop(paste("unsupported R -> PFA data type name conversion:", t))
    }
}
