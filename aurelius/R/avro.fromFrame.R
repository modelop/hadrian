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

# function to get a PFA record structure from a data frame

#' avro.fromFrame
#'
#' Convenience function for creating an Avro input schema from a data frame
#' @param dataFrame a data.frame object
#' @param exclude set of field names to exclude
#' @param name required name of the record (if not specified, uniqueRecordName will be invoked)
#' @param namespace optional namespace of the record
#' @return Avro list-of-lists representing a record type
#' @export avro.fromFrame
#' @examples
#' avro.fromFrame(dataFrame)

avro.fromFrame <- function(dataFrame, exclude = list(), name = NULL, namespace = NULL) {
    fields <- list()
    for (x in names(dataFrame))
        if (!(x %in% exclude))
            fields[[x]] <- avro.type(dataFrame[[x]])
    avro.record(fields, name, namespace)
}
