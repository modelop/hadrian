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

# function to get a PFA record structure from a data frame

#' avro_from_df
#'
#' Convenience function for creating an Avro input schema from a data frame
#' 
#' @param df a \code{data.frame}
#' @param exclude set of field names to exclude
#' @param name required name of the record (if not specified, gen_unique_rec_name() will be invoked)
#' @param namespace optional namespace of the record
#' @return a \code{list} of lists representing an Avro record type
#' @examples
#' avro_from_df(data.frame(x = c(1, 3, 5)))
#' @export
avro_from_df <- function(df, exclude = list(), name = NULL, namespace = NULL) {
  fields <- list()
  for (x in names(df)){
      if (!(x %in% exclude)){
          fields[[x]] <- avro_type(df[[x]])
      }
  }
  avro_record(fields, name, namespace)
}