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


#' gen_unique_eng_name
#'
#' Convenience or internal function for generating engine names; each call results in a new name.
#' 
#' @return name in the form "Engine_###"
#' @examples
#' gen_unique_eng_name()
#' @export
gen_unique_eng_name <- function() {
    counter$engine <- counter$engine + 1
    paste("Engine_", counter$engine, sep = "")
}


#' gen_unique_rec_name
#'
#' Convenience or internal function for generating record names; each call results in a new name.
#' 
#' @return name in the form "Record_###"
#' @examples
#' gen_unique_rec_name()
#' @export
gen_unique_rec_name <- function() {
    counter$record <- counter$record + 1
    paste("Record_", counter$record, sep = "")
}


#' gen_unique_enum_name
#'
#' Convenience or internal function for generating enum names; each call results in a new name.
#' 
#' @return name in the form "Enum_###"
#' @examples
#' gen_unique_enum_name()
#' @export
gen_unique_enum_name <- function() {
    counter$enum <- counter$enum + 1
    paste("Enum_", counter$enum, sep = "")
}


#' gen_unique_fixed_name
#'
#' Convenience or internal function for generating fixed names; each call results in a new name.
#' 
#' @return name in the form "Fixed_###"
#' @examples
#' gen_unique_fixed_name()
#' @export
gen_unique_fixed_name <- function() {
    counter$fixed <- counter$fixed + 1
    paste("Fixed_", counter$fixed, sep = "")
}


#' gen_unique_symb_name
#'
#' Convenience or internal function for generating symbol names; each call results in a new name.
#' 
#' @param symbols Symbols
#' @return name in the form "tmp_###"
#' @examples
#' gen_unique_symb_name()
#' @export
gen_unique_symb_name <- function(symbols) {
    while (TRUE) {
        counter$symbol <- counter$symbol + 1
        test <- paste("tmp_", counter$symbol, sep = "")
        if (!(test %in% ls(symbols)))
            return(test)
    }
}