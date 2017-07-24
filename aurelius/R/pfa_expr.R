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


#' pfa_expr
#'
#' Convert a quoted R expression into a \code{list} of lists that can be 
#' inserted into PFA
#' 
#' @importFrom utils tail
#' @param expr quoted R expression (e.g. quote(2 + 2))
#' @param symbols list of symbol names that would be in scope when evaluating this expression
#' @param cells list of cell names that would be in scope when evaluating this expression
#' @param pools list of pool names that would be in scope when evaluating this expression
#' @param fcns list of function names that would be in scope when evaluating this expression
#' @param env environment for resolving unrecognized symbols as substitutions
#' @return a \code{list} of lists representing a fragment of a PFA document
#' @examples
#' pfa_expr(quote(2 + 2))
#' @export
pfa_expr <- function(expr, symbols = list(), cells = list(), pools = list(), fcns = list(), env = parent.frame()) {
    symbolsEnv <- new.env()
    if (is.list(symbols)) {
        for (x in symbols)
            symbolsEnv[[x]] = x
    }
    if (!is.environment(symbols))
        symbols <- symbolsEnv

    cellsEnv <- new.env()
    if (is.list(cells)) {
        for (x in cells)
            cellsEnv[[x]] = x
    }
    if (!is.environment(cells))
        cells <- cellsEnv

    poolsEnv <- new.env()
    if (is.list(pools)) {
        for (x in pools)
            poolsEnv[[x]] = x
    }
    if (!is.environment(pools))
        pools <- poolsEnv

    fcnsEnv <- new.env()
    if (is.list(fcns)) {
        for (x in fcns)
            fcnsEnv[[x]] = x
    }
    if (!is.environment(fcns))
        fcns <- fcnsEnv

    if (is.expression(expr)) {
        pfa_expr_again <- function(expr) pfa_expr(expr, symbols = symbols, cells = cells, pools = pools, fcns = fcns, env = env)
        list(do = lapply(expr, pfa_expr_again))
    }
    else if (is.null(expr))
        NULL
    else if (is.logical(expr)) {
        if (expr == quote(TRUE))
            TRUE
        else
            FALSE
    }
    else if (is.name(expr)) {
        asstr <- as.character(expr)
        if (asstr %in% ls(cells))
            list(cell = asstr)
        else if (asstr %in% ls(pools))
            list(pool = asstr)
        else if (asstr %in% ls(fcns))
            list(fcn = asstr)
        else if (asstr %in% ls(symbols))
            asstr
        else if (identical(expr, quote(T)))
            TRUE
        else if (identical(expr, quote(F)))
            FALSE
        else if (identical(expr, quote(pi)))
            list(m.pi = list())
        else
            eval(expr, env)   # substitute in predefined expressions
    }
    else if (is.atomic(expr)  &&  length(expr) == 1) {
        if (class(expr) == "character")
            list(string = as.character(expr))
        else
            as.numeric(expr)
    }
    else if (is.call(expr)) {
        name <- as.character(expr[[1]])
        args <- tail(as.list(expr), -1)
        convert_function(name, args, symbols, cells, pools, fcns, env = env)
    }
    else
        stop("unrecognized expression of type ", typeof(expr))
}