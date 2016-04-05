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

# function to convert an R expression into a PFA expression

#' pfa.expr
#'
#' Convert a quoted R expression into a list-of-lists that can be inserted into PFA
#' @param expr quoted R expression (e.g. quote(2 + 2))
#' @param symbols list of symbol names that would be in scope when evaluating this expression
#' @param cells list of cell names that would be in scope when evaluating this expression
#' @param pools list of pool names that would be in scope when evaluating this expression
#' @param fcns list of function names that would be in scope when evaluating this expression
#' @param env environment for resolving unrecognized symbols as substitutions
#' @return a list-of-lists representing a fragment of a PFA document
#' @export pfa.expr
#' @examples
#' pfa.expr(quote(2 + 2))

pfa.expr <- function(expr, symbols = list(), cells = list(), pools = list(), fcns = list(), env = parent.frame()) {
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
        pfa.expr.again <- function(expr) pfa.expr(expr, symbols = symbols, cells = cells, pools = pools, fcns = fcns, env = env)
        list(do = lapply(expr, pfa.expr.again))
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
        convertFunction(name, args, symbols, cells, pools, fcns, env = env)
    }
    else
        stop("unrecognized expression of type ", typeof(expr))
}

# extensible ruleset for converting R code into PFA
converters <- list()
converters.add <- function(converter) {
    converters[[length(converters) + 1]] <<- converter
}

convertFunction <- function(name, args, symbols, cells, pools, fcns, env) {
    for (converter in converters)
        if (converter$domain(name, args))
            return(converter$apply(name, args, symbols, cells, pools, fcns, env))

    # after trying all converters, assume it's a literal PFA function
    pfa.expr.nested <- function (x) pfa.expr(x, symbols = as.environment(as.list(symbols)), cells = cells, pools = pools, fcns = fcns, env = env)
    out <- list()
    out[[name]] = lapply(args, pfa.expr.nested)
    out
}

# function objects are very peculiar because we need to add type annotations in a way that:
#     1. looks natural to the programmer
#     2. can be extracted from the AST
#     3. would produce otherwise-valid R code
# 
# so we require functions to look like any of the following:
#     function(. = avro.rettype) body
#     function(x = avro.type1, . = avro.rettype) body
#     function(x = avro.type1, y = avro.type2, . = avro.rettype) body
#     function(x = avro.type1 -> avro.rettype) body
#     function(x = avro.type1, y = avro.type2 -> avro.rettype) body
#     ...
# 
# PFA (and therefore this R subset) doesn't really have default arguments; every argument must
# always be supplied, and hence these avro objects won't really be used.  The R subset behaves
# exactly like the equivalent PFA.
convertFcn <- function(name, args, symbols, cells, pools, fcns, env) {
    params <- args[[1]]
    body <- args[[2]]

    symbols.body <- as.environment(as.list(symbols))

    out <- list(params = list())
    if ("." %in% names(params)) {
        out$ret <- eval(params[["."]], env)
        for (p in names(params))
            if (p != ".") {
                obj <- list()
                obj[[p]] <- eval(params[[p]], env)
                out$params[[length(out$params) + 1]] <- obj
                symbols.body[[p]] = p
            }
    }
    else if (length(params) > 0  &&  is.call(params[[length(params)]])  &&  params[[length(params)]][[1]] == "<-") {
        out$ret <- eval(params[[length(params)]][[2]], env)
        params[[length(params)]] <- params[[length(params)]][[3]]
        for (p in names(params)) {
            obj <- list()
            obj[[p]] <- eval(params[[p]], env)
            out$params[[length(out$params) + 1]] <- obj
            symbols.body[[p]] = p
        }
    }
    else
        stop("function definition is missing a return type (specified with a '.' argument or '->')")

    pfa.expr.body <- function (x) pfa.expr(x, symbols = symbols.body, cells = cells, pools = pools, fcns = fcns, env = env)

    if (is.call(body)  &&  body[[1]] == "{")
        out$do <- lapply(tail(as.list(body), -1), pfa.expr.body)
    else
        out$do <- pfa.expr.body(body)
    out
}
converters.add(list(domain = function(name, args) { name == "function"  &&  length(args) >= 2 },
                    apply  = convertFcn))

# new generates literals, such as a "value" form, or a "new" form
converters.add(list(domain = function(name, args) { name == "new"  &&  length(args) >= 2 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        pfa.expr.nested <- function (x) pfa.expr(x, symbols = as.environment(as.list(symbols)), cells = cells, pools = pools, fcns = fcns, env = env)

                        type = eval(args[[1]], env)

                        if (!is.character(type)  &&  is.null(names(type)))
                            list(type = type, value = args[[2]])

                        else if (type == "null"  ||  identical(type, list(type = "null")))
                            stop("use NULL to create null values")

                        else if (type == "boolean"  ||  identical(type, list(type = "boolean")))
                            stop("use TRUE/FALSE to create boolean values")

                        else if (type == "int"  ||  identical(type, list(type = "int")))
                            list(int = as.numeric(args[[2]]))

                        else if (type == "long"  ||  identical(type, list(type = "long")))
                            list(long = as.numeric(args[[2]]))

                        else if (type == "float"  ||  identical(type, list(type = "float")))
                            list(float = as.numeric(args[[2]]))

                        else if (type == "double"  ||  identical(type, list(type = "double")))
                            list(double = as.numeric(args[[2]]))

                        else if (type == "string"  ||  identical(type, list(type = "string")))
                            list(string = as.character(args[[2]]))

                        else if (type == "bytes"  ||  identical(type, list(type = "bytes")))
                            stop("bytes Base64 conversion not yet implemented")

                        else if (is.character(args[[2]]))
                            list(type = type, value = unjson(args[[2]]))

                        else
                            list(type = type, new = lapply(tail(as.list(args), -1), pfa.expr.nested))
                    }))

# curly brackets generate "do" forms
converters.add(list(domain = function(name, args) { name == "{" },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        symbols.nested <- as.environment(as.list(symbols))
                        pfa.expr.nested <- function (x) pfa.expr(x, symbols = symbols.nested, cells = cells, pools = pools, fcns = fcns, env = env)
                        list(do = lapply(args, pfa.expr.nested))
                    }))

# parentheses don't generate anything: PFA is already Polish notation, no need for groupings
converters.add(list(domain = function(name, args) { name == "(" },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        pfa.expr(args[[1]], symbols = symbols, cells = cells, pools = pools, fcns = fcns, env = env)
                    }))

# assignments generate "let" for new symbols, "set" for old ones, "attr-to", "cell-to", or "pool-to" if subscripted
# note that subscripts generate "do" mini-programs to handle the difference in how R and PFA return values
converters.add(list(domain = function(name, args) { (name == "<-"  ||  name == "="  ||  name == "<<-")  &&  length(args) == 2 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        symbols.nested <- as.environment(as.list(symbols))
                        pfa.expr.nested <- function (x) pfa.expr(x, symbols = symbols.nested, cells = cells, pools = pools, fcns = fcns, env = env)

                        if (is.name(args[[1]])  &&  as.character(args[[1]]) %in% ls(cells)) {
                            if (name == "<-"  ||  name == "=")
                                stop(paste("use <<- for cells like", as.character(args[[1]]), ", not <- or ="))
                            list(cell = as.character(args[[1]]), to = pfa.expr.nested(args[[2]]))
                        }

                        else if (is.name(args[[1]])  &&  as.character(args[[1]]) %in% ls(pools))
                            stop("pool assignment must have a path")
                        
                        else if (is.name(args[[1]])) {
                            if (name == "<<-")
                                stop(paste("use <- for ordinary symbols like", as.character(args[[1]]), ", not <<-"))
                            asstr <- as.character(args[[1]])
                            out <- list()
                            out[[asstr]] <- pfa.expr.nested(args[[2]])
                            if (asstr %in% ls(symbols))
                                list(set = out)
                            else {
                                symbols[[asstr]] <- asstr
                                list(let = out)
                            }
                        }

                        else if (is.call(args[[1]])) {
                            expr <- args[[1]]
                            subname <- as.character(expr[[1]])
                            subargs <- tail(as.list(expr), -1)
                            if ((subname == "["  ||  subname == "[["  ||  subname == "$")  &&  length(subargs) == 2) {
                                extractor <- convertFunction(subname, subargs, symbols, cells, pools, fcns, env)
                                
                                if ("attr" %in% names(extractor)) {
                                    if (name == "<<-")
                                        stop(paste("use <- for ordinary symbols like", extractor$attr, ", not <<-"))

                                    tmpSymbol <- uniqueSymbolName(symbols)
                                    extractor$to <- tmpSymbol

                                    assignment <- list()
                                    assignment[[tmpSymbol]] <- pfa.expr.nested(args[[2]])

                                    secondAssignment <- list()
                                    secondAssignment[[extractor$attr]] <- extractor

                                    list(do = list(list(let = assignment), list(set = secondAssignment), tmpSymbol))
                                }
                                else if ("cell" %in% names(extractor)) {
                                    if (name == "<-"  ||  name == "=")
                                        stop(paste("use <<- for cells like", extractor$cell, ", not <- or ="))
                                    extractor$to <- pfa.expr.nested(args[[2]])
                                    extractor
                                }
                                else if ("pool" %in% names(extractor)) {
                                    if (name == "<-"  ||  name == "=")
                                        stop(paste("use <<- for pools like", extractor$pool, ", not <- or ="))

                                    symbols.init <- as.environment(as.list(symbols))
                                    pfa.expr.init <- function (x) pfa.expr(x, symbols = symbols.init, cells = cells, pools = pools, fcns = fcns, env = env)

                                    if (is.call(args[[2]])  &&  args[[2]][[1]] == "function"  &&  is.call(args[[2]][[3]])  &&  args[[2]][[3]][[1]] == "<-") {
                                        to <- as.call(list(args[[2]][[1]], args[[2]][[2]], args[[2]][[3]][[2]], args[[2]][[4]]))
                                        extractor$to <- pfa.expr.nested(to)
                                        extractor$init <- pfa.expr.init(args[[2]][[3]][[3]])
                                        extractor
                                    }
                                    else if (is.call(args[[2]])  &&  args[[2]][[1]] == "<-") {
                                        to <- args[[2]][[2]]
                                        extractor$to <- pfa.expr.nested(to)
                                        extractor$init <- pfa.expr.init(args[[2]][[3]])
                                        extractor
                                    }
                                    else
                                        stop(paste("pools like", extractor$pool, "need a <- clause after the <<- clause"))
                                }
                            }
                            else
                                stop(paste("unrecognized assignment:", as.call(c(as.symbol(name), args))))
                        }
                        else
                            stop(paste("unrecognized assignment:", as.call(c(as.symbol(name), args))))
                    }))

unwrapBrackets <- function(path, expr) {
    if (is.call(expr)) {
        name <- as.character(expr[[1]])
        args <- tail(as.list(expr), -1)
        if (name == "["  &&  length(args) == 2) {
            ## auto-minus-one is dangerous
            ## path <- c(as.call(list("-", args[[2]], 1)), path)
            path <- c(args[[2]], path)
            path <- unwrapBrackets(path, args[[1]])
        }
        else if (name == "[["  &&  length(args) == 2) {
            path <- c(args[[2]], path)
            path <- unwrapBrackets(path, args[[1]])
        }
        else if (name == "$"  &&  length(args) == 2) {
            path <- c(as.character(args[[2]]), path)
            path <- unwrapBrackets(path, args[[1]])
        }
        else
            path <- c(expr, path)
    }
    else
        path <- c(expr, path)
    path
}

# subscripts (outside of an assignment) generate "attr", "cell", or "pool"
converters.add(list(domain = function(name, args) { (name == "["  ||  name == "[["  ||  name == "$")  &&  length(args) == 2 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        expr <- as.call(c(as.symbol(name), args))
                        path <- unwrapBrackets(list(), expr)
                        first <- path[[1]]
                        rest <- tail(path, -1)

                        pfa.expr.path <- function (x) pfa.expr(x, symbols = as.environment(as.list(symbols)), cells = cells, pools = pools, fcns = fcns, env = env)

                        if (is.name(first)  &&  as.character(first) %in% ls(cells))
                            list(cell = as.character(first), path = lapply(rest, pfa.expr.path))
                        else if (is.name(first)  &&  as.character(first) %in% ls(pools))
                            list(pool = as.character(first), path = lapply(rest, pfa.expr.path))
                        else {
                            symbols.expr <- as.environment(as.list(symbols))
                            pfa.expr.expr <- function (x) pfa.expr(x, symbols = symbols.expr, cells = cells, pools = pools, fcns = fcns, env = env)
                            list(attr = pfa.expr.expr(first), path = lapply(rest, pfa.expr.path))
                        }
                    }))

# if statements generate "if" with an "else" clause (R returns a union for the no-else case, so PFA is modified to match)
# no attempt is made to unwind nested "if" into "cond"
# ifnotnull special case: if (!is.null(x <- expr)) { something } else { whatever }
converters.add(list(domain = function(name, args) { name == "if"  &&  length(args) == 2 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        symbols.predicate <- as.environment(as.list(symbols))
                        pfa.expr.predicate <- function (x) pfa.expr(x, symbols = symbols.predicate, cells = cells, pools = pools, fcns = fcns, env = env)
                        symbols.then <- as.environment(as.list(symbols))
                        pfa.expr.then <- function (x) pfa.expr(x, symbols = symbols.then, cells = cells, pools = pools, fcns = fcns, env = env)

                        # ifnotnull special case
                        if (is.call(args[[1]])  &&  args[[1]][[1]] == "!"  &&  is.call(args[[1]][[2]])  &&  args[[1]][[2]][[1]] == "is.null"  &&  is.call(args[[1]][[2]][[2]])  &&  args[[1]][[2]][[2]][[1]] == "<-") {
                            asstr <- as.character(args[[1]][[2]][[2]][[2]])
                            symbols.then[[asstr]] <- asstr

                            assignments <- list()
                            assignments[[asstr]] <- pfa.expr.predicate(args[[1]][[2]][[2]][[3]])
                            out <- list()
                            out[["ifnotnull"]] <- assignments
                            out[["then"]] <- pfa.expr.then(args[[2]])
                            out[["else"]] <- NULL
                            out
                        }
                        else {
                            out <- list()
                            out[["if"]] <- pfa.expr.predicate(args[[1]])
                            out[["then"]] <- pfa.expr.then(args[[2]])
                            out[["else"]] <- NULL   # not required by PFA, but for conformance with R's return value of "if" expressions
                            out
                        }
                    }))

# if-else generates "if" with an "else" clause
converters.add(list(domain = function(name, args) { name == "if"  &&  length(args) == 3 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        symbols.predicate <- as.environment(as.list(symbols))
                        pfa.expr.predicate <- function (x) pfa.expr(x, symbols = symbols.predicate, cells = cells, pools = pools, fcns = fcns, env = env)
                        symbols.then <- as.environment(as.list(symbols))
                        pfa.expr.then <- function (x) pfa.expr(x, symbols = symbols.then, cells = cells, pools = pools, fcns = fcns, env = env)
                        symbols.else <- as.environment(as.list(symbols))
                        pfa.expr.else <- function (x) pfa.expr(x, symbols = symbols.else, cells = cells, pools = pools, fcns = fcns, env = env)

                        # ifnotnull special case
                        if (is.call(args[[1]])  &&  args[[1]][[1]] == "!"  &&  is.call(args[[1]][[2]])  &&  args[[1]][[2]][[1]] == "is.null"  &&  is.call(args[[1]][[2]][[2]])  &&  args[[1]][[2]][[2]][[1]] == "<-") {
                            asstr <- as.character(args[[1]][[2]][[2]][[2]])
                            symbols.then[[asstr]] <- asstr

                            assignments <- list()
                            assignments[[asstr]] <- pfa.expr.predicate(args[[1]][[2]][[2]][[3]])
                            out <- list()
                            out[["ifnotnull"]] <- assignments
                            out[["then"]] <- pfa.expr.then(args[[2]])
                            out[["else"]] <- pfa.expr.else(args[[3]])
                            out
                        }
                        else {
                            out <- list()
                            out[["if"]] <- pfa.expr.predicate(args[[1]])
                            out[["then"]] <- pfa.expr.then(args[[2]])
                            out[["else"]] <- pfa.expr.else(args[[3]])
                            out
                        }
                    }))

# while generates a "while" loop--- note R's lack of post-test loop
converters.add(list(domain = function(name, args) { name == "while"  &&  length(args) == 2 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        symbols.predicate <- as.environment(as.list(symbols))
                        pfa.expr.predicate <- function (x) pfa.expr(x, symbols = symbols.predicate, cells = cells, pools = pools, fcns = fcns, env = env)
                        symbols.body <- as.environment(as.list(symbols))
                        pfa.expr.body <- function (x) pfa.expr(x, symbols = symbols.body, cells = cells, pools = pools, fcns = fcns, env = env)

                        out <- list()
                        out[["while"]] <- pfa.expr.predicate(args[[1]])
                        out[["do"]] <- pfa.expr.body(args[[2]])
                        out
                    }))

# for loop over a low:high range generates a PFA "for" loop
converters.add(list(domain = function(name, args) { name == "for"  &&  length(args) == 3  &&  is.symbol(args[[1]])  &&  is.call(args[[2]])  &&  args[[2]][[1]] == ":" },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        index <- as.character(args[[1]])

                        symbols.init <- as.environment(as.list(symbols))
                        pfa.expr.init <- function (x) pfa.expr(x, symbols = symbols.init, cells = cells, pools = pools, fcns = fcns, env = env)
                        symbols.predicate <- as.environment(as.list(symbols))
                        pfa.expr.predicate <- function (x) pfa.expr(x, symbols = symbols.predicate, cells = cells, pools = pools, fcns = fcns, env = env)
                        symbols.body <- as.environment(as.list(symbols))
                        symbols.body[[index]] = index
                        pfa.expr.body <- function (x) pfa.expr(x, symbols = symbols.body, cells = cells, pools = pools, fcns = fcns, env = env)

                        init <- list()
                        init[[index]] <- pfa.expr.init(args[[2]][[2]])

                        predicate <- list()
                        predicate[["<="]] <- list(index, pfa.expr.predicate(args[[2]][[3]]))

                        plus <- list()
                        plus[["+"]] <- list(index, 1)
                        step <- list()
                        step[[index]] <- plus

                        out <- list()
                        out[["for"]] <- init
                        out[["while"]] <- predicate
                        out[["step"]] <- step

                        if (is.call(args[[3]])  &&  args[[3]][[1]] == "{")
                            out[["do"]] <- lapply(tail(as.list(args[[3]]), -1), pfa.expr.body)
                        else
                            out[["do"]] <- pfa.expr.body(args[[3]])

                        out
                    }))

# any other for loop generates a PFA "foreach" loop (if there's no dot in the name) or a PFA "forkeyval" loop (if there is one)
converters.add(list(domain = function(name, args) { name == "for"  &&  length(args) == 3  &&  is.symbol(args[[1]]) },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        indexes <- strsplit(as.character(args[[1]]), "\\.")[[1]]
                        index <- indexes[[1]]

                        symbols.array <- as.environment(as.list(symbols))
                        pfa.expr.array <- function (x) pfa.expr(x, symbols = symbols.array, cells = cells, pools = pools, fcns = fcns, env = env)
                        symbols.body <- as.environment(as.list(symbols))
                        symbols.body[[index]] = index
                        pfa.expr.body <- function (x) pfa.expr(x, symbols = symbols.body, cells = cells, pools = pools, fcns = fcns, env = env)

                        out <- list()
                        if (length(indexes) == 1) {
                            out[["foreach"]] <- index
                            out[["seq"]] <- TRUE
                        }
                        else if (length(indexes) == 2) {
                            out[["forkey"]] <- index
                            out[["forval"]] <- indexes[[2]]
                            symbols.body[[indexes[[2]]]] = indexes[[2]]
                        }
                        else
                            stop("too many dots in index of for loop")

                        out[["in"]] <- pfa.expr.array(args[[2]])

                        if (is.call(args[[3]])  &&  args[[3]][[1]] == "{")
                            out[["do"]] <- lapply(tail(as.list(args[[3]]), -1), pfa.expr.body)
                        else
                            out[["do"]] <- pfa.expr.body(args[[3]])

                        out
                    }))

# stop generates "error"
converters.add(list(domain = function(name, args) { name == "stop"  &&  length(args) == 1  &&  is.character(args[[1]]) },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        list(error = as.character(args[[1]]))
                    }))

# print generates a "do" mini-program to call "log" and return the logged value
converters.add(list(domain = function(name, args) { name == "print"  &&  length(args) == 1 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        symbols.nested <- as.environment(as.list(symbols))
                        pfa.expr.nested <- function (x) pfa.expr(x, symbols = symbols.nested, cells = cells, pools = pools, fcns = fcns, env = env)

                        tmpSymbol <- uniqueSymbolName(symbols)
                        assignment <- list()
                        assignment[[tmpSymbol]] <- pfa.expr.nested(args[[1]])

                        list(do = list(list(let = assignment), list(log = tmpSymbol), tmpSymbol))
                    }))

# allow user-defined functions
converters.add(list(domain = function(name, args) { substr(name, 1, 2) == "u." },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        if (substr(name, 3, 1000000L) %in% ls(fcns)) {
                            pfa.expr.nested <- function (x) pfa.expr(x, symbols = as.environment(as.list(symbols)), cells = cells, pools = pools, fcns = fcns, env = env)
                            out <- list()
                            out[[name]] <- lapply(args, pfa.expr.nested)
                            out
                        }
                        else
                            stop(paste("unrecognized user-defined function:", name))
                    }))

rToPFA <- list()

# lib1/core
rToPFA[["+"]] <- "+"
rToPFA[["-"]] <- function(args) {
    out <- list()
    if (length(args) == 1)
        out[["u-"]] <- args
    else
        out[["-"]] <- args
    out
}
rToPFA[["*"]] <- "*"
rToPFA[["/"]] <- "/"
rToPFA[["%%"]] <- "%"
rToPFA[["^"]] <- "**"
rToPFA[["=="]] <- "=="
rToPFA[[">="]] <- ">="
rToPFA[[">"]] <- ">"
rToPFA[["!="]] <- "!="
rToPFA[["<="]] <- "<="
rToPFA[["<"]] <- "<"
rToPFA[["&"]] <- "&&"
rToPFA[["&&"]] <- "&&"
rToPFA[["|"]] <- "||"
rToPFA[["||"]] <- "||"
rToPFA[["!"]] <- "!"

converters.add(list(domain = function(name, args) { name %in% names(rToPFA) },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        pfa.expr.nested <- function (x) pfa.expr(x, symbols = as.environment(as.list(symbols)), cells = cells, pools = pools, fcns = fcns, env = env)
                        evalArgs <- lapply(args, pfa.expr.nested)

                        pfaEquivalent <- rToPFA[[name]]
                        if (is.function(pfaEquivalent))
                            pfaEquivalent(evalArgs)
                        else {
                            out <- list()
                            out[[pfaEquivalent]] <- evalArgs
                            out
                        }
                    }))

