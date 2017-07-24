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

# extensible ruleset for converting R code into PFA
#' @keywords internal
converters <- list()
add_to_converters <- function(converter) {
    converters[[length(converters) + 1]] <<- converter
}

#' @keywords internal
convert_function <- function(name, args, symbols, cells, pools, fcns, env) {
    for (converter in converters)
        if (converter$domain(name, args))
            return(converter$apply(name, args, symbols, cells, pools, fcns, env))

    # after trying all converters, assume it's a literal PFA function
    pfa_expr_nested <- function (x) pfa_expr(x, symbols = as.environment(as.list(symbols)), cells = cells, pools = pools, fcns = fcns, env = env)
    out <- list()
    out[[name]] = lapply(args, pfa_expr_nested)
    out
}

# function objects are very peculiar because we need to add type annotations in a way that:
#     1. looks natural to the programmer
#     2. can be extracted from the AST
#     3. would produce otherwise-valid R code
# 
# so we require functions to look like any of the following:
#     function(. = avro_rettype) body
#     function(x = avro_type1, . = avro_rettype) body
#     function(x = avro_type1, y = avro_type2, . = avro_rettype) body
#     function(x = avro_type1 -> avro_rettype) body
#     function(x = avro_type1, y = avro_type2 -> avro_rettype) body
#     ...
# 
# PFA (and therefore this R subset) doesn't really have default arguments; every argument must
# always be supplied, and hence these avro objects won't really be used.  The R subset behaves
# exactly like the equivalent pfa_

#' convert_fcn
#' 
#' A function to convert R expressions to their PFA equivalent
#' 
#' @keywords internal
#' @importFrom utils tail
convert_fcn <- function(name, args, symbols, cells, pools, fcns, env) {
    params <- args[[1]]
    body <- args[[2]]

    symbols_body <- as.environment(as.list(symbols))

    out <- list(params = list())
    if ("." %in% names(params)) {
        out$ret <- eval(params[["."]], env)
        for (p in names(params))
            if (p != ".") {
                obj <- list()
                obj[[p]] <- eval(params[[p]], env)
                out$params[[length(out$params) + 1]] <- obj
                symbols_body[[p]] = p
            }
    }
    else if (length(params) > 0  &&  is.call(params[[length(params)]])  &&  params[[length(params)]][[1]] == "<-") {
        out$ret <- eval(params[[length(params)]][[2]], env)
        params[[length(params)]] <- params[[length(params)]][[3]]
        for (p in names(params)) {
            obj <- list()
            obj[[p]] <- eval(params[[p]], env)
            out$params[[length(out$params) + 1]] <- obj
            symbols_body[[p]] = p
        }
    }
    else
        stop("function definition is missing a return type (specified with a '.' argument or '->')")

    pfa_expr_body <- function (x) pfa_expr(x, symbols = symbols_body, cells = cells, pools = pools, fcns = fcns, env = env)

    if (is.call(body)  &&  body[[1]] == "{")
        out$do <- lapply(tail(as.list(body), -1), pfa_expr_body)
    else
        out$do <- pfa_expr_body(body)
    out
}

add_to_converters(list(domain = function(name, args) { name == "function"  &&  length(args) >= 2 },
                    apply  = convert_fcn))

# new generates literals, such as a "value" form, or a "new" form
add_to_converters(list(domain = function(name, args) { name == "new"  &&  length(args) >= 2 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        pfa_expr_nested <- function (x) pfa_expr(x, symbols = as.environment(as.list(symbols)), cells = cells, pools = pools, fcns = fcns, env = env)

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
                            list(type = type, new = lapply(tail(as.list(args), -1), pfa_expr_nested))
                    }))

# curly brackets generate "do" forms
add_to_converters(list(domain = function(name, args) { name == "{" },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        symbols_nested <- as.environment(as.list(symbols))
                        pfa_expr_nested <- function (x) pfa_expr(x, symbols = symbols_nested, cells = cells, pools = pools, fcns = fcns, env = env)
                        list(do = lapply(args, pfa_expr_nested))
                    }))

# parentheses don't generate anything: PFA is already Polish notation, no need for groupings
add_to_converters(list(domain = function(name, args) { name == "(" },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        pfa_expr(args[[1]], symbols = symbols, cells = cells, pools = pools, fcns = fcns, env = env)
                    }))

# assignments generate "let" for new symbols, "set" for old ones, "attr-to", "cell-to", or "pool-to" if subscripted
# note that subscripts generate "do" mini-programs to handle the difference in how R and PFA return values
add_to_converters(list(domain = function(name, args) { (name == "<-"  ||  name == "="  ||  name == "<<-")  &&  length(args) == 2 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        symbols_nested <- as.environment(as.list(symbols))
                        pfa_expr_nested <- function (x) pfa_expr(x, symbols = symbols_nested, cells = cells, pools = pools, fcns = fcns, env = env)

                        if (is.name(args[[1]])  &&  as.character(args[[1]]) %in% ls(cells)) {
                            if (name == "<-"  ||  name == "=")
                                stop(paste("use <<- for cells like", as.character(args[[1]]), ", not <- or ="))
                            list(cell = as.character(args[[1]]), to = pfa_expr_nested(args[[2]]))
                        }

                        else if (is.name(args[[1]])  &&  as.character(args[[1]]) %in% ls(pools))
                            stop("pool assignment must have a path")
                        
                        else if (is.name(args[[1]])) {
                            if (name == "<<-")
                                stop(paste("use <- for ordinary symbols like", as.character(args[[1]]), ", not <<-"))
                            asstr <- as.character(args[[1]])
                            out <- list()
                            out[[asstr]] <- pfa_expr_nested(args[[2]])
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
                                extractor <- convert_function(subname, subargs, symbols, cells, pools, fcns, env)
                                
                                if ("attr" %in% names(extractor)) {
                                    if (name == "<<-")
                                        stop(paste("use <- for ordinary symbols like", extractor$attr, ", not <<-"))

                                    tmpSymbol <- gen_unique_symb_name(symbols)
                                    extractor$to <- tmpSymbol

                                    assignment <- list()
                                    assignment[[tmpSymbol]] <- pfa_expr_nested(args[[2]])

                                    secondAssignment <- list()
                                    secondAssignment[[extractor$attr]] <- extractor

                                    list(do = list(list(let = assignment), list(set = secondAssignment), tmpSymbol))
                                }
                                else if ("cell" %in% names(extractor)) {
                                    if (name == "<-"  ||  name == "=")
                                        stop(paste("use <<- for cells like", extractor$cell, ", not <- or ="))
                                    extractor$to <- pfa_expr_nested(args[[2]])
                                    extractor
                                }
                                else if ("pool" %in% names(extractor)) {
                                    if (name == "<-"  ||  name == "=")
                                        stop(paste("use <<- for pools like", extractor$pool, ", not <- or ="))

                                    symbols_init <- as.environment(as.list(symbols))
                                    pfa_expr_init <- function (x) pfa_expr(x, symbols = symbols_init, cells = cells, pools = pools, fcns = fcns, env = env)

                                    if (is.call(args[[2]])  &&  args[[2]][[1]] == "function"  &&  is.call(args[[2]][[3]])  &&  args[[2]][[3]][[1]] == "<-") {
                                        to <- as.call(list(args[[2]][[1]], args[[2]][[2]], args[[2]][[3]][[2]], args[[2]][[4]]))
                                        extractor$to <- pfa_expr_nested(to)
                                        extractor$init <- pfa_expr_init(args[[2]][[3]][[3]])
                                        extractor
                                    }
                                    else if (is.call(args[[2]])  &&  args[[2]][[1]] == "<-") {
                                        to <- args[[2]][[2]]
                                        extractor$to <- pfa_expr_nested(to)
                                        extractor$init <- pfa_expr_init(args[[2]][[3]])
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

unwrap_brackets <- function(path, expr) {
    if (is.call(expr)) {
        name <- as.character(expr[[1]])
        args <- tail(as.list(expr), -1)
        if (name == "["  &&  length(args) == 2) {
            ## auto-minus-one is dangerous
            ## path <- c(as.call(list("-", args[[2]], 1)), path)
            path <- c(args[[2]], path)
            path <- unwrap_brackets(path, args[[1]])
        }
        else if (name == "[["  &&  length(args) == 2) {
            path <- c(args[[2]], path)
            path <- unwrap_brackets(path, args[[1]])
        }
        else if (name == "$"  &&  length(args) == 2) {
            path <- c(as.character(args[[2]]), path)
            path <- unwrap_brackets(path, args[[1]])
        }
        else
            path <- c(expr, path)
    }
    else
        path <- c(expr, path)
    path
}

# subscripts (outside of an assignment) generate "attr", "cell", or "pool"
add_to_converters(list(domain = function(name, args) { (name == "["  ||  name == "[["  ||  name == "$")  &&  length(args) == 2 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        expr <- as.call(c(as.symbol(name), args))
                        path <- unwrap_brackets(list(), expr)
                        first <- path[[1]]
                        rest <- tail(path, -1)

                        pfa_expr_path <- function (x) pfa_expr(x, symbols = as.environment(as.list(symbols)), cells = cells, pools = pools, fcns = fcns, env = env)

                        if (is.name(first)  &&  as.character(first) %in% ls(cells))
                            list(cell = as.character(first), path = lapply(rest, pfa_expr_path))
                        else if (is.name(first)  &&  as.character(first) %in% ls(pools))
                            list(pool = as.character(first), path = lapply(rest, pfa_expr_path))
                        else {
                            symbols_expr <- as.environment(as.list(symbols))
                            pfa_expr_expr <- function (x) pfa_expr(x, symbols = symbols_expr, cells = cells, pools = pools, fcns = fcns, env = env)
                            list(attr = pfa_expr_expr(first), path = lapply(rest, pfa_expr_path))
                        }
                    }))

# if statements generate "if" with an "else" clause (R returns a union for the 
# no-else case, so PFA is modified to match) no attempt is made to unwind nested
# "if" into "cond" ifnotnull special case: 
# if (!is.null(x <- expr)) { something } else { whatever }
add_to_converters(list(domain = function(name, args) { name == "if"  &&  length(args) == 2 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        symbols_predicate <- as.environment(as.list(symbols))
                        pfa_expr_predicate <- function (x) pfa_expr(x, symbols = symbols_predicate, cells = cells, pools = pools, fcns = fcns, env = env)
                        symbols_then <- as.environment(as.list(symbols))
                        pfa_expr_then <- function (x) pfa_expr(x, symbols = symbols_then, cells = cells, pools = pools, fcns = fcns, env = env)

                        # ifnotnull special case
                        if (is.call(args[[1]])  &&  args[[1]][[1]] == "!"  &&  is.call(args[[1]][[2]])  &&  args[[1]][[2]][[1]] == "is.null"  &&  is.call(args[[1]][[2]][[2]])  &&  args[[1]][[2]][[2]][[1]] == "<-") {
                            asstr <- as.character(args[[1]][[2]][[2]][[2]])
                            symbols_then[[asstr]] <- asstr

                            assignments <- list()
                            assignments[[asstr]] <- pfa_expr_predicate(args[[1]][[2]][[2]][[3]])
                            out <- list()
                            out[["ifnotnull"]] <- assignments
                            out[["then"]] <- pfa_expr_then(args[[2]])
                            out[["else"]] <- NULL
                            out
                        }
                        else {
                            out <- list()
                            out[["if"]] <- pfa_expr_predicate(args[[1]])
                            out[["then"]] <- pfa_expr_then(args[[2]])
                            out[["else"]] <- NULL   # not required by PFA, but for conformance with R's return value of "if" expressions
                            out
                        }
                    }))

# if-else generates "if" with an "else" clause
add_to_converters(list(domain = function(name, args) { name == "if"  &&  length(args) == 3 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        symbols_predicate <- as.environment(as.list(symbols))
                        pfa_expr_predicate <- function (x) pfa_expr(x, symbols = symbols_predicate, cells = cells, pools = pools, fcns = fcns, env = env)
                        symbols_then <- as.environment(as.list(symbols))
                        pfa_expr_then <- function (x) pfa_expr(x, symbols = symbols_then, cells = cells, pools = pools, fcns = fcns, env = env)
                        symbols_else <- as.environment(as.list(symbols))
                        pfa_expr_else <- function (x) pfa_expr(x, symbols = symbols_else, cells = cells, pools = pools, fcns = fcns, env = env)

                        # ifnotnull special case
                        if (is.call(args[[1]])  &&  args[[1]][[1]] == "!"  &&  is.call(args[[1]][[2]])  &&  args[[1]][[2]][[1]] == "is.null"  &&  is.call(args[[1]][[2]][[2]])  &&  args[[1]][[2]][[2]][[1]] == "<-") {
                            asstr <- as.character(args[[1]][[2]][[2]][[2]])
                            symbols_then[[asstr]] <- asstr

                            assignments <- list()
                            assignments[[asstr]] <- pfa_expr_predicate(args[[1]][[2]][[2]][[3]])
                            out <- list()
                            out[["ifnotnull"]] <- assignments
                            out[["then"]] <- pfa_expr_then(args[[2]])
                            out[["else"]] <- pfa_expr_else(args[[3]])
                            out
                        }
                        else {
                            out <- list()
                            out[["if"]] <- pfa_expr_predicate(args[[1]])
                            out[["then"]] <- pfa_expr_then(args[[2]])
                            out[["else"]] <- pfa_expr_else(args[[3]])
                            out
                        }
                    }))

# while generates a "while" loop--- note R's lack of post-test loop
add_to_converters(list(domain = function(name, args) { name == "while"  &&  length(args) == 2 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        symbols_predicate <- as.environment(as.list(symbols))
                        pfa_expr_predicate <- function (x) pfa_expr(x, symbols = symbols_predicate, cells = cells, pools = pools, fcns = fcns, env = env)
                        symbols_body <- as.environment(as.list(symbols))
                        pfa_expr_body <- function (x) pfa_expr(x, symbols = symbols_body, cells = cells, pools = pools, fcns = fcns, env = env)

                        out <- list()
                        out[["while"]] <- pfa_expr_predicate(args[[1]])
                        out[["do"]] <- pfa_expr_body(args[[2]])
                        out
                    }))

# castcase generates a cast-case 
add_to_converters(list(domain = function(name, args) { name == "castcase"  &&  length(args) == 3 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                      
                      named_symbol <- as.list(args[[2]])
                      names(named_symbol) <- args[[2]]
                      symbols_body <- c(as.list(symbols), named_symbol)
                      pfa_expr_body <- function (x) pfa_expr(x, symbols = symbols_body, cells = cells, pools = pools, fcns = fcns, env = env)
                      
                      out <- list()
                      out[["as"]] <- args[[1]]
                      out[["named"]] <- args[[2]]
                      out[["do"]] <- pfa_expr_body(args[[3]])
                      return(out)
                    }))

# castblock generates a cast block of cast cases
add_to_converters(list(domain = function(name, args) { name == "castblock"  &&  length(args) == 3 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {

                        symbols_body <- as.environment(as.list(symbols))
                        pfa_expr_body <- function (x) pfa_expr(x, symbols = symbols_body, cells = cells, pools = pools, fcns = fcns, env = env)
                        
                        out <- list()
                        out[["cast"]] <- pfa_expr_body(args[[1]])
                        out[["cases"]] <- pfa_expr_body(args[[2]])
                        out[["partial"]] <- pfa_expr_body(args[[3]])
                        out
                    }))

add_to_converters(list(domain = function(name, args) { name == "list" },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                      
                      symbols_body <- as.environment(as.list(symbols))
                      pfa_expr_body <- function (x) pfa_expr(x, symbols = symbols_body, cells = cells, pools = pools, fcns = fcns, env = env)
                      
                      out <- list()
                      for(i in seq.int(length(args))){
                        out[[i]] <- pfa_expr_body(args[[i]])
                      }
                      names(out) <- names(args)
                      
                      out
                    }))

# for loop over a low:high range generates a PFA "for" loop
add_to_converters(list(domain = function(name, args) { name == "for"  &&  length(args) == 3  &&  is.symbol(args[[1]])  &&  is.call(args[[2]])  &&  args[[2]][[1]] == ":" },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        index <- as.character(args[[1]])

                        symbols_init <- as.environment(as.list(symbols))
                        pfa_expr_init <- function (x) pfa_expr(x, symbols = symbols_init, cells = cells, pools = pools, fcns = fcns, env = env)
                        symbols_predicate <- as.environment(as.list(symbols))
                        pfa_expr_predicate <- function (x) pfa_expr(x, symbols = symbols_predicate, cells = cells, pools = pools, fcns = fcns, env = env)
                        symbols_body <- as.environment(as.list(symbols))
                        symbols_body[[index]] = index
                        pfa_expr_body <- function (x) pfa_expr(x, symbols = symbols_body, cells = cells, pools = pools, fcns = fcns, env = env)

                        init <- list()
                        init[[index]] <- pfa_expr_init(args[[2]][[2]])

                        predicate <- list()
                        predicate[["<="]] <- list(index, pfa_expr_predicate(args[[2]][[3]]))

                        plus <- list()
                        plus[["+"]] <- list(index, 1)
                        step <- list()
                        step[[index]] <- plus

                        out <- list()
                        out[["for"]] <- init
                        out[["while"]] <- predicate
                        out[["step"]] <- step

                        if (is.call(args[[3]])  &&  args[[3]][[1]] == "{")
                            out[["do"]] <- lapply(tail(as.list(args[[3]]), -1), pfa_expr_body)
                        else
                            out[["do"]] <- pfa_expr_body(args[[3]])

                        out
                    }))

# any other for loop generates a PFA "foreach" loop (if there's no dot in the name) or a PFA "forkeyval" loop (if there is one)
add_to_converters(list(domain = function(name, args) { name == "for"  &&  length(args) == 3  &&  is.symbol(args[[1]]) },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        indexes <- strsplit(as.character(args[[1]]), "\\.")[[1]]
                        index <- indexes[[1]]

                        symbols_array <- as.environment(as.list(symbols))
                        pfa_expr_array <- function (x) pfa_expr(x, symbols = symbols_array, cells = cells, pools = pools, fcns = fcns, env = env)
                        symbols_body <- as.environment(as.list(symbols))
                        symbols_body[[index]] = index
                        pfa_expr_body <- function (x) pfa_expr(x, symbols = symbols_body, cells = cells, pools = pools, fcns = fcns, env = env)

                        out <- list()
                        if (length(indexes) == 1) {
                            out[["foreach"]] <- index
                            out[["seq"]] <- TRUE
                        }
                        else if (length(indexes) == 2) {
                            out[["forkey"]] <- index
                            out[["forval"]] <- indexes[[2]]
                            symbols_body[[indexes[[2]]]] = indexes[[2]]
                        }
                        else
                            stop("too many dots in index of for loop")

                        out[["in"]] <- pfa_expr_array(args[[2]])

                        if (is.call(args[[3]])  &&  args[[3]][[1]] == "{")
                            out[["do"]] <- lapply(tail(as.list(args[[3]]), -1), pfa_expr_body)
                        else
                            out[["do"]] <- pfa_expr_body(args[[3]])

                        out
                    }))

# stop generates "error"
add_to_converters(list(domain = function(name, args) { name == "stop"  &&  length(args) == 1  &&  is.character(args[[1]]) },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        list(error = as.character(args[[1]]))
                    }))

# print generates a "do" mini-program to call "log" and return the logged value
add_to_converters(list(domain = function(name, args) { name == "print"  &&  length(args) == 1 },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        symbols_nested <- as.environment(as.list(symbols))
                        pfa_expr_nested <- function (x) pfa_expr(x, symbols = symbols_nested, cells = cells, pools = pools, fcns = fcns, env = env)

                        tmpSymbol <- gen_unique_symb_name(symbols)
                        assignment <- list()
                        assignment[[tmpSymbol]] <- pfa_expr_nested(args[[1]])

                        list(do = list(list(let = assignment), list(log = tmpSymbol), tmpSymbol))
                    }))

# allow user-defined functions
add_to_converters(list(domain = function(name, args) { substr(name, 1, 2) == "u." },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        if (substr(name, 3, 1000000L) %in% ls(fcns)) {
                            pfa_expr_nested <- function (x) pfa_expr(x, symbols = as.environment(as.list(symbols)), cells = cells, pools = pools, fcns = fcns, env = env)
                            out <- list()
                            out[[name]] <- lapply(args, pfa_expr_nested)
                            out
                        }
                        else
                            stop(paste("unrecognized user-defined function:", name))
                    }))

r_to_pfa <- list()
# lib1/core
r_to_pfa[["+"]] <- "+"
r_to_pfa[["-"]] <- function(args) {
    out <- list()
    if (length(args) == 1)
        out[["u-"]] <- args
    else
        out[["-"]] <- args
    out
}
r_to_pfa[["*"]] <- "*"
r_to_pfa[["/"]] <- "/"
r_to_pfa[["%%"]] <- "%"
r_to_pfa[["^"]] <- "**"
r_to_pfa[["=="]] <- "=="
r_to_pfa[[">="]] <- ">="
r_to_pfa[[">"]] <- ">"
r_to_pfa[["!="]] <- "!="
r_to_pfa[["<="]] <- "<="
r_to_pfa[["<"]] <- "<"
r_to_pfa[["&"]] <- "&&"
r_to_pfa[["&&"]] <- "&&"
r_to_pfa[["|"]] <- "||"
r_to_pfa[["||"]] <- "||"
r_to_pfa[["!"]] <- "!"

add_to_converters(list(domain = function(name, args) { name %in% names(r_to_pfa) },
                    apply  = function(name, args, symbols, cells, pools, fcns, env) {
                        pfa_expr_nested <- function (x) pfa_expr(x, symbols = as.environment(as.list(symbols)), cells = cells, pools = pools, fcns = fcns, env = env)
                        eval_args <- lapply(args, pfa_expr_nested)

                        pfa_equivalent <- r_to_pfa[[name]]
                        if (is.function(pfa_equivalent))
                            pfa_equivalent(eval_args)
                        else {
                            out <- list()
                            out[[pfa_equivalent]] <- eval_args
                            out
                        }
                    }))

r_funs_to_pfa <- list()
r_funs_to_pfa[["log"]] <- "m.log"
r_funs_to_pfa[["log2"]] <- "m.log"
r_funs_to_pfa[["logb"]] <- "m.log"
r_funs_to_pfa[["log10"]] <- "m.log10"
r_funs_to_pfa[["log1p"]] <- "m.ln1p"
r_funs_to_pfa[["exp"]] <- "m.exp"
r_funs_to_pfa[["expm1"]] <- "m.expm1"
r_funs_to_pfa[["abs"]] <- "m.abs"
r_funs_to_pfa[["sqrt"]] <- "m.sqrt"
r_funs_to_pfa[["sin"]] <- "m.sin"
r_funs_to_pfa[["cos"]] <- "m.cos"
r_funs_to_pfa[["tan"]] <- "m.tan"
r_funs_to_pfa[["asin"]] <- "m.asin"
r_funs_to_pfa[["acos"]] <- "m.acos"
r_funs_to_pfa[["atan"]] <- "m.atan"
r_funs_to_pfa[["sinh"]] <- "m.sinh"
r_funs_to_pfa[["cosh"]] <- "m.cosh"
r_funs_to_pfa[["tanh"]] <- "m.tanh"
r_funs_to_pfa[["floor"]] <- "m.floor"
r_funs_to_pfa[["ceiling"]] <- "m.ceil"
r_funs_to_pfa[["round"]] <- "m.round" 

# r_funs_to_pfa[["trunc"]] <- "" # if(x >= 0) m.floor else m.ceil
# r_funs_to_pfa[["signif"]] <- ""
# D = 10^(d-ceil(log10(x)))
# round(x*D)/D
# r_funs_to_pfa[["asinh"]] <- "m.asin"
# r_funs_to_pfa[["acosh"]] <- "m.acos"
# r_funs_to_pfa[["atanh"]] <- "m.atan"
# r_funs_to_pfa[["sinpi"]] <- "m.sin" # mutiplied by pi in the converter (m.pi * x)
# r_funs_to_pfa[["cospi"]] <- "m.cos" # mutiplied by pi in the converter (m.pi * x)
# r_funs_to_pfa[["tanpi"]] <- "m.tan" # mutiplied by pi in the converter (m.pi * x)
# atan2(y, x)
# min - ? a.min
# max - ? a.max

add_to_converters(list(domain = function(name, args) { name %in% names(r_funs_to_pfa) },
                       apply  = function(name, args, symbols, cells, pools, fcns, env) { 
                         
                           # handle special cases 
                           if(name %in% c('log', 'logb') && length(args) == 1){
                             pfa_equivalent <- 'm.ln'
                           } else if(name == 'log2' && length(args) == 1){
                             pfa_equivalent <- 'm.log'
                             args[[2]] <- 2
                           } else {
                             pfa_equivalent <- r_funs_to_pfa[[name]]
                           }
                         
                           pfa_expr_nested <- function (x) pfa_expr(x, symbols = as.environment(as.list(symbols)), cells = cells, pools = pools, fcns = fcns, env = env)
                           eval_args <- lapply(args, pfa_expr_nested)
 
                           if (is.function(pfa_equivalent))
                             pfa_equivalent(eval_args)
                           else {
                             out <- list()
                             out[[pfa_equivalent]] <- eval_args
                             out
                           }
                         }
                       )
                  )

#' unjson
#'
#' Convert a JSON string in memory or a JSON file on disk into a list-of-lists structure.
#' @param x input string or connection (if not open, this function opens it)
#' @return a \code{list} of lists
#' @keywords internal
unjson <- function(x) {
    if (is.character(x)  &&  length(x) == 1) {
        x <- strsplit(x, "", useBytes = TRUE)[[1]]
        i <- 0
        getNext <- function() {
            if (i < length(x)) {
                out <- x[i : i+1]
                i <<- i + 1
                out
            }
            else
                character(0)
        }
        getIndex <- function() { i }
        rewind <- function() { i <<- i - 1 }
    }
    else if (is(x, "connection")) {
        if (!isOpen(x))
            open(x)
        i <- 0
        useStorage <- FALSE
        storage <- NULL
        getNext <- function() {
            i <- i + 1
            if (useStorage) {
                useStorage <<- FALSE
                storage
            }
            else {
                storage <<- readChar(x, 1, useBytes = TRUE)
                storage
            }
        }
        getIndex <- function() { i }
        rewind <- function() {
            i <<- i - 1
            useStorage <<- TRUE
        }
    }
    else
        stop("unjson requires a string or a connection (file)")

    out <- parseValue(getNext, getIndex, rewind)
    while (length(y <- getNext()) != 0)
        if (y != " "  &&  y != "\t"  &&  y != "\n"  &&  y != "\r")
            stop(paste("unexpected character", y, "after JSON terminates at index", getIndex()))
    out
}

#' @keywords internal
parseValue <- function(getNext, getIndex, rewind) {
    x <- getNext()
    if (length(x) == 0)
        stop("end of JSON file or string while expecting value")
    else if (x == "n") {
        if (getNext() != "u"  ||  getNext() != "l"  ||  getNext() != "l")
            stop(paste("token starting with \"n\" is not \"null\" at index", getIndex()))
        NULL
    }
    else if (x == "t") {
        if (getNext() != "r"  ||  getNext() != "u"  ||  getNext() != "e")
            stop(paste("token starting with \"t\" is not \"true\" at index", getIndex()))
        TRUE
    }
    else if (x == "f") {
        if (getNext() != "a"  ||  getNext() != "l"  ||  getNext() != "s"  ||  getNext() != "e")
            stop(paste("token starting with \"f\" is not \"false\" at index", getIndex()))
        FALSE
    }
    else if (x == "-"  ||  (x >= "0"  &&  x <= "9"))
        parseNumber(x, getNext, getIndex, rewind)
    else if (x == "\"")
        parseString(getNext, getIndex, rewind)
    else if (x == "{")
        parseObject(getNext, getIndex, rewind)
    else if (x == "[")
        parseArray(getNext, getIndex, rewind)
    else if (x == " "  ||  x == "\t"  ||  x == "\n"  ||  x == "\r")
        parseValue(getNext, getIndex, rewind)
    else
        stop(paste("unexpected character", x, "at index", getIndex()))
}

#' @keywords internal
parseNumber <- function(x, getNext, getIndex, rewind) {
    y <- character(0)
    while (length(x) != 0  &&  (x == "-"  ||  x == "+"  ||  x == "."  ||  x == "e"  ||  x == "E"  ||  (x >= "0"  &&  x <= "9"))) {
        y <- c(y, x)
        x <- getNext()
    }
    rewind()
    y <- paste(y, collapse = "")
    out <- as.numeric(y)
    if (is.na(out))
        stop(paste("bad number", y, "at index", getIndex()))
    out
}

#' @keywords internal
parseString <- function(getNext, getIndex, rewind) {
    y <- character(0)
    x <- getNext()
    while (length(x) != 0  &&  x != "\"") {
        if (x == "\\") {
            x <- getNext()
            if (x == "\"")
                y <- c(y, "\x22")               # turn \" into " (0x22)
            else if (x == "\\")
                y <- c(y, "\x5c")               # turn \\ into \ (0x5c)
            else if (x == "/")
                y <- c(y, "\x2f")               # turn \/ into / (0x2f)
            else if (x == "b")
                y <- c(y, "\x08")               # turn \b into backspace (0x08)
            else if (x == "f")
                y <- c(y, "\x0c")               # turn \f into formfeed (0x0c)
            else if (x == "n")
                y <- c(y, "\x0a")               # turn \n into newline (0x0a)
            else if (x == "r")
                y <- c(y, "\x0d")               # turn \r into carriage return (0x0d)
            else if (x == "t")
                y <- c(y, "\x09")               # turn \t into horizontal tab (0x09)
            else if (x == "u")
                y <- c(y, eval(parse(text = paste(c("\"", "\\", "u", getNext(), getNext(), getNext(), getNext(), "\""), collapse = ""))))
            else
                stop(paste("unrecognized backslash sequence at index", getIndex()))
        }
        else
            y <- c(y, x)
        x <- getNext()
    }
    paste(y, collapse = "")
}

#' @keywords internal
parseObject <- function(getNext, getIndex, rewind) {
    out <- json_map()
    repeat {
        commas <- 0
        while (length(x <- getNext()) != 0  &&  x != "\""  &&  x != "}") {
            if (x == ",") {
                if (length(out) == 0)
                    stop(paste("comma before first object key at index", getIndex()))
                commas <- commas + 1
            }
            else if (x != " "  &&  x != "\t"  &&  x != "\n"  &&  x != "\r")
                stop(paste("unrecognized character", x, "while expecting object key at index", getIndex()))
        }

        if (length(x) == 0)
            stop("end of JSON file or string while expecting object key")

        else if (x == "}") {
            if (commas != 0)
                stop(paste("trailing comma at end of object at index", getIndex()))
            break
        }

        else if (x == "\"") {
            if (commas >= 2)
                stop(paste("too many commas before key at index", getIndex()))
            else if (commas != 1  &&  length(out) > 0)
                stop(paste("missing comma between key-value pairs at index", getIndex()))

            key <- parseString(getNext, getIndex, rewind)

            while (length(x <- getNext()) != 0  &&  x != ":")
                if (x != " "  &&  x != "\t"  &&  x != "\n"  &&  x != "\r")
                    stop(paste("unrecognized character", x, "while expecting colon at index", getIndex()))

            if (length(x) == 0)
                stop("end of JSON file or string while expecting object value")

            out[[key]] <- parseValue(getNext, getIndex, rewind)
        }
    }
    out
}

#' @keywords internal
parseArray <- function(getNext, getIndex, rewind) {
    out <- json_array()

    while (length(x <- getNext()) != 0  &&  x != "]")
        if (x != " "  &&  x != "\t"  &&  x != "\n"  &&  x != "\r") {
            rewind()
            break
        }

    if (x != "]")
        repeat {
            out[[length(out) + 1]] <- parseValue(getNext, getIndex, rewind)

            while (length(x <- getNext()) != 0  &&  x != ","  &&  x != "]")
                if (x != " "  &&  x != "\t"  &&  x != "\n"  &&  x != "\r")
                    stop(paste("unrecognized character", x, "while expecting array delimiter at index", getIndex()))

            if (length(x) == 0)
                stop("end of JSON file or string while expecting array delimiter")

            else if (x == "]")
                break
        }
    out
}