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

# lists-to-json        json(x)
# json-to-lists        unjson(x)
# JSON objects         json.map()
# JSON arrays          json.array()

# get fullname         avro.fullName(type)
# organize types       avro.typemap(types)
# data frame to types  avro.fromFrame(dataFrame)

# AvroNull             avro.null
# AvroBoolean          avro.boolean
# AvroInt              avro.int
# AvroLong             avro.long
# AvroFloat            avro.float
# AvroDouble           avro.double
# AvroBytes            avro.bytes
# AvroFixed            avro.fixed(size, name = NULL, namespace = NULL)
# AvroString           avro.string
# AvroEnum             avro.enum(c("one", "two", "three"), name = NULL, namespace = NULL)
# AvroArray            avro.array(items)
# AvroMap              avro.map(values)
# AvroRecord           avro.record(list(field1 = type1, field2 = type2), name = NULL, namespace = NULL)
# AvroUnion            avro.union(type1, type2)

# any quoted expr      pfa.expr(expr, symbols = list(), cells = list(), pools = list(), fcns = list(), env = parent.frame())
# substitution         symbol name not in scope, cells, pools, or fcns, but is in env
# run in Titus         pfa.engine(config)

# EngineConfig         pfa.config(input, output, action, name = NULL, method = NULL, begin = NULL, end = NULL, fcns = NULL, zero = NULL, merge = NULL, cells = NULL, pools = NULL, randseed = NULL, doc = NULL, version = NULL, metadata = NULL, options = NULL, env = parent.frame())
# Cell                 pfa.cell(type, init, shared = FALSE, rollback = FALSE)
# Pool                 pfa.pool(type, init, shared = FALSE, rollback = FALSE)
# FcnDef               function(x = avro.type1, y = avro.type2, . = avro.rettype) { body }
# FcnRef               symbol name in fcns
# FcnRefFill           **FIXME**
# CallUserFcn          **FIXME**
# Call                 something(with, args) or something()
# Ref                  symbol name in scope
# LiteralNull          NULL
# LiteralBoolean       TRUE or FALSE
# LiteralInt           12 or new(avro.int, 12)
# LiteralLong          new(avro.long, 12)
# LiteralFloat         new(avro.float, 12.4)
# LiteralDouble        12.4 or new(avro.double, 12)
# LiteralString        "something"
# LiteralBase64        **FIXME**
# Literal              new(type, "{json: string}")
# NewObject            new(type, field1 = 1, field2 = 2 + 2)
# NewArray             new(type, 1, 2 + 2)
# Do                   { curly braces }
# Let                  newsymb <- something
# SetVar               oldxymb <- something
# AttrGet              some[thing] or some[[thing]]
# AttrTo               some[thing] or some[[thing]] <- value
# CellGet              symbol in cells or cell[thing] or cell[[thing]]
# CellTo               cell[thing] or cell[[thing]] <<- value
# PoolGet              symbol in pools or pool[thing] or pool[[thing]]
# PoolTo               pool[thing] or pool[[thing]] <<- value
# If                   if (this) { that } else { whatever }
# Cond                 **FIXME** (else-if chains are left as deep trees, not rolled up into a cond)
# While                while (predicate) { body }
# DoUntil              **FIXME**
# For                  for (i in start:end) { body }
# Foreach              for (i in array) { body }
# Forkeyval            for (k.v in array) { body }
# CastCase             **FIXME**
# CastBlock            **FIXME**
# Upcast               **FIXME**
# IfNotNull            if (!is.null(x <- expr)) { something } else { whatever }
# Unpack               **FIXME**
# Doc                  **FIXME**
# Error                stop("some reason")
# Try                  **FIXME**
# Log                  print(something)

json <- function(x, fileName = NULL, newline = TRUE, spaces = TRUE, sigfigs = NULL, stringsAsIs = FALSE) {
    if (!is.null(fileName)) {
        outFile <- file(fileName)
        open(outFile, open = "wt")
        json.write(x, outFile, spaces, sigfigs, stringsAsIs)
        if (newline)
            writeLines("", con = outFile, sep = "\n")
        close(outFile)
    }
    else {
        out <- paste(json.string(x, spaces, sigfigs, stringsAsIs), collapse = "")
        if (newline)
            out <- paste(out, "", sep = "\n")
        out
    }
}

json.string <- function(x, spaces = TRUE, sigfigs = NULL, stringsAsIs = FALSE) {
    if (is.null(x))
        "null"
    else if (is.logical(x)  &&  x)
        "true"
    else if (is.logical(x)  &&  !x)
        "false"
    else if (is.numeric(x)  &&  length(x) == 1) {
        if (is.null(sigfigs))
            as.character(x)
        else
            as.character(signif(x, sigfigs))
    }
    else if (is.character(x)  &&  length(x) == 1) {
        if (stringsAsIs)
            paste("\"", x, "\"", sep = "")
        else
            paste("\"", gsub("\\x22", "\\\\\"",     # step 7: turn " (0x22) into \"
                        gsub("\\x08", "\\\\b",      # step 6: turn backspace (0x08) into \b
                        gsub("\\x0c", "\\\\f",      # step 5: turn formfeed (0x0c) into \f
                        gsub("\\x0a", "\\\\n",      # step 4: turn newline (0x0a) into \n
                        gsub("\\x0d", "\\\\r",      # step 3: turn carriage return (0x0d) into \r
                        gsub("\\x09", "\\\\t",      # step 2: turn horizontal tab (0x09) into \t
                        gsub("\\x5c", "\\\\\\\\",   # step 1: turn \ (0x5c) into \\
                             x))))))), "\"", sep = "")
    }
    else if (is.list(x)  &&  is.null(names(x))) {
        comma <- if (spaces) ", " else ","
        withcommas <- unlist(lapply(x, function(v) c(comma, json.string(v, spaces, sigfigs, stringsAsIs))))
        if (length(withcommas) > 0)
            withcommas <- withcommas[2:length(withcommas)]
        c("[", withcommas, "]")
    }
    else if (is.list(x)) {
        comma <- if (spaces) ", " else ","
        colon <- if (spaces) ": " else ":"
        withcommas <- unlist(lapply(names(x), function(n) c(comma, json.string(n), colon, json.string(x[[n]], spaces, sigfigs, stringsAsIs))))
        if (length(withcommas) > 0)
            withcommas <- withcommas[2:length(withcommas)]
        c("{", withcommas, "}")
    }
    else
        stop(paste("cannot convert object of class", class(x), "length", length(x), "to JSON", sep=" "))
}

json.write <- function(x, outFile, spaces = TRUE, sigfigs = NULL, stringsAsIs = FALSE) {
    if (is.list(x)  &&  is.null(names(x))) {
        writeLines("[", con = outFile, sep = "")
        first <- TRUE
        for (v in x) {
            if (!first) {
                if (spaces)
                    writeLines(", ", con = outFile, sep = "")
                else
                    writeLines(",", con = outFile, sep = "")
            }
            else
                first <- FALSE
            json.write(v, outFile, spaces, sigfigs, stringsAsIs)
        }
        writeLines("]", con = outFile, sep = "")
    }
    else if (is.list(x)) {
        writeLines("{", con = outFile, sep = "")
        first <- TRUE
        for (n in names(x)) {
            if (!first) {
                if (spaces)
                    writeLines(", ", con = outFile, sep = "")
                else
                    writeLines(",", con = outFile, sep = "")
            }
            else
                first <- FALSE
            json.write(n, outFile, spaces, sigfigs, stringsAsIs)
            if (spaces)
                writeLines(": ", con = outFile, sep = "")
            else
                writeLines(":", con = outFile, sep = "")
            json.write(x[[n]], outFile, spaces, sigfigs, stringsAsIs)
        }
        writeLines("}", con = outFile, sep = "")
    }
    else {
        writeLines(json.string(x, spaces, sigfigs, stringsAsIs), con = outFile, sep = "")
    }
}

# wrap the fromJSON function with our favorite arguments
unjson <- function(x) {
    library(RJSONIO)
    fromJSON(x, simplify = FALSE)
}

# uniform method to create named lists (which translate to json maps)
json.map <- function(...) {
    args <- list(...)
    if (length(args) == 0)
        setNames(list(), list())
    else if (is.null(names(args)))
        stop("json.map must have named parameters")
    else
        args
}

# uniform method to create unnamed lists (which translate to json arrays)
json.array <- function(...) {
    args <- list(...)
    if (!is.null(names(args)))
        stop("json.array must not have named parameters")
    else
        args
}

pythonEngineNameCounter <- 0
pfa.engine <- function(config, tempFile = NULL) {
    library(RJSONIO)
    library(rPython)

    pythonEngineNameCounter <<- pythonEngineNameCounter + 1
    name <- paste("engine_", pythonEngineNameCounter, sep = "")
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

engineNameCounter <- 0
uniqueEngineName <- function() {
    engineNameCounter <<- engineNameCounter + 1
    paste("Engine_", engineNameCounter, sep = "")
}

recordNameCounter <- 0
uniqueRecordName <- function() {
    recordNameCounter <<- recordNameCounter + 1
    paste("Record_", recordNameCounter, sep = "")
}

enumNameCounter <- 0
uniqueEnumName <- function() {
    enumNameCounter <<- enumNameCounter + 1
    paste("Enum_", enumNameCounter, sep = "")
}

fixedNameCounter <- 0
uniqueFixedName <- function() {
    fixedNameCounter <<- fixedNameCounter + 1
    paste("Fixed_", fixedNameCounter, sep = "")
}

symbolNameCounter <- 0
uniqueSymbolName <- function(symbols) {
    while (TRUE) {
        symbolNameCounter <<- symbolNameCounter + 1
        test <- paste("tmp_", symbolNameCounter, sep = "")
        if (!(test %in% ls(symbols)))
            return(test)
    }
}

# constants and functions for building up Avro type schemae
avro.null <- "null"
avro.boolean <- "boolean"
avro.int <- "int"
avro.long <- "long"
avro.float <- "float"
avro.double <- "double"
avro.bytes <- "bytes"
avro.fixed <- function(size, name = NULL, namespace = NULL) {
    if (is.null(name))
        name <- uniqueFixedName()
    if (is.null(namespace))
        list(type = "fixed", size = size, name = name)
    else
        list(type = "fixed", size = size, name = name, namespace = namespace)
}
avro.string <- "string"
avro.enum <- function(symbols, name = NULL, namespace = NULL) {
    if (is.null(name))
        name <- uniqueEnumName()
    if (is.null(namespace))
        list(type = "enum", symbols = symbols, name = name)
    else
        list(type = "enum", symbols = symbols, name = name, namespace = namespace)
}
avro.array <- function(items) {
    list(type = "array", items = items)
}
avro.map <- function(values) {
    list(type = "map", values = values)
}
avro.record <- function(fields, name = NULL, namespace = NULL) {
    outputFields <- list()
    for (x in names(fields))
        outputFields[[length(outputFields) + 1]] = list(name = x, type = fields[[x]])
    if (is.null(name))
        name <- uniqueRecordName()
    if (is.null(namespace))
        list(type = "record", fields = outputFields, name = name)
    else
        list(type = "record", fields = outputFields, name = name, namespace = namespace)
}
avro.union <- function(...) list(...)

# function to get fully qualified names and distinguish named types from unnamed types
avro.fullName <- function(type) {
    if (!is.atomic(type)  &&  !is.null(type$name)) {
        if (is.null(type$namespace))
            type$name
        else
            paste(type$namespace, type$name, sep=".")
    }
    else
        NULL
}

# function to handle the define-type-once rule
avro.typemap <- function(...) {
    types <- list(...)

    # closure over this counter keeps track of how many times we've seen a name
    counter <- new.env()
    for (name in names(types))
        if (!is.null(avro.fullName(types[[name]])))
            counter[[name]] = 0

    function(name) {
        if (name %in% ls(counter)) {
            out <- if (counter[[name]] == 0) types[[name]] else avro.fullName(types[[name]])
            counter[[name]] = counter[[name]] + 1
            out
        }
        else
            types[[name]]
    }
}

# function to get PFA type names from R type names (FIXME: incomplete)
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

# function to get a PFA record structure from a data frame
avro.fromFrame <- function(dataFrame, exclude = list(), name = NULL, namespace = NULL) {
    fields <- list()
    for (x in names(dataFrame))
        if (!(x %in% exclude))
            fields[[x]] <- avro.type(dataFrame[[x]])
    avro.record(fields, name, namespace)
}

# functions to build cells and pools (not really necessary, just helps make the code self-documenting)
pfa.cell <- function(type, init, source = "embedded", shared = FALSE, rollback = FALSE)
    list(type = type, init = init, source = source, shared = shared, rollback = rollback)
pfa.pool <- function(type, init, source = "embedded", shared = FALSE, rollback = FALSE)
    list(type = type, init = init, source = source, shared = shared, rollback = rollback)

# function to build a PFA configuration
pfa.config <- function(input,
                       output,
                       action,
                       name = NULL,
                       method = NULL,
                       begin = NULL,
                       end = NULL,
                       fcns = NULL,
                       zero = NULL,
                       merge = NULL,
                       cells = NULL,
                       pools = NULL,
                       randseed = NULL,
                       doc = NULL,
                       version = NULL,
                       metadata = NULL,
                       options = NULL,
                       env = parent.frame()) {
    beginSymbols <- as.environment(list(name = "name", instance = "instance", metadata = "metadata"))
    actionSymbols <- as.environment(list(input = "input", name = "name", instance = "instance", metadata = "metadata", actionsStarted = "actionsStarted", actionsFinished = "actionsFinished"))
    endSymbols <- as.environment(list(name = "name", instance = "instance", metadata = "metadata", actionsStarted = "actionsStarted", actionsFinished = "actionsFinished"))
    mergeSymbols <- as.environment(list(tallyOne = "tallyOne", tallyTwo = "tallyTwo", name = "name", instance = "instance", metadata = "metadata"))

    if (!is.null(version)) {
        beginSymbols$version = "version"
        actionSymbols$version = "version"
        endSymbols$version = "version"
        mergeSymbols$version = "version"
    }

    if (!is.null(method)  &&  method == "fold") {
        actionSymbols$tally = "tally"
        endSymbols$tally = "tally"
    }

    cellsEnv <- new.env()
    if (length(cells) > 0)
        for (x in names(cells))
            cellsEnv[[x]] = x
    poolsEnv <- new.env()
    if (length(pools) > 0)
        for (x in names(pools))
            poolsEnv[[x]] = x
    fcnsEnv <- new.env()
    if (length(fcns) > 0)
        for (x in names(fcns))
            fcnsEnv[[x]] = x
    if (!is.null(method)  &&  method == "emit")
        fcnsEnv$emit = "emit"
    
    if (is.expression(action))
        action <- lapply(action, function(expr) pfa.expr(expr, symbols = actionSymbols, cells = cellsEnv, pools = poolsEnv, fcns = fcnsEnv, env = env))

    if (is.expression(begin))
        begin <- lapply(begin, function(expr) pfa.expr(expr, symbols = beginSymbols, cells = cellsEnv, pools = poolsEnv, fcns = fcnsEnv, env = env))

    if (is.expression(end))
        end <- lapply(end, function(expr) pfa.expr(expr, symbols = endSymbols, cells = cellsEnv, pools = poolsEnv, fcns = fcnsEnv, env = env))

    if (is.expression(merge))
        merge <- lapply(merge, function(expr) pfa.expr(expr, symbols = mergeSymbols, cells = cellsEnv, pools = poolsEnv, fcns = fcnsEnv, env = env))

    out <- list(input = input, output = output, action = action)
    if (!is.null(name))
        out$name = name
    if (!is.null(method)) {
        if (method != "map"  &&  method != "emit"  &&  method != "fold")
            stop("method must be one of \"map\", \"emit\", \"fold\"")
        out$method = method
    }
    if (!is.null(begin))
        out$begin = begin
    if (!is.null(end))
        out$end = end
    if (!is.null(fcns)) {
        convert <- function(expr) {
            if (is.expression(expr)  &&  length(expr) == 1) {
                name <- as.character(expr[[1]][[1]])
                args <- tail(as.list(expr[[1]]), -1)
                convertFcn(name, args, new.env(), cellsEnv, poolsEnv, fcnsEnv, env = env)
            }
            else
                expr
        }
        out$fcns = lapply(fcns, convert)
    }
    if (!is.null(zero))
        out$zero = zero
    if (!is.null(merge))
        out$merge = merge
    if (!is.null(cells))
        out$cells = cells
    if (!is.null(pools))
        out$pools = pools
    if (!is.null(randseed))
        out$randseed = randseed
    if (!is.null(doc))
        out$doc = doc
    if (!is.null(version))
        out$version = version
    if (!is.null(metadata))
        out$metadata = metadata
    if (!is.null(options))
        out$options = options
    out
}

# function to convert an R expression into a PFA expression
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
        "null"
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
                        if (length(indexes) == 1)
                            out[["foreach"]] <- index
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

# lib1/math
rToPFA[["abs"]] <- "m.abs"
rToPFA[["acos"]] <- "m.acos"
rToPFA[["asin"]] <- "m.asin"
rToPFA[["atan"]] <- "m.atan"
rToPFA[["atan2"]] <- "m.atan2"
rToPFA[["ceiling"]] <- "m.ceil"
rToPFA[["cos"]] <- "m.cos"
rToPFA[["cosh"]] <- "m.cosh"
rToPFA[["exp"]] <- "m.exp"
rToPFA[["expm1"]] <- "m.expm1"
rToPFA[["floor"]] <- "m.floor"
rToPFA[["log"]] <- function(args) {
    if (length(args) == 1)
        list(m.ln = args)
    else
        list(m.log = args)
}
rToPFA[["log10"]] <- "m.log10"
rToPFA[["round"]] <- "m.round"
rToPFA[["sin"]] <- "m.sin"
rToPFA[["sinh"]] <- "m.sinh"
rToPFA[["sqrt"]] <- "m.sqrt"
rToPFA[["tan"]] <- "m.tan"
rToPFA[["tanh"]] <- "m.tanh"

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

