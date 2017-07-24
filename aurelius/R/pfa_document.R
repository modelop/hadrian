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


#' pfa_document
#'
#' Create a complete PFA document as a list-of-lists. Composing with the JSON 
#' function creates a PFA file on disk.
#' 
#' @importFrom utils tail
#' @source pfa_engine.R
#' @param input input schema, which is an Avro schema as list-of-lists (created by avro_* functions)
#' @param output output schema, which is an Avro schema as list-of-lists (created by avro_* functions)
#' @param action R commands wrapped as an expression (see R's built-in expression function)
#' @param cells named list of cell specifications (see the pfa_cell function)
#' @param pools named list of pool specifications (see the pfa_cell function)
#' @param fcns named list of R commands, wrapped as expressions
#' @param validate a logical indicating whether the generated PFA document should 
#' be validated using Titus-in-Aurelius function \code{\link{pfa_engine}}
#' @param name optional name for the scoring engine (string)
#' @param method "map", "emit", "fold", or NULL (for "map")
#' @param begin R commands wrapped as an expression
#' @param end R commands wrapped as an expression
#' @param zero list-of-lists representing the initial value for a "fold"'s tally
#' @param merge R commands wrapped as an expression
#' @param randseed optional random number seed (integer) for ensuring that the scoring engine is deterministic
#' @param doc optional model documentation string
#' @param version optional model version number (integer)
#' @param metadata optional named list of strings to store model metadata
#' @param options optional list-of-lists to specify PFA options
#' @param env environment for resolving unrecognized symbols as substitutions
#' @return a \code{list} of lists representing a complete PFA document
#' @seealso \code{\link{pfa_engine}}
#' @examples
#' pfa_document(avro_double, avro_double, expression(input + 10))
#' @export
pfa_document <- function(input,
                         output,
                         action,
                         cells = NULL,
                         pools = NULL,
                         fcns = NULL,
                         validate = FALSE,
                         name = NULL,
                         method = NULL,
                         begin = NULL,
                         end = NULL,
                         zero = NULL,
                         merge = NULL,
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
      action <- lapply(action, function(expr) pfa_expr(expr, symbols = actionSymbols, cells = cellsEnv, pools = poolsEnv, fcns = fcnsEnv, env = env))

  if (is.expression(begin))
      begin <- lapply(begin, function(expr) pfa_expr(expr, symbols = beginSymbols, cells = cellsEnv, pools = poolsEnv, fcns = fcnsEnv, env = env))

  if (is.expression(end))
      end <- lapply(end, function(expr) pfa_expr(expr, symbols = endSymbols, cells = cellsEnv, pools = poolsEnv, fcns = fcnsEnv, env = env))

  if (is.expression(merge))
      merge <- lapply(merge, function(expr) pfa_expr(expr, symbols = mergeSymbols, cells = cellsEnv, pools = poolsEnv, fcns = fcnsEnv, env = env))

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
              convert_fcn(name, args, new.env(), cellsEnv, poolsEnv, fcnsEnv, env = env)
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
  
  if(validate){
    invisible(pfa_engine(out))
  }
  
  return(out)
}