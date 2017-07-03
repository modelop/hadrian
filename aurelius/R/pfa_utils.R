
#' @keywords internal
validate_names <- function(x) {
  x <- gsub('[^a-zA-Z0-9_]+', '_', x)
  x <- gsub('(^[0-9]{1})', '_\\1', x)
  stopifnot(all(grepl('[A-Za-z_][A-Za-z0-9_]*', x)))
  return(x)
}


#' @keywords internal
matrix_to_arr_of_arr <- function(x, row_unpack = TRUE){
  if(row_unpack){
    apply(unname(x), 1, FUN=function(y){as.list(y)})
  } else {
    apply(unname(x), 1, FUN=function(y){list(y)})
  }
}


#' @include avro.R
#' @keywords internal
gen_blank_array_of_arrays <- function(avro_type){
  list(type = avro_array(avro_array(avro_double)),
          new = list())
}


#' @include avro.R
#' @keywords internal
gen_blank_array <- function(avro_type){
  list(type = avro_array(avro_type),
          new = list())
}


#' @include pfa_expr.R
#' @keywords internal
normalize_array_fcn <- list("a.normalize" = c(list(params = list(list('x' = avro_array(avro_double))),
                                                   ret = avro_array(avro_double)),
                                              pfa_expr(expr=parse(text=paste('la.scale(x, 1/a.sum(x))')),
                                                       symbols = list('x'))))


#' @include pfa_expr.R
#' @keywords internal
ln_la_fcn <- list("la.ln" = c(list(params = list(list('x' = avro_array(avro_array(avro_double)))),
                                     ret = avro_array(avro_array(avro_double))),
                                pfa_expr(expr=parse(text=paste('la.map(x, function(y = avro_double -> avro_double) m.ln(y))')),
                                         symbols = list('x'))))


#' @include pfa_expr.R
#' @keywords internal
exp_la_fcn <- list("la.exp" = c(list(params = list(list('x' =  avro_array(avro_array(avro_double)))),
                                       ret = avro_array(avro_array(avro_double))),
                                  pfa_expr(expr=parse(text=paste('la.map(x, function(y = avro_double -> avro_double) m.exp(y))')),
                                           symbols = list('x'))))


#' @include pfa_expr.R
#' @keywords internal
divide_fcn <- list("divide" = c(list(params = list(list('x' = avro_double),
                                                   list('y' = avro_double)),
                                     ret = avro_double),
                                pfa_expr(expr=parse(text=paste('x / y')),
                                         symbols = list('x', 'y'))))


# used as a metric for computing manhatten distance
#' @keywords internal
manhattan_dist_fun  <- list(params = list(list('x' = avro_array(avro_double)),
                                          list('y' = avro_array(avro_double))),
                            ret = avro_double)


#' @include pfa_expr.R
#' @keywords internal
manhattan_dist_fun [['do']] <- pfa_expr(expr=parse(text=paste('metric.taxicab(metric.absDiff,x,y)')),
                                        fcns = list('metric.absDiff'),
                                        symbols = list('x', 'y'))$do


# used as a metric for computing jaccard distance (not similarity since we multiply by -1)
#' @keywords internal
jaccard_dist_fun  <- list(params = list(list('x' = avro_array(avro_boolean)),
                                        list('y' = avro_array(avro_boolean))),
                          ret = avro_double)


#' @include pfa_expr.R
#' @keywords internal
jaccard_dist_fun [['do']] <- pfa_expr(expr=parse(text=paste('-1 * metric.jaccard(x,y)')),
                                      symbols = list('x', 'y'))$do


# used as a metric for computing angle distance (not similarity since we subtract 1)
#' @keywords internal
angle_dist_fun <- list(params = list(list('x' = avro_array(avro_array(avro_double))),
                                     list('y' =  avro_array(avro_array(avro_double)))),
                       ret = avro_double)


#' @include pfa_expr.R
#' @keywords internal
angle_dist_fun[['do']] <- pfa_expr(expr=parse(text=paste('1 - a.sum(a.flatten(la.dot(x,la.transpose(y))))')),
                                   symbols = list('x', 'y'))$do


#' @include pfa_expr.R
#' @keywords internal
cutoff_ratio_cmp_fcn <- list("cutoff_ratio_cmp" = c(list(params = list(list('input' = avro_map(avro_double)), 
                                                                       list('cutoffs' = avro_map(avro_double))),
                                                         ret = avro_map(avro_double)),
                                                    pfa_expr(expr=parse(text=paste('map.zipmap(input, cutoffs, u.divide)')),
                                                             symbols = list('input', 'cutoffs'), 
                                                             fcns = list('u.divide'))))


#' @keywords internal
validate_cutoffs <- function(classes, cutoffs=NULL){
  
  if(is.null(cutoffs)){
    cutoffs <- round(rep(1 / length(classes), length(classes)), 8)
    names(cutoffs) <- classes
    cutoffs <- as.list(cutoffs)
  } else {
    if (sum(cutoffs) > 1 || sum(cutoffs) < 0) {
        stop("Cutoffs must sum to 1.")
    }
    if (!all(cutoffs > 0)) {
      stop("Each cutoff must be greater than zero to avoid divide by zero issues.")
    }
    if (length(cutoffs) != length(classes)) {
      stop("Cutoffs must be specified for every possible target.")
    }
    if (!is.null(names(cutoffs))) {
      if (!all(names(cutoffs) %in% classes)) {
        message(sprintf('You supplied cutoffs with names: %s', 
                        paste0(names(cutoffs), collapse=', ')))
        message(sprintf('The model contains classes with names: %s', 
                paste0(classes, collapse=', ')))
        stop("Wrong name(s) for cutoffs")
      }
      cutoffs <- cutoffs[classes]
    }
    if(!is.list(cutoffs)){
      cutoffs <- as.list(cutoffs)
    }
  }
  
  return(cutoffs)
}