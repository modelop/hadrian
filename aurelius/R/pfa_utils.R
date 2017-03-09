
#' @include avro.R
gen_blank_array_of_arrays <- function(avro_type){
  list(type = avro_array(avro_array(avro_double)),
          new = list())
}

#' @include avro.R
gen_blank_array <- function(avro_type){
  list(type = avro_array(avro_type),
          new = list())
}

#' @include pfa_expr.R
normalize_array_fcn <- list("a.normalize" = c(list(params = list(list('x' = avro_array(avro_double))),
                                                   ret = avro_array(avro_double)),
                                              pfa_expr(expr=parse(text=paste('la.scale(x, 1/a.sum(x))')),
                                                       symbols = list('x'))))
#' @include pfa_expr.R
ln_la_fcn <- list("la.ln" = c(list(params = list(list('x' = avro_array(avro_array(avro_double)))),
                                     ret = avro_array(avro_array(avro_double))),
                                pfa_expr(expr=parse(text=paste('la.map(x, function(y = avro_double -> avro_double) m.ln(y))')),
                                         symbols = list('x'))))

#' @include pfa_expr.R
exp_la_fcn <- list("la.exp" = c(list(params = list(list('x' =  avro_array(avro_array(avro_double)))),
                                       ret = avro_array(avro_array(avro_double))),
                                  pfa_expr(expr=parse(text=paste('la.map(x, function(y = avro_double -> avro_double) m.exp(y))')),
                                           symbols = list('x'))))

#' @include pfa_expr.R
divide_fcn <- list("divide" = c(list(params = list(list('x' = avro_double),
                                                   list('y' = avro_double)),
                                     ret = avro_double),
                                pfa_expr(expr=parse(text=paste('x / y')),
                                         symbols = list('x', 'y'))))

#' @include pfa_expr.R
cutoff_ratio_cmp_fcn <- list("cutoff_ratio_cmp" = c(list(params = list(list('input' = avro_map(avro_double)), 
                                                                       list('cutoffs' = avro_map(avro_double))),
                                                         ret = avro_map(avro_double)),
                                                    pfa_expr(expr=parse(text=paste('map.zipmap(input, cutoffs, u.divide)')),
                                                             symbols = list('input', 'cutoffs'), 
                                                             fcns = list('u.divide'))))

validate_cutoffs <- function(classes, cutoffs=NULL){
  
  if(is.null(cutoffs)){
    cutoffs <- round(rep(1 / length(classes), length(classes)), 8)
    names(cutoffs) <- classes
    cutoffs <- as.list(cutoffs)
  } else {
    if (sum(cutoffs) > 1 || sum(cutoffs) < 0 || !all(cutoffs > 0) || length(cutoffs) != length(classes)) {
        stop("Incorrect cutoffs specified.")
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

