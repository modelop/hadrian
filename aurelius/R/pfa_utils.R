
#' @include pfa_expr.R
cutoff_cmp_fcn <- list("cutoff_cmp" = c(list(params = list(list('x' = avro_double), 
                                                           list('y' = avro_double)),
                                             ret = avro_int),
                                        pfa_expr(expr=parse(text=paste('if(x >= y) {1} else {0}')),
                                                 symbols = list('x', 'y'))))

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

