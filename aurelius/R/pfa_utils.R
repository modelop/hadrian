
#' @include pfa_expr.R
cutoff_cmp_fcn <- list("cutoff_cmp" = c(list(params = list(list('x' = avro_double), 
                                                           list('y' = avro_double)),
                                             ret = avro_int),
                                        pfa_expr(expr=parse(text=paste('if(x >= y) {1} else {0}')),
                                                 symbols = list('x', 'y'))))
