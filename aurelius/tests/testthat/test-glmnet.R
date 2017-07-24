context("pfa.glmnet")

suppressMessages(suppressWarnings(library(glmnet)))

glmnet_resp_to_prob <- function(model, newdata, lambda){
  pred_prob <- predict(model, newx=newdata, s=lambda, type='response')
  glmnet_res <- c(1-pred_prob, pred_prob)
  names(glmnet_res) <- model$classnames
  return(glmnet_res) 
}

glmnet_resp_to_resp <- function(model, newdata, lambda, cutoff){
  pred_prob <- predict(model, newx=newdata, s=lambda, type='response')
  if(pred_prob >= cutoff) model$classnames[2] else model$classnames[1]
}

glmnet_mult_resp_to_prob  <- function(model, newdata, lambda){
  pred_prob <- predict(model, newx=newdata, s=lambda, type='response')[,,1]
  return(pred_prob)
}

glmnet_mult_resp_to_resp  <- function(model, newdata, lambda){
  pred_prob <- predict(model, newx=newdata, s=lambda, type='response')[,,1]
  names(pred_prob)[which.max(pred_prob)]
}

test_that("check gaussian glmnets", {

  elnet_input <- list(X1=.15, X2=.99)
  lambda <- .01
  
  x <- matrix(rnorm(100*2), 100, 2, dimnames = list(NULL, c('X1','X2')))
  y <- rnorm(100)
  
  object <- glmnet(x,y)
  elnet_model_as_pfa <- pfa(object = object, lambda = lambda)
  elnet_engine <- pfa_engine(elnet_model_as_pfa)
  
  expect_equal(elnet_engine$action(elnet_input), 
               as.numeric(predict(object, 
                                  newx=as.matrix(t(unlist(elnet_input))), 
                                  s=lambda,
                                  type='response')),
               tolerance = .0001)
  
  # no intercept
  no_int_object <- glmnet(x,y,intercept=FALSE)
  no_int_elnet_model_as_pfa <- pfa(object = no_int_object, lambda = lambda)
  no_int_elnet_engine <- pfa_engine(no_int_elnet_model_as_pfa)
  
  expect_equal(no_int_elnet_engine$action(elnet_input), 
               as.numeric(predict(no_int_object, 
                                  newx=as.matrix(t(unlist(elnet_input))), 
                                  s=lambda,
                                  type='response')),
               tolerance = .0001)
  
  # cross validated object
  cv_object <- cv.glmnet(x,y)
  cv_elnet_model_as_pfa <- pfa(object = cv_object, lambda = lambda)
  cv_elnet_engine <- pfa_engine(cv_elnet_model_as_pfa)
  
  expect_equal(cv_elnet_engine$action(elnet_input), 
               as.numeric(predict(cv_object, 
                                  newx=as.matrix(t(unlist(elnet_input))), 
                                  s=lambda,
                                  type='response')),
               tolerance = .0001)

})


test_that("check binomial glmnet", {

  lognet_input <- list(X1=.01, X2=.3)
  lambda <- .001
  
  # add test case where the coefficients and intercept are all zero
  set.seed(1)
  x <- matrix(rnorm(100*2), 100, 2, dimnames = list(NULL, c('X1','X2')))
  y <- factor(sample(c('Y', 'Z'), 100, replace = TRUE))
  
  this_lambda <- 1000
  lognet_model <- glmnet(x, y, family="binomial", intercept = F)
  lognet_model_as_pfa <- pfa(object = lognet_model, lambda = this_lambda, 
                             pred_type='prob')
  lognet_engine <- pfa_engine(lognet_model_as_pfa)
  
  expect_equal(lognet_engine$action(lognet_input)[lognet_model$classnames], 
               glmnet_resp_to_prob(model = lognet_model,
                                   newdata = as.matrix(t(unlist(lognet_input))),
                                   lambda = this_lambda),
               tolerance = .0001)
  
  # test case where coefficients are non-zero
  set.seed(1)
  x <- matrix(rnorm(100*2), 100, 2, dimnames = list(NULL, c('X1','X2')))
  y <- 3 - 4 * x[,'X1'] + 3 * x[,'X2'] + rnorm(100, 0, 4)
  y <- factor(ifelse(y > 0, 'Y', 'Z'))
  
  lognet_model <- glmnet(x, y, family="binomial")
  lognet_model_as_pfa <- pfa(object = lognet_model, lambda = lambda, 
                             pred_type='prob')
  lognet_engine <- pfa_engine(lognet_model_as_pfa)
  
  expect_equal(lognet_engine$action(lognet_input)[lognet_model$classnames], 
               glmnet_resp_to_prob(model = lognet_model,
                                   newdata = as.matrix(t(unlist(lognet_input))),
                                   lambda = lambda),
               tolerance = .0001)
  
  lognet_model_as_pfa <- pfa(object = lognet_model, lambda = lambda, 
                             pred_type = 'response', 
                             cutoffs = c(Y = .5, Z = .5))
  lognet_engine <- pfa_engine(lognet_model_as_pfa)
  
  expect_equal(lognet_engine$action(lognet_input), 
               glmnet_resp_to_resp(model = lognet_model,
                                   newdata=as.matrix(t(unlist(lognet_input))),
                                   lambda=lambda, 
                                   cutoff = 0.5),
               tolerance = .0001)
  
  # test non-uniform cutoffs
  test_cutoffs <- c(Y=.05, Z=.95)
  
  prob_pred <- predict(lognet_model, newx=as.matrix(t(unlist(lognet_input))), s=lambda, type='response')
  prob_pred <- c(1-prob_pred, prob_pred)
  names(prob_pred) <- lognet_model$classnames
  cutoff_ratio_adjusted_preds <- prob_pred / test_cutoffs
  
  lognet_model_as_pfa <- pfa(object = lognet_model, lambda = lambda,
                             pred_type='response', cutoff = test_cutoffs)
  lognet_engine <- pfa_engine(lognet_model_as_pfa)

  expect_equal(lognet_engine$action(lognet_input), 
               names(cutoff_ratio_adjusted_preds)[which.max(cutoff_ratio_adjusted_preds)])  
})


test_that("check poisson glmnet", {

  fishnet_input <- list(X1=.15, X2=.99)
  lambda <- .01
  
  N=500; p=2
  nzc=2
  x=matrix(rnorm(N*p),N,p, dimnames = list(NULL, c('X1','X2')))
  beta=rnorm(nzc)
  f = x[,seq(nzc)]%*%beta
  mu=exp(f)
  y=rpois(N,mu)
  
  fishnet_model <- glmnet(x,y,family="poisson")
  fishnet_model_as_pfa <- pfa(object = fishnet_model, lambda = lambda)
  fishnet_engine <- pfa_engine(fishnet_model_as_pfa)
  
  expect_equal(fishnet_engine$action(fishnet_input), 
               as.numeric(predict(fishnet_model, 
                                  newx=as.matrix(t(unlist(fishnet_input))), 
                                  s=lambda,
                                  type='response')),
               tolerance = .0001)
  
})


test_that("check cox glmnet", {

  coxnet_input <- list(X1=.15, X2=.99, X3=.5, X4=.5)
  lambda <- .01
  
  set.seed(10101)
  N=1000;p=4
  nzc=p/2
  x=matrix(rnorm(N*p),N,p, dimnames = list(NULL, c('X1','X2','X3','X4')))
  beta=rnorm(nzc)
  fx=x[,seq(nzc)]%*%beta/2
  hx=exp(fx)
  ty=rexp(N,hx)
  tcens=rbinom(n=N,prob=.3,size=1)# censoring indicator
  y=cbind(time=ty,status=1-tcens) # y=Surv(ty,1-tcens) with library(survival)
  
  coxnet_model <- glmnet(x,y,family="cox")
  coxnet_model_as_pfa <- pfa(object = coxnet_model, lambda = lambda)
  coxnet_engine <- pfa_engine(coxnet_model_as_pfa)
  
  expect_equal(coxnet_engine$action(coxnet_input), 
               as.numeric(predict(coxnet_model, 
                                  newx=as.matrix(t(unlist(coxnet_input))), 
                                  s=lambda,
                                  type='response')),
               tolerance = .0001)
  
})


test_that("check multinomial glmnet", {
  
  multinomial_input <- list(X1=.15, X2=.99, X3=.5)
  lambda <- .01 
  
  x <- matrix(rnorm(100*3), 100, 3, dimnames = list(NULL, c('X1','X2', 'X3')))
  g4 <- sample(LETTERS[1:4], 100, replace=TRUE)
  multinomial_model <- glmnet(x, g4, family="multinomial", intercept = FALSE)

  multinomial_model_as_pfa <- pfa(object = multinomial_model, lambda = lambda, 
                                  pred_type='prob')
  multinomial_engine <- pfa_engine(multinomial_model_as_pfa)
  multinomial_res <- multinomial_engine$action(multinomial_input)
  
  expect_equal(multinomial_res[multinomial_model$classnames], 
               glmnet_mult_resp_to_prob(model = multinomial_model, 
                                        newdata = as.matrix(t(unlist(multinomial_input))), 
                                        lambda  = lambda),
               tolerance = .0001)
  
  # check that "response" pred type behaves as expected
  multinomial_model_as_pfa <- pfa(object = multinomial_model, lambda = lambda, 
                                  pred_type='response')
  multinomial_engine <- pfa_engine(multinomial_model_as_pfa)
  
  expect_equal(multinomial_engine$action(multinomial_input), 
               glmnet_mult_resp_to_resp(model = multinomial_model, 
                                        newdata = as.matrix(t(unlist(multinomial_input))), 
                                        lambda  = lambda))
  
  # check that it works for grouped multinomial models
  multinomial_grouped_model <- glmnet(x, g4, family="multinomial", 
                                      type.multinomial="grouped")
  
  multinomial_grouped_model_as_pfa <- pfa(object = multinomial_grouped_model, lambda = lambda,
                                          pred_type='prob')
  multinomial_grouped_engine <- pfa_engine(multinomial_grouped_model_as_pfa)
  multinomial_grouped_res <- multinomial_grouped_engine$action(multinomial_input)
  
  expect_equal(multinomial_grouped_res[multinomial_grouped_model$classnames], 
               glmnet_mult_resp_to_prob(model = multinomial_grouped_model, 
                                        newdata = as.matrix(t(unlist(multinomial_input))), 
                                        lambda  = lambda))
})


test_that("check multivariate gaussian glmnet", {
  x <- matrix(rnorm(100*20),100,20)
  y <- matrix(rnorm(100*3),100,3)
  mgaussian_model <- glmnet(x, y, family="mgaussian")
  expect_error(pfa(mgaussian_model), 
             'Currently not supporting glmnet models of net type: mrelnet')
})
