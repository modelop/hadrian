context("pfa.glmnet")

test_that("check gaussian glmnets", {

  elnet_input <- list(X1=.15, X2=.99)
  lambda <- .01
  
  x <- matrix(rnorm(100*2), 100, 2, dimnames = list(NULL, c('X1','X2')))
  y <- rnorm(100)
  
  object <- glmnet(x,y)
  elnet_model_as_pfa <- pfa(object, lambda)
  elnet_engine <- pfa_engine(elnet_model_as_pfa)
  
  expect_equal(elnet_engine$action(elnet_input), 
               as.numeric(predict(object, 
                                  newx=as.matrix(t(unlist(elnet_input))), 
                                  s=lambda,
                                  type='response')),
               tolerance = .0001)
  
  # no intercept
  no_int_object <- glmnet(x,y,intercept=FALSE)
  no_int_elnet_model_as_pfa <- pfa(no_int_object, lambda)
  no_int_elnet_engine <- pfa_engine(no_int_elnet_model_as_pfa)
  
  expect_equal(no_int_elnet_engine$action(elnet_input), 
               as.numeric(predict(no_int_object, 
                                  newx=as.matrix(t(unlist(elnet_input))), 
                                  s=lambda,
                                  type='response')),
               tolerance = .0001)
  
  # cross validated object
  cv_object <- cv.glmnet(x,y)
  cv_elnet_model_as_pfa <- pfa(cv_object, lambda)
  cv_elnet_engine <- pfa_engine(cv_elnet_model_as_pfa)
  
  expect_equal(cv_elnet_engine$action(elnet_input), 
               as.numeric(predict(cv_object, 
                                  newx=as.matrix(t(unlist(elnet_input))), 
                                  s=lambda,
                                  type='response')),
               tolerance = .0001)

})


test_that("check binomial glmnet", {

  lognet_input <- list(X1=.15, X2=.99)
  lambda <- .01
  
  x <- matrix(rnorm(100*2), 100, 2, dimnames = list(NULL, c('X1','X2')))
  y <- factor(sample(1:2,100,replace=TRUE))
  
  object <- glmnet(x,y,family="binomial")
  lognet_model_as_pfa <- pfa(object, lambda)
  lognet_engine <- pfa_engine(lognet_model_as_pfa)
  
  expect_equal(lognet_engine$action(lognet_input), 
               as.numeric(predict(object, 
                                  newx=as.matrix(t(unlist(lognet_input))), 
                                  s=lambda,
                                  type='response')),
               tolerance = .0001)
  
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
  
  object <- glmnet(x,y,family="poisson")
  fishnet_model_as_pfa <- pfa(object, lambda)
  fishnet_engine <- pfa_engine(fishnet_model_as_pfa)
  
  expect_equal(fishnet_engine$action(fishnet_input), 
               as.numeric(predict(object, 
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
  
  object <- glmnet(x,y,family="cox")
  coxnet_model_as_pfa <- pfa(object, lambda)
  coxnet_engine <- pfa_engine(coxnet_model_as_pfa)
  
  expect_equal(coxnet_engine$action(coxnet_input), 
               as.numeric(predict(object, 
                                  newx=as.matrix(t(unlist(coxnet_input))), 
                                  s=lambda,
                                  type='response')),
               tolerance = .0001)
  
})


# not yet supported
# x=matrix(rnorm(100*20),100,20)
# 
# #multivariate gaussian
# y=matrix(rnorm(100*3),100,3)
# fit1m=glmnet(x,y,family="mgaussian")
# 
# #multinomial
# g4=sample(1:4,100,replace=TRUE)
# fit3=glmnet(x,g4,family="multinomial")
# fit3a=glmnet(x,g4,family="multinomial",type.multinomial="grouped")