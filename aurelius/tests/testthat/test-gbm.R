context("gbm")

test_that("check gaussian family GBMs", {

  gauss_input <- list(X1=1, X2='B')

  guass_dat <- data.frame(X1 = runif(100),
                          X2 = factor(c(rep(c('A', 'B'), 30), rep('C', 40)),
                                      ordered=FALSE,
                                      levels=c('B','C','A')),
                          Y = ifelse(as.character(X2)=='B', 10, 20) * X1 - 
                            ifelse(as.character(X2)=='B', 15, 0) + 
                            rnorm(10,0,.5))
  
  gauss_model <- gbm(Y ~ X1 + X2, data=guass_dat, n.trees = 2, interaction.depth=3, distribution = 'gaussian')
  gauss_model_as_pfa <- pfa.gbm(gauss_model)
  gauss_engine <- pfa_engine(gauss_model_as_pfa)
  
  expect_equal(gauss_engine$action(gauss_input), 
               unname(predict(gauss_model, n.trees=gauss_model$n.trees, 
                              newdata = as.data.frame(gauss_input), type='response')),
               tolerance = .0001)
  
  laplace_model <- gbm(Y ~ X1 + X2, data=guass_dat, n.trees = 2, interaction.depth=3, distribution = 'laplace')
  laplace_model_as_pfa <- pfa.gbm(laplace_model)
  laplace_engine <- pfa_engine(laplace_model_as_pfa)
  
  expect_equal(laplace_engine$action(gauss_input), 
               unname(predict(laplace_model, n.trees=laplace_model$n.trees, 
                              newdata = as.data.frame(gauss_input), type='response')),
               tolerance = .0001)
  
  tdist_model <- gbm(Y ~ X1 + X2, data=guass_dat, n.trees = 2, interaction.depth=3, distribution = 'tdist')
  tdist_model_as_pfa <- pfa.gbm(tdist_model)
  tdist_engine <- pfa_engine(tdist_model_as_pfa)
  
  expect_equal(tdist_engine$action(gauss_input), 
               unname(predict(tdist_model, n.trees=tdist_model$n.trees, 
                              newdata = as.data.frame(gauss_input), type='response')),
               tolerance = .0001)
})


test_that("check binomial family GBMs", {
  
  binomial_input <- list(X1=0.5, X2=0)
  
  binomial_dat <- data.frame(X1 = runif(100), 
                             X2 = rnorm(100), 
                             Y = (rexp(100,5) + 5 * X1 - 4 * X2) > 0)

  bernoulli_model <- gbm(Y ~ X1 + X2, data=binomial_dat, n.trees = 2, interaction.depth=3, distribution = 'bernoulli')
  object <- bernoulli_model
  bernoulli_model_as_pfa <- pfa.gbm(bernoulli_model)
  bernoulli_engine <- pfa_engine(bernoulli_model_as_pfa)
  
  expect_equal(bernoulli_engine$action(binomial_input), 
               unname(predict(bernoulli_model, n.trees=bernoulli_model$n.trees, 
                              newdata = as.data.frame(binomial_input), type='response')),
               tolerance = .0001)
  
  huberized_model <- gbm(Y ~ X1 + X2, data=binomial_dat, n.trees = 2, interaction.depth=3, distribution = 'huberized')
  huberized_model_as_pfa <- pfa.gbm(huberized_model)
  huberized_engine <- pfa_engine(huberized_model_as_pfa)
  
  expect_equal(huberized_engine$action(binomial_input), 
               unname(predict(huberized_model, n.trees=huberized_model$n.trees, 
                              newdata = as.data.frame(binomial_input), type='response')),
               tolerance = .0001)
  
  adaboost_model <- gbm(Y ~ X1 + X2, data=binomial_dat, n.trees = 2, interaction.depth=3, distribution = 'adaboost')
  adaboost_model_as_pfa <- pfa.gbm(adaboost_model)
  adaboost_engine <- pfa_engine(adaboost_model_as_pfa)
  
  expect_equal(adaboost_engine$action(binomial_input), 
               unname(predict(adaboost_model, n.trees=adaboost_model$n.trees, 
                              newdata = as.data.frame(binomial_input), type='response')),
               tolerance = .0001)
})


test_that("check poisson family GBMs", {

  poisson_input <- list(X1=3, X2=3)

  poisson_dat <- data.frame(X1 = runif(100), 
                            X2 = runif(100), 
                            Y = round(3 + 5 * X1 + 3 * X2 + rnorm(100, 0, 1)))
  
  poisson_model <- gbm(Y ~ X1 + X2, data=poisson_dat, distribution='poisson') 
  poisson_model_as_pfa <- pfa.gbm(poisson_model)
  poisson_engine <- pfa_engine(poisson_model_as_pfa)
  
  expect_equal(poisson_engine$action(poisson_input), 
               unname(predict(poisson_model, n.trees=poisson_model$n.trees, 
                              newdata = as.data.frame(poisson_input), type='response')),
               tolerance = .0001)
})


test_that("check survival/cox family GBMs", {

  cox_input <- list(x=0, sex=0)
  
  test1 <- list(time=rep(c(4,3,1,1,2,2,3), 50), 
                status=rep(c(1,1,1,0,1,1,0), 50), 
                x=rep(c(0,2,1,1,1,0,0), 50), 
                sex=rep(c(0,0,0,0,1,1,1), 50))
  
  cox_model <- gbm(Surv(time, status) ~ x + sex, data=test1, distribution='coxph') 
  cox_model_as_pfa <- pfa.gbm(cox_model)
  cox_engine <- pfa_engine(cox_model_as_pfa)
  
  expect_equal(cox_engine$action(cox_input), 
               unname(predict(cox_model, n.trees=cox_model$n.trees, 
                              newdata = as.data.frame(cox_input), type='response')),
               tolerance = .0001)
})

# multinomial
# quantile
# pairwise
