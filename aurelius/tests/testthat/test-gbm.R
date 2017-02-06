context("pfa.gbm")

gbm_resp_to_prob <- function(model, n.trees, newdata){
  pred_prob <- unname(predict(model, n.trees=n.trees, newdata=newdata, type='response'))
  gbm_res <- c(`1`=pred_prob, `0`=1-pred_prob)
  return(gbm_res) 
}

gbm_resp_to_resp <- function(model, n.trees, newdata, cutoff){
  pred_prob <- unname(predict(model, n.trees=n.trees, newdata=newdata, type='response'))
  if(pred_prob >= cutoff) 1 else 0
}

gbm_mult_resp_to_prob  <- function(model, n.trees, newdata, cutoff){
  pred_prob <- as.vector(predict(model, n.trees=n.trees, newdata=newdata, type='response'))
  names(pred_prob) <- model$classes
  return(pred_prob)
}

gbm_mult_resp_to_resp  <- function(model, n.trees, newdata, cutoff){
  pred_prob <- as.vector(predict(model, n.trees=n.trees, newdata=newdata, type='response'))
  model$classes[which.max(pred_prob)]
}

test_that("check gaussian family GBMs", {

  gauss_input <- list(X1=1, X2='B')

  guass_dat <- data.frame(X1 = runif(100),
                          X2 = factor(c(rep(c('A', 'B'), 30), rep('C', 40)),
                                      ordered=FALSE,
                                      levels=c('B','C','A')))
  guass_dat$Y <- ifelse(as.character(guass_dat$X2)=='B', 10, 20) * guass_dat$X1 - 
    ifelse(as.character(guass_dat$X2)=='B', 15, 0) +  rnorm(10,0,.5)
  
  gauss_model <- gbm(Y ~ X1 + X2, 
                     data = guass_dat,
                     distribution = 'gaussian',
                     n.trees = 2, 
                     interaction.depth = 3)
  
  gauss_model_as_pfa <- pfa(gauss_model)
  gauss_engine <- pfa_engine(gauss_model_as_pfa)
  
  expect_equal(gauss_engine$action(gauss_input), 
               unname(predict(gauss_model, n.trees=gauss_model$n.trees, 
                              newdata = as.data.frame(gauss_input), type='response')),
               tolerance = .0001)
  
  laplace_model <- gbm(Y ~ X1 + X2, data=guass_dat, n.trees = 2, interaction.depth=3, distribution = 'laplace')
  laplace_model_as_pfa <- pfa(laplace_model)
  laplace_engine <- pfa_engine(laplace_model_as_pfa)
  
  expect_equal(laplace_engine$action(gauss_input), 
               unname(predict(laplace_model, n.trees=laplace_model$n.trees, 
                              newdata = as.data.frame(gauss_input), type='response')),
               tolerance = .0001)
  
  tdist_model <- gbm(Y ~ X1 + X2, data=guass_dat, n.trees = 2, interaction.depth=3, distribution = 'tdist')
  tdist_model_as_pfa <- pfa(tdist_model)
  tdist_engine <- pfa_engine(tdist_model_as_pfa)
  
  expect_equal(tdist_engine$action(gauss_input), 
               unname(predict(tdist_model, n.trees=tdist_model$n.trees, 
                              newdata = as.data.frame(gauss_input), type='response')),
               tolerance = .0001)
})


test_that("check binomial family GBMs", {
  
  binomial_input <- list(X1=0.5, X2=0)
  
  binomial_dat <- data.frame(X1 = runif(100), 
                             X2 = rnorm(100))
  
  binomial_dat$Y <- (rexp(100,5) + 5 * binomial_dat$X1 - 4 * binomial_dat$X2) > 0

  bernoulli_model <- gbm(Y ~ X1 + X2, 
                         data = binomial_dat, 
                         distribution = 'bernoulli',
                         n.trees = 1, 
                         interaction.depth = 3)
  
  bernoulli_model_as_pfa <- pfa(bernoulli_model, pred_type='prob')
  bernoulli_engine <- pfa_engine(bernoulli_model_as_pfa)
  
  expect_equal(bernoulli_engine$action(binomial_input),
               gbm_resp_to_prob(model = bernoulli_model,
                                n.trees = bernoulli_model$n.trees,
                                newdata = as.data.frame(binomial_input)),
               tolerance = .0001)
  
  bernoulli_model_as_pfa <- pfa(bernoulli_model, pred_type='response', cutoff=0.5)
  bernoulli_engine <- pfa_engine(bernoulli_model_as_pfa)
  
  # check that "response" pred type behaves as expected
  expect_equal(bernoulli_engine$action(binomial_input),
               gbm_resp_to_resp(bernoulli_model,
                                bernoulli_model$n.trees,
                                as.data.frame(binomial_input), 
                                cutoff=0.5),
               tolerance = .0001)
  
  huberized_model <- gbm(Y ~ X1 + X2, data=binomial_dat, n.trees = 2, 
                         interaction.depth=3, distribution = 'huberized')
  huberized_model_as_pfa <- pfa(huberized_model, pred_type='prob')
  huberized_engine <- pfa_engine(huberized_model_as_pfa)
  
  expect_equal(huberized_engine$action(binomial_input),
               gbm_resp_to_prob(huberized_model,
                                huberized_model$n.trees,
                                as.data.frame(binomial_input)),
               tolerance = .0001)
  
  adaboost_model <- gbm(Y ~ X1 + X2, 
                        data=binomial_dat, 
                        n.trees = 3, 
                        interaction.depth=3, 
                        distribution = 'adaboost')
  adaboost_model_as_pfa <- pfa(adaboost_model, pred_type='prob')
  adaboost_engine <- pfa_engine(adaboost_model_as_pfa)
  
  expect_equal(adaboost_engine$action(binomial_input),
               gbm_resp_to_prob(adaboost_model,
                                adaboost_model$n.trees,
                                as.data.frame(binomial_input)),
               tolerance = .0001)
})


test_that("check poisson family GBMs", {

  poisson_input <- list(X1=3, X2=3)

  poisson_dat <- data.frame(X1 = runif(100), 
                            X2 = runif(100))
  
  poisson_dat$Y <- round(3 + 5 * poisson_dat$X1 + 3 * poisson_dat$X2 + rnorm(100, 0, 1))
  
  poisson_model <- gbm(Y ~ X1 + X2, data=poisson_dat, distribution='poisson') 
  poisson_model_as_pfa <- pfa(poisson_model)
  poisson_engine <- pfa_engine(poisson_model_as_pfa)
  
  expect_equal(poisson_engine$action(poisson_input), 
               unname(predict(poisson_model, n.trees=poisson_model$n.trees, 
                              newdata = as.data.frame(poisson_input), type='response')),
               tolerance = .0001)
})


test_that("check survival/cox family GBMs", {

  cox_input <- list(x=0, sex=0)
  
  test1 <- data.frame(time=rep(c(4,3,1,1,2,2,3), 50), 
                      status=rep(c(1,1,1,0,1,1,0), 50), 
                      x=rep(c(0,2,1,1,1,0,0), 50), 
                      sex=rep(c(0,0,0,0,1,1,1), 50))
  
  cox_model <- gbm(Surv(time, status) ~ x + sex, data=test1, distribution='coxph') 
  cox_model_as_pfa <- pfa(cox_model)
  cox_engine <- pfa_engine(cox_model_as_pfa)
  
  expect_equal(cox_engine$action(cox_input), 
               unname(predict(cox_model, n.trees=cox_model$n.trees, 
                              newdata = as.data.frame(cox_input), type='response')),
               tolerance = .0001)
})

# waiting for support of other distributions
# multinomial
# quantile
# pairwise

test_that("check unsupported multinomial gbms", {
  
  # multinomial_input <- list(Petal.Length = 1.25, 
  #                           Sepal.Length = 5)
  # 
  # multinomial_model <- gbm(Species ~ Petal.Length + Sepal.Length,
  #                        data = iris,
  #                        distribution='multinomial',
  #                        n.trees=1)
  # 
  # multinomial_model_as_pfa <- pfa(multinomial_model, pred_type='prob')
  # multinomial_engine <- pfa_engine(multinomial_model_as_pfa)
  # 
  # expect_equal(multinomial_engine$action(multinomial_input),
  #              gbm_mult_resp_to_prob(model = multinomial_model,
  #                                    n.trees = multinomial_model$n.trees,
  #                                    newdata = as.data.frame(multinomial_input)),
  #              tolerance = .0001)
  # 
  # multinomial_model_as_pfa <- pfa(multinomial_model, pred_type='response')
  # multinomial_engine <- pfa_engine(multinomial_model_as_pfa)
  # 
  # check that "response" pred type behaves as expected
  expect_equal(multinomial_engine$action(multinomial_input),
               gbm_mult_resp_to_resp(multinomial_model,
                                multinomial_model$n.trees,
                                as.data.frame(multinomial_input), 
                                cutoff=0.5))
  
  expect_error(pfa(multinomial_gbm), 
               'Currently not supporting gbm models of distribution multinomial')
  
})
