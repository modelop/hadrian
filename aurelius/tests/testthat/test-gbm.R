context("pfa.gbm")

suppressMessages(suppressWarnings(library(gbm)))

gbm_resp_to_prob <- function(model, n.trees, newdata){
  pred_prob <- unname(predict(model, n.trees=n.trees, newdata=newdata, type='response'))
  gbm_res <- c(`1`=pred_prob, `0`=1-pred_prob)
  return(gbm_res) 
}

gbm_resp_to_resp <- function(model, n.trees, newdata, cutoff){
  pred_prob <- unname(predict(model, n.trees=n.trees, newdata=newdata, type='response'))
  if(pred_prob >= cutoff) "1" else "0"
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
  
  bernoulli_model_as_pfa <- pfa(bernoulli_model, pred_type='response')
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

test_that("check multinomial gbms", {
  
  multinomial_input <- list(Petal_Length = 1.25,
                            Sepal_Length = 5)
  iris2 <- iris
  names(iris2) <- gsub('\\.', '_', names(iris))

  multinomial_model <- gbm(Species ~ Petal_Length + Sepal_Length,
                         data = iris2,
                         distribution='multinomial',
                         n.trees=2)

  multinomial_model_as_pfa <- pfa(multinomial_model, pred_type='response')
  multinomial_engine <- pfa_engine(multinomial_model_as_pfa)

  expect_equal(multinomial_engine$action(multinomial_input),
               gbm_mult_resp_to_resp(multinomial_model,
                                     multinomial_model$n.trees,
                                     as.data.frame(multinomial_input)))
  
  # check that "prob" pred type behaves as predict.gbm
  multinomial_model_as_pfa <- pfa(multinomial_model, pred_type='prob')
  multinomial_engine <- pfa_engine(multinomial_model_as_pfa)

  expect_equal(multinomial_engine$action(multinomial_input),
               gbm_mult_resp_to_prob(model = multinomial_model,
                                     n.trees = multinomial_model$n.trees,
                                     newdata = as.data.frame(multinomial_input)),
               tolerance = .0001)
  
  # test non-uniform cutoffs
  test_cutoffs <- c(setosa=.4, versicolor=.4, virginica=.2)
  
  prob_pred <- predict(multinomial_model, 
                       n.trees=multinomial_model$n.trees, 
                       newdata=as.data.frame(multinomial_input), 
                       type='response')[,,1]
  cutoff_ratio_adjusted_preds <- prob_pred / test_cutoffs

  multinomial_model_as_pfa <- pfa(multinomial_model, pred_type='response', cutoff = test_cutoffs)
  multinomial_engine <- pfa_engine(multinomial_model_as_pfa)  

  expect_equal(multinomial_engine$action(multinomial_input), 
               names(cutoff_ratio_adjusted_preds)[which.max(cutoff_ratio_adjusted_preds)])
  
})

# waiting for support of other distributions
# quantile
# pairwise

# pairwise data generator
# taken from https://github.com/harrysouthworth/gbm/blob/master/demo/pairwise.R
gen_pairwise_data <- function(N) {

  # create query groups, with an average size of 25 items each
  num.queries <- floor(N/25)
  query <- sample(1:num.queries, N, replace=TRUE)

  # X1 is a variable determined by query group only
  query.level <- runif(num.queries)
  X1 <- query.level[query]

  # X2 varies with each item
  X2 <- runif(N)

  # X3 is uncorrelated with target
  X3 <- runif(N)

  # The target
  Y <- X1 + X2

  # Add some random noise to X2 that is correlated with
  # queries, but uncorrelated with items

  X2 <- X2 + scale(runif(num.queries))[query]

  # Add some random noise to target
  SNR <- 5 # signal-to-noise ratio
  sigma <- sqrt(var(Y)/SNR)
  Y <- Y + runif(N, 0, sigma)

  data.frame(Y, query=query, X1, X2, X3)
}

test_that("check unsupported gbms", {
  
  quantile_dat <- data.frame(X1 = runif(100), 
                             Y = rnorm(100, 0, .05))
  quantile_model <- gbm(Y ~ X1, 
                     data = quantile_dat,
                     distribution = list(name='quantile', alpha=.75),
                     n.trees = 2, 
                     interaction.depth = 3)
  
  expect_error(pfa(quantile_model), 
               'Currently not supporting gbm models of distribution quantile')
    
  pairwise_dat <- gen_pairwise_data(100)
  pairwise_model <- gbm(Y ~ X1+X2+X3, 
                     data = pairwise_dat,
                     distribution = list(name='pairwise', metric="ndcg", group='query'),
                     n.trees = 2, 
                     interaction.depth = 3)
  
  expect_error(pfa(pairwise_model), 
               'Currently not supporting gbm models of distribution pairwise')
})