context("pfa.randomForest")

suppressMessages(suppressWarnings(library(randomForest)))

test_that("check regression randomForests", {

  continuous_input <- list(X1=5)
  
  continuous_dat <- data.frame(X1 = iris$Sepal.Length, 
                               Y = iris$Petal.Length)
  
  regression_rf <- randomForest(Y ~ X1, 
                                data=continuous_dat,
                                ntree = 3)
  
  regression_rf_as_pfa <- pfa(regression_rf)
  regression_rf_engine <- pfa_engine(regression_rf_as_pfa)

  expect_equal(regression_rf_engine$action(continuous_input), 
               unname(predict(regression_rf, newdata=data.frame(continuous_input))),
               tolerance = .0001)
  
  # check that the pred_type "prob" is ignored
  regression_rf_as_pfa <- pfa(regression_rf, pred_type='prob')
  regression_rf_engine <- pfa_engine(regression_rf_as_pfa)

  expect_equal(regression_rf_engine$action(continuous_input), 
               unname(predict(regression_rf, newdata=data.frame(continuous_input))),
               tolerance = .0001)
  
})


test_that("check classification randomForests", {
  
  binomial_input <- list(X1=6)
  
  binomial_dat <- data.frame(X1 = iris$Sepal.Length, 
                             Y = factor(as.character(iris$Species) == 'virginica'))
  
  classification_rf <- randomForest(Y ~ X1, 
                                    data = binomial_dat,
                                    ntree = 13)
    
  classification_rf_as_pfa <- pfa(classification_rf)
  classification_rf_engine <- pfa_engine(classification_rf_as_pfa)

  expect_equal(classification_rf_engine$action(binomial_input), 
               unname(as.character(predict(classification_rf, newdata=data.frame(binomial_input)))))
  
  # test non-uniform cutoffs
  test_cutoffs <- c(`FALSE`=.95, `TRUE`=.05)
  classification_rf_as_pfa <- pfa(classification_rf, cutoffs = test_cutoffs)
  classification_rf_engine <- pfa_engine(classification_rf_as_pfa)
  
  prob_pred <- unclass(predict(classification_rf, newdata=data.frame(binomial_input), type='prob'))[1,]
  cutoff_ratio_adjusted_preds <- prob_pred / test_cutoffs

  expect_equal(classification_rf_engine$action(binomial_input), 
               names(cutoff_ratio_adjusted_preds)[which.max(cutoff_ratio_adjusted_preds)])  
  
  classification_rf_as_pfa <- pfa(classification_rf, pred_type='prob')
  classification_rf_engine <- pfa_engine(classification_rf_as_pfa)

  # check that "prob" pred type behaves as predict.randomForest
  expect_equal(classification_rf_engine$action(binomial_input), 
               unclass(predict(classification_rf, newdata=data.frame(binomial_input), type='prob'))[1,])
               
})


test_that("check multiclass classification randomForest", {
  
  multiclass_input <- list(X1=5)
  
  multiclass_df <- data.frame(X1 = iris$Sepal.Length, 
                              Y = iris$Species)
  
  multiclass_rf <- randomForest(Y ~ X1,
                                data = multiclass_df,
                                ntree = 3)
  
  multiclass_rf_as_pfa <- pfa(multiclass_rf)
  multiclass_rf_engine <- pfa_engine(multiclass_rf_as_pfa)
  
  expect_equal(multiclass_rf_engine$action(multiclass_input), 
               unname(as.character(predict(multiclass_rf, newdata=data.frame(multiclass_input)))))
  
  multiclass_rf_as_pfa <- pfa(multiclass_rf, pred_type='prob')
  multiclass_rf_engine <- pfa_engine(multiclass_rf_as_pfa)

  # check that "prob" pred type behaves as predict.randomForest
  expect_equal(multiclass_rf_engine$action(multiclass_input), 
               unclass(predict(multiclass_rf, newdata=data.frame(multiclass_input), type='prob'))[1,])
               
})


test_that("check unsupported unsupervised randomForests", {

  continuous_dat <- data.frame(X1 = iris$Sepal.Length, 
                               Y = iris$Petal.Length)
  
  unsupervised_rf <- randomForest( ~ X1, 
                                  data = continuous_dat,
                                  ntree = 3)
  
  expect_error(pfa(unsupervised_rf), 
               'Currently not supporting randomForest models of type unsupervised')
  
})
