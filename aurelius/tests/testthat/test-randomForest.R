context("randomForest")

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
  
})


test_that("check classification randomForests", {
  
  binomial_input <- list(X1=6)
  
  binomial_dat <- data.frame(X1 = iris$Sepal.Length, 
                             Y = factor(as.character(iris$Species) == 'virginica'))
  
  classification_rf <- randomForest(Y ~ X1, 
                                    data = binomial_dat,
                                    ntree = 3)
    
  classification_rf_as_pfa <- pfa(classification_rf)
  classification_rf_engine <- pfa_engine(classification_rf_as_pfa)

  expect_equal(classification_rf_engine$action(binomial_input), 
               unname(as.character(predict(classification_rf, newdata=data.frame(binomial_input)))))
  
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
