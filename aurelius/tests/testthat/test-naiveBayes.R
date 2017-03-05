context("pfa.naiveBayes")

suppressMessages(suppressWarnings(library(e1071)))

test_that("check Naive Bayes model with all numeric inputs", {
  
  numeric_input <- list(X1=5.5, 
                        X2=5)
  
  numeric_dat <- data.frame(X1 = iris$Sepal.Length, 
                            X2 = iris$Petal.Length, 
                            Y = iris$Species)
  
  numeric_model <- naiveBayes(Y ~ X1 + X2, data=numeric_dat) 
  numeric_model_as_pfa <- pfa(numeric_model, pred_type = 'response')
  numeric_model_engine <- pfa_engine(numeric_model_as_pfa)
  
  expect_equal(numeric_model_engine$action(numeric_input), 
               as.character(predict(numeric_model, as.data.frame(numeric_input))),
               tolerance = .0001)
  
  numeric_model_as_pfa <- pfa(numeric_model, pred_type = 'prob')
  numeric_model_engine <- pfa_engine(numeric_model_as_pfa)
  
  expect_equal(numeric_model_engine$action(numeric_input)[numeric_model$levels], 
               predict(numeric_model, as.data.frame(numeric_input), 'raw')[1,],
               tolerance = .0001)
  
})


test_that("check Naive Bayes model with all categorical inputs", {
  
  categorical_input <- list(X1='6', X2='4')
  categorical_input2 <- data.frame(X1=factor('6',levels=c('4','6','8')), 
                                   X2=factor('4',levels=c('3','4','5'))) 
  
  categorical_dat <- data.frame(X1 = factor(mtcars$cyl), 
                                X2 = factor(mtcars$gear), 
                                Y = factor(mtcars$am))
  
  categorical_model <- naiveBayes(Y ~ X1 + X2, data=categorical_dat) 
  categorical_model_as_pfa <- pfa(categorical_model, pred_type = 'response')
  categorical_model_engine <- pfa_engine(categorical_model_as_pfa)
  
  expect_equal(categorical_model_engine$action(categorical_input), 
               as.character(predict(categorical_model, categorical_input2)),
               tolerance = .0001)
  
  categorical_model_as_pfa <- pfa(categorical_model, pred_type = 'prob')
  categorical_model_engine <- pfa_engine(categorical_model_as_pfa)
  
  expect_equal(categorical_model_engine$action(categorical_input)[categorical_model$levels], 
               predict(categorical_model, categorical_input2, 'raw')[1,],
               tolerance = .0001)
  
})


test_that("check Naive Bayes model with mix of categorial and numeric inputs", {

  mixed_input <- list(X1='6', X2='4', X3 = 20, X4 = 90)
  mixed_input2 <- data.frame(X1=factor('6',levels=c('4','6','8')), 
                             X2=factor('4',levels=c('3','4','5')), 
                             X3=20, 
                             X4=90) 
  
  mixed_dat <- data.frame(X1 = factor(mtcars$cyl), 
                          X2 = factor(mtcars$gear), 
                          X3 = mtcars$mpg, 
                          X4 = mtcars$hp,
                          Y = factor(mtcars$am))
  
  mixed_model <- naiveBayes(Y ~ X1 + X2 + X3 + X4, data=mixed_dat) 
  mixed_model_as_pfa <- pfa(mixed_model, pred_type = 'response')
  mixed_model_engine <- pfa_engine(mixed_model_as_pfa)
  
  expect_equal(mixed_model_engine$action(mixed_input), 
               as.character(predict(mixed_model, mixed_input2)),
               tolerance = .0001)
  
  mixed_model_as_pfa <- pfa(mixed_model, pred_type = 'prob')
  mixed_model_engine <- pfa_engine(mixed_model_as_pfa)
  
  expect_equal(mixed_model_engine$action(mixed_input)[mixed_model$levels], 
               predict(mixed_model, mixed_input2, 'raw')[1,],
               tolerance = .0001)
  
})


test_that("check Naive Bayes model with custom prediction cutoffs", {
  
  mixed_input <- list(X1='6', X2='4', X3 = 20, X4 = 90)
  mixed_input2 <- data.frame(X1=factor('6',levels=c('4','6','8')), 
                             X2=factor('4',levels=c('3','4','5')), 
                             X3=20, 
                             X4=90) 
  
  mixed_dat <- data.frame(X1 = factor(mtcars$cyl), 
                          X2 = factor(mtcars$gear), 
                          X3 = mtcars$mpg, 
                          X4 = mtcars$hp,
                          Y = factor(mtcars$am))
  
  mixed_model <- naiveBayes(Y ~ X1 + X2 + X3 + X4, data=mixed_dat)
  
  # test non-uniform cutoffs
  test_cutoffs <- c(`0`=.3, `1`=.7)
  mixed_model_as_pfa <- pfa(mixed_model, cutoffs = test_cutoffs)
  mixed_model_engine <- pfa_engine(mixed_model_as_pfa)

  prob_pred <- predict(mixed_model, mixed_input2, 'raw')[1,]
  cutoff_ratio_adjusted_preds <- prob_pred / test_cutoffs

  expect_equal(mixed_model_engine$action(mixed_input), 
               names(cutoff_ratio_adjusted_preds)[which.max(cutoff_ratio_adjusted_preds)]) 
  
})

test_that("check Naive Bayes model with missing value inputs", {
  
  mixed_input <- list(X1='6', X2=NA, X3 = 20, X4 = NA)
  mixed_input2 <- data.frame(X1=factor('6',levels=c('4','6','8')), 
                             X2=factor(NA,levels=c('3','4','5')), 
                             X3=20, 
                             X4=NA) 
  
  mixed_dat <- data.frame(X1 = factor(mtcars$cyl), 
                          X2 = factor(mtcars$gear), 
                          X3 = mtcars$mpg, 
                          X4 = mtcars$hp,
                          Y = factor(mtcars$am))
  
  mixed_model <- naiveBayes(Y ~ X1 + X2 + X3 + X4, data=mixed_dat)
  
  mixed_model_as_pfa <- pfa(mixed_model, pred_type = 'prob')
  mixed_model_engine <- pfa_engine(mixed_model_as_pfa)
  
  expect_equal(mixed_model_engine$action(mixed_input)[mixed_model$levels], 
               predict(mixed_model, mixed_input2, 'raw')[1,],
               tolerance = .0001)
  
  # test all missing - it should equal the prior probs
  mixed_input_all_na <- list(X1=NA, X2=NA, X3 = NA, X4 = NA)
  mixed_input2_all_na <- data.frame(X1=factor(NA,levels=c('4','6','8')), 
                                    X2=factor(NA,levels=c('3','4','5')), 
                                    X3=NA, 
                                    X4=NA)
  
  expect_equal(mixed_model_engine$action(mixed_input_all_na)[mixed_model$levels], 
               predict(mixed_model, mixed_input2_all_na, 'raw')[1,],
               tolerance = .0001)
  # check against prior probs
  expect_equal(mixed_model_engine$action(mixed_input_all_na)[mixed_model$levels], 
               t(mixed_model$apriori/sum(mixed_model$apriori))[1,],
             tolerance = .0001)
  
})
