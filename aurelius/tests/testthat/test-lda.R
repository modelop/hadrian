context("pfa.lda")

suppressMessages(suppressWarnings(library(MASS)))

test_that("check Linear Discrimininant model with all numeric inputs", {
  
  numeric_input <- list(X1=5.5, 
                        X2=5)
  
  numeric_dat <- data.frame(X1 = iris$Sepal.Length, 
                            X2 = iris$Petal.Length, 
                            Y = iris$Species)
  
  numeric_model <- lda(Y ~ X1 + X2, data=numeric_dat) 
  numeric_model_as_pfa <- pfa(numeric_model, pred_type = 'response')
  numeric_model_engine <- pfa_engine(numeric_model_as_pfa)
  
  expect_equal(numeric_model_engine$action(numeric_input), 
               as.character(predict(numeric_model, as.data.frame(numeric_input))$class),
               tolerance = .0001)
  
  numeric_model_as_pfa <- pfa(numeric_model, pred_type = 'prob')
  numeric_model_engine <- pfa_engine(numeric_model_as_pfa)
  
  expect_equal(numeric_model_engine$action(numeric_input)[numeric_model$lev], 
               predict(numeric_model, as.data.frame(numeric_input))$posterior[1,],
               tolerance = .0001)
})


test_that("check Linear Discrimininant model with all categorical inputs", {
  
  categorical_input <- list(X1='6', X2='4')
  categorical_input2 <- data.frame(X1=factor('6',levels=c('4','6','8')), 
                                   X2=factor('4',levels=c('3','4','5'))) 
  
  categorical_dat <- data.frame(X1 = factor(mtcars$cyl), 
                                X2 = factor(mtcars$gear), 
                                Y = factor(mtcars$am))
  
  categorical_model <- lda(Y ~ X1 + X2, data=categorical_dat) 
  categorical_model_as_pfa <- pfa(categorical_model, pred_type = 'response')
  categorical_model_engine <- pfa_engine(categorical_model_as_pfa)
  
  expect_equal(categorical_model_engine$action(categorical_input), 
               as.character(predict(categorical_model, as.data.frame(categorical_input))$class),
               tolerance = .0001)
  
  categorical_model_as_pfa <- pfa(categorical_model, pred_type = 'prob')
  categorical_model_engine <- pfa_engine(categorical_model_as_pfa)
  
  expect_equal(categorical_model_engine$action(categorical_input)[categorical_model$lev], 
               predict(categorical_model, as.data.frame(categorical_input))$posterior[1,],
               tolerance = .0001)
})


test_that("check Linear Discrimininant model with mix of categorial and numeric inputs", {

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
  
  mixed_model <- lda(Y ~ X1 + X2 + X3 + X4, data=mixed_dat) 
  mixed_model_as_pfa <- pfa(mixed_model, pred_type = 'response')
  mixed_model_engine <- pfa_engine(mixed_model_as_pfa)
  
  expect_equal(mixed_model_engine$action(mixed_input), 
               as.character(predict(mixed_model, as.data.frame(mixed_input))$class),
               tolerance = .0001)
  
  mixed_model_as_pfa <- pfa(mixed_model, pred_type = 'prob')
  mixed_model_engine <- pfa_engine(mixed_model_as_pfa)
  
  expect_equal(mixed_model_engine$action(mixed_input)[mixed_model$lev], 
               predict(mixed_model, as.data.frame(mixed_input))$posterior[1,],
               tolerance = .0001)
})


test_that("check Linear Discrimininant model with custom prediction cutoffs", {
  
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
  
  mixed_model <- lda(Y ~ X1 + X2 + X3 + X4, data=mixed_dat)
  
  # test non-uniform cutoffs
  test_cutoffs <- c(`0`=.3, `1`=.7)
  mixed_model_as_pfa <- pfa(mixed_model, cutoffs = test_cutoffs)
  mixed_model_engine <- pfa_engine(mixed_model_as_pfa)

  prob_pred <- predict(mixed_model, mixed_input2)$posterior[1,]
  cutoff_ratio_adjusted_preds <- prob_pred / test_cutoffs

  expect_equal(mixed_model_engine$action(mixed_input), 
               names(cutoff_ratio_adjusted_preds)[which.max(cutoff_ratio_adjusted_preds)]) 
  
})


test_that("check Linear Discrimininant model with new prior", {
  
  numeric_input <- list(X1=5.5, 
                        X2=5)
  
  numeric_dat <- data.frame(X1 = iris$Sepal.Length, 
                            X2 = iris$Petal.Length, 
                            Y = iris$Species)
  
  numeric_model <- lda(Y ~ X1 + X2, data=numeric_dat) 
  
  new_prior <- c(`setosa`=.39, `versicolor`=.6, `virginica`=.01)
  
  numeric_model_as_pfa <- pfa(numeric_model, prior = new_prior, pred_type = 'response')
  numeric_model_engine <- pfa_engine(numeric_model_as_pfa)
  
  expect_equal(numeric_model_engine$action(numeric_input), 
               as.character(predict(numeric_model, prior = new_prior, as.data.frame(numeric_input))$class),
               tolerance = .0001)
  
  numeric_model_as_pfa <- pfa(numeric_model, prior = new_prior, pred_type = 'prob')
  numeric_model_engine <- pfa_engine(numeric_model_as_pfa)
  
  expect_equal(numeric_model_engine$action(numeric_input)[numeric_model$lev], 
               predict(numeric_model, prior = new_prior, as.data.frame(numeric_input))$posterior[1,],
               tolerance = .0001)
})


test_that("check Linear Discrimininant model with reduced dimensions", {
  
  numeric_input <- list(X1=5.5, 
                        X2=5)
  
  numeric_dat <- data.frame(X1 = iris$Sepal.Length, 
                            X2 = iris$Petal.Length, 
                            Y = iris$Species)
  
  numeric_model <- lda(Y ~ X1 + X2, data=numeric_dat) 
  
  new_dimen <- 1
  
  numeric_model_as_pfa <- pfa(numeric_model, dimen = new_dimen, pred_type = 'response')
  numeric_model_engine <- pfa_engine(numeric_model_as_pfa)
  
  expect_equal(numeric_model_engine$action(numeric_input), 
               as.character(predict(numeric_model, dimen = new_dimen, as.data.frame(numeric_input))$class),
               tolerance = .0001)
  
  numeric_model_as_pfa <- pfa(numeric_model, dimen = new_dimen, pred_type = 'prob')
  numeric_model_engine <- pfa_engine(numeric_model_as_pfa)
  
  expect_equal(numeric_model_engine$action(numeric_input)[numeric_model$lev], 
               predict(numeric_model, dimen = new_dimen, as.data.frame(numeric_input))$posterior[1,],
               tolerance = .0001)
  
})
