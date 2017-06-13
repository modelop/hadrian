context("pfa.knn")

suppressMessages(suppressWarnings(library(caret)))
suppressMessages(suppressWarnings(library(ipred)))

test_that("Check knn3 model", {
  
  input <- as.list(iris[73,3:4])
  names(input) <- gsub('\\.', '_', names(input))
  
  knn_model <- knn3(x = iris[, 3:4], y = iris$Species, k = 3)

  knn_model_as_pfa <- pfa(knn_model)
  knn_engine <- pfa_engine(knn_model_as_pfa)
  
  expect_equal(knn_engine$action(input),
               as.character(predict(knn_model, newdata=as.data.frame(input), type = 'class')))
  
  # test non-uniform cutoffs
  test_cutoffs <- c(setosa=.1, versicolor=.8, virginica=.1)
  knn_model_as_pfa <- pfa(knn_model, cutoffs = test_cutoffs)
  knn_engine <- pfa_engine(knn_model_as_pfa)
  
  prob_pred <- unclass(predict(knn_model, newdata=as.data.frame(input), type='prob'))[1,]
  cutoff_ratio_adjusted_preds <- prob_pred / test_cutoffs
  
  expect_equal(knn_engine$action(input), 
               names(cutoff_ratio_adjusted_preds)[which.max(cutoff_ratio_adjusted_preds)])  
  
  knn_model_as_pfa <- pfa(knn_model, pred_type = 'prob')
  knn_engine <- pfa_engine(knn_model_as_pfa)
  
  order_names <- c('virginica', 'versicolor', 'setosa')
  expect_equal(knn_engine$action(input)[order_names],
               predict(knn_model, newdata=as.data.frame(input), type = 'prob')[,order_names])
})

test_that("Check ipredknn model", {
  model <- ipredknn(Species ~ Petal_Length + Petal_Width, data = iris2)
  
})

test_that("Check knnreg model", {
  model <- knnreg(mpg ~ cyl + hp + am + gear + carb, data = mtcars)
})
