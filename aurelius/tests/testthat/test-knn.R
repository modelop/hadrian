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
  
  input <- list(X1=4.9, X2=1.5)
  
  dat <- data.frame(X1 = iris$Petal.Length, 
                    X2 = iris$Petal.Width, 
                    Y = iris$Species, 
                    stringsAsFactors = FALSE)
  
  knn_model <- ipredknn(Y ~ X1 + X2, data = dat, k = 3)
  
  knn_model_as_pfa <- pfa(knn_model)
  knn_engine <- pfa_engine(knn_model_as_pfa)
  
  expect_equal(knn_engine$action(input),
               as.character(predict(knn_model, newdata=as.data.frame(input), type = 'class')))
  
  knn_model_as_pfa <- pfa(knn_model, pred_type = 'prob')
  knn_engine <- pfa_engine(knn_model_as_pfa)
  
  # ipred is funny it only provides probs on winning class 
  # "either the predicted class or the the proportion of the votes for the winning class."
  winner <- 'versicolor'
  expect_equal(unname(knn_engine$action(input)[winner]),
               predict(knn_model, newdata=as.data.frame(input), type = 'prob'))
})

test_that("Check knnreg model", {
  
  input <- as.list(mtcars[9, c('cyl', 'hp', 'wt')])
  knnreg_model <- knnreg(mpg ~ cyl + hp + wt, data = mtcars)
  
  knnreg_model_as_pfa <- pfa.knnreg(knnreg_model)
  knnreg_engine <- pfa_engine(knnreg_model_as_pfa)
  
  pred <- predict(knnreg_model, newdata=as.data.frame(input))[1,]
  expect_equal(knnreg_engine$action(input),
               weighted.mean(as.numeric(names(pred)), pred))
})
