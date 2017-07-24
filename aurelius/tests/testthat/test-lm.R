context("pfa.lm")

test_that("check lm models", {
  
  input <- list(X1=3, X2=3)
  
  X1 <- rnorm(100)
  X2 <- runif(100)
  Y <- 3 - 5 * X1 + 3 * X2 + rnorm(100, 0, 3)
  
  lm_model <- lm(Y ~ X1 + X2)
  
  lm_model_as_pfa <- pfa(lm_model)
  lm_engine <- pfa_engine(lm_model_as_pfa)
  
  expect_equal(lm_engine$action(input), 
               unname(predict(lm_model, newdata = as.data.frame(input))),
               tolerance = .0001)
  
  no_int_lm_model <- lm(Y ~ X1 + X2 - 1)
  
  no_int_lm_model_as_pfa <- pfa(no_int_lm_model)
  no_int_lm_engine <- pfa_engine(no_int_lm_model_as_pfa)
  
  expect_equal(no_int_lm_engine$action(input), 
               unname(predict(no_int_lm_model, newdata = as.data.frame(input))),
               tolerance = .0001)
  
})