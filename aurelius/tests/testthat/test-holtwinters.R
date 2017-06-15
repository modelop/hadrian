context("pfa.HoltWinters")

test_that("check Holt Winters model", {
  
  ts_model <- HoltWinters(co2)
  
  hw_as_pfa <- pfa(ts_model)
  hw_engine <- pfa_engine(hw_as_pfa)

  expect_equal(unlist(hw_engine$action(list(h=1, y=NULL))), 
               as.numeric(predict(ts_model, n.ahead=1)))
  
  # test multiple time horizons
  expect_equal(unlist(hw_engine$action(list(h=10, y=NULL))), 
               as.numeric(predict(ts_model, n.ahead=10)))
})

test_that("check different specifications of Holt Winters model", {
  
  ts_model <- HoltWinters(co2)
  hw_as_pfa <- pfa(ts_model)
  hw_engine <- pfa_engine(hw_as_pfa)
  expect_equal(unlist(hw_engine$action(list(h=1, y=NULL))), 
               as.numeric(predict(ts_model, n.ahead=1)))
  
  ts_model <- HoltWinters(AirPassengers, seasonal = "mult")
  hw_as_pfa <- pfa(ts_model)
  hw_engine <- pfa_engine(hw_as_pfa)
  expect_equal(unlist(hw_engine$action(list(h=1, y=NULL))), 
               as.numeric(predict(ts_model, n.ahead=1)))
  
  ## Non-Seasonal Holt-Winters
  x <- uspop + rnorm(uspop, sd = 5)
  ts_model <- HoltWinters(x, gamma = FALSE)
  hw_as_pfa <- pfa(ts_model)
  hw_engine <- pfa_engine(hw_as_pfa)
  expect_equal(unlist(hw_engine$action(list(h=1, y=NULL))), 
               as.numeric(predict(ts_model, n.ahead=1)))
  
  ## Exponential Smoothing
  ts_model <- HoltWinters(x, gamma = FALSE, beta = FALSE)
  hw_as_pfa <- pfa(ts_model)
  hw_engine <- pfa_engine(hw_as_pfa)
  expect_equal(unlist(hw_engine$action(list(h=1, y=NULL))), 
               as.numeric(predict(ts_model, n.ahead=1)))
})

test_that("test updating Holt Winters models", {
  
  ts_model <- HoltWinters(co2)
  hw_as_pfa <- pfa(ts_model)
  hw_engine <- pfa_engine(hw_as_pfa)
  
  initial_A <- hw_engine$action(list(h=1, y=NULL))
  invisible(hw_engine$action(list(h=NULL, y=NULL)))
  initial_C <- hw_engine$action(list(h=2, y=NULL))
  expect_equal(initial_A[[1]][1], initial_C[[1]][1])
  
  # update the model and check that the next step prediction changed
  next_step <- hw_engine$action(list(h=1, y=360))
  expect_false(initial_A[[1]][1] == next_step[[1]][1])
  
  invisible(hw_engine$action(list(h=NULL, y=360)))
  two_stepsA <- hw_engine$action(list(h=1, y=NULL))
  
  hw_engine <- pfa_engine(hw_as_pfa)
  invisible(hw_engine$action(list(h=NULL, y=360)))
  invisible(hw_engine$action(list(h=NULL, y=360)))
  two_stepsB <- hw_engine$action(list(h=1, y=NULL))
  
  expect_equal(two_stepsA[[1]][1], two_stepsB[[1]][1])
})