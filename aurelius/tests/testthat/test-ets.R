context("pfa.ets")

suppressMessages(suppressWarnings(library(forecast)))

test_model_type <- function(model_type){
  
  model <- ets(USAccDeaths, model=model_type, damped=TRUE)
  as_pfa <- pfa(model)
  engine <- pfa_engine(as_pfa)
  expect_equal(unlist(engine$action(list(h=1))), 
               as.numeric(forecast(model, h=1)$mean))
  
  model <- ets(USAccDeaths, model=model_type, damped=FALSE)
  as_pfa <- pfa(model)
  engine <- pfa_engine(as_pfa)
  expect_equal(unlist(engine$action(list(h=1))), 
               as.numeric(forecast(model, h=1)$mean))
}

test_that("check ets model", {
  
  ets_model <- ets(USAccDeaths, model="AAA", damped=FALSE)
  
  ets_as_pfa <- pfa(ets_model)
  ets_engine <- pfa_engine(ets_as_pfa)

  expect_equal(unlist(ets_engine$action(list(h=1))), 
               as.numeric(forecast(ets_model, h=1)$mean))
  
  # test multiple time horizons
  expect_equal(unlist(ets_engine$action(list(h=10))), 
               as.numeric(forecast(ets_model, h=10)$mean))
})

test_that("check different specifications of ets models", {
  
  test_model_type(model_type="ZZZ")
  
  test_model_type(model_type="AAA")
  test_model_type(model_type="AAN")
  
  test_model_type(model_type="MAA")
  test_model_type(model_type="MAN")
  
  # expect errors on unsupported model types
  expect_error(test_model_type(model_type="MAM"),
               'Currently not supporting models with multiplicative seasonality')
  expect_error(test_model_type(model_type="MMM"),
               'Currently not supporting models with multiplicative trend')
})

test_that("test cycle_reset argument", {
  
  ets_model <- ets(USAccDeaths, model="AAA", damped=FALSE)
  
  ets_as_pfa <- pfa(ets_model, cycle_reset = FALSE)
  ets_engine <- pfa_engine(ets_as_pfa)  

  first_pred <- ets_engine$action(list(h=1))
  second_pred <- ets_engine$action(list(h=1))
  
  expect_equal(c(first_pred, second_pred), 
               as.numeric(forecast(ets_model, h=2)$mean))
})