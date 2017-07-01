context("pfa.arima")

suppressMessages(suppressWarnings(library(forecast)))

test_model_type <- function(order, seasonal){
  
  model <- arima(USAccDeaths, order=order, seasonal=seasonal)
  as_pfa <- pfa(model)
  engine <- pfa_engine(as_pfa)
  expect_equal(unlist(engine$action(list(h=1))), 
               as.numeric(predict(model, n.ahead=1)$pred))
}

test_that("check ARIMA model", {
  
  arima_model <- arima(presidents, c(3, 1, 1))
  
  arima_as_pfa <- pfa(arima_model)
  arima_engine <- pfa_engine(arima_as_pfa)

  expect_equal(unlist(arima_engine$action(list(h=1))), 
               as.numeric(predict(arima_model, n.ahead=1)$pred))
  
  # test multiple time horizons
  expect_equal(unlist(arima_engine$action(list(h=10))), 
               as.numeric(predict(arima_model, n.ahead=10)$pred))
})

test_that("check different specifications of ARIMA models", {
  
  test_model_type(order=c(0,0,0), seasonal=list(order=c(0,0,0)))
  test_model_type(order=c(1,0,0), seasonal=list(order=c(0,0,0)))
  test_model_type(order=c(0,1,0), seasonal=list(order=c(0,0,0)))
  test_model_type(order=c(0,0,1), seasonal=list(order=c(0,0,0)))
  test_model_type(order=c(1,1,0), seasonal=list(order=c(0,0,0)))
  test_model_type(order=c(0,1,1), seasonal=list(order=c(0,0,0)))
  test_model_type(order=c(2,2,2), seasonal=list(order=c(0,0,0)))
  
  test_model_type(order=c(0,0,0), seasonal=list(order=c(1,0,0)))
  test_model_type(order=c(0,0,0), seasonal=list(order=c(0,1,0)))
  test_model_type(order=c(0,0,0), seasonal=list(order=c(0,0,1)))
                  
  test_model_type(order=c(2,2,2), seasonal=list(order=c(1,1,0)))
})

test_that("test model with external regressors", {      
  lake_huron_xtime <- as.numeric(time(LakeHuron) - 1920)
  input <- lake_huron_xtime[length(lake_huron_xtime)]+(1:10)
  model <- arima(LakeHuron, order = c(2,0,0), xreg = lake_huron_xtime)
  as_pfa <- pfa(model)
  engine <- pfa_engine(as_pfa)
  expect_equal(unlist(engine$action(list(h=3, xreg=list(lake_huron_xtime=input)))), 
               as.numeric(predict(model, n.ahead=10, newxreg = as.data.frame(input))$pred))
})

test_that("test cycle_reset argument", {
  
  arima_model <- arima(presidents, c(3, 1, 1))
  
  arima_as_pfa <- pfa(arima_model, cycle_reset=FALSE)
  arima_engine <- pfa_engine(arima_as_pfa)
  
  first_pred <- arima_engine$action(list(h=1))
  second_pred <- arima_engine$action(list(h=1))
  
  expect_equal(c(first_pred, second_pred), 
               as.numeric(predict(arima_model, n.ahead=2)$pred))
})

test_that("test ARIMA models created by forecast package", {
  
  arima_model <- Arima(presidents, c(3, 1, 1))
  arima_as_pfa <- pfa(arima_model)
  arima_engine <- pfa_engine(arima_as_pfa)
  expect_equal(unlist(arima_engine$action(list(h=1))), 
               as.numeric(forecast(arima_model, h=1)$mean))
  
  arima_model <- auto.arima(presidents)
  arima_as_pfa <- pfa(arima_model)
  arima_engine <- pfa_engine(arima_as_pfa)
  expect_equal(unlist(arima_engine$action(list(h=1))), 
               as.numeric(forecast(arima_model, h=1)$mean))
})