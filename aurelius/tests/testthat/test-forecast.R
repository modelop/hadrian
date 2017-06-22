context("pfa.forecast")

suppressMessages(suppressWarnings(library(forecast)))

test_that("check various forecast models", {
  
  model1 <- holt(airmiles)
  model1_as_pfa <- pfa(model1)
  engine <- pfa_engine(model1_as_pfa)
  expect_equal(unlist(engine$action(list(h=1))), 
               as.numeric(forecast(model1$model, h=1)$mean))

  model2 <- hw(USAccDeaths,h=48)
  model2_as_pfa <- pfa(model2)
  engine <- pfa_engine(model2_as_pfa)
  expect_equal(unlist(engine$action(list(h=1))), 
               as.numeric(forecast(model2$model, h=1)$mean))

  model3 <- ses(LakeHuron)
  model3_as_pfa <- pfa(model3)
  engine <- pfa_engine(model3_as_pfa)
  expect_equal(unlist(engine$action(list(h=1))), 
               as.numeric(forecast(model3$model, h=1)$mean))
})