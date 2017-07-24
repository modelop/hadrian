context("pfa.kmeans")

test_that("Check kmeans models", {
  
  input <- as.list(iris[73,3:4])
  names(input) <- gsub('\\.', '_', names(input))
  
  set.seed(20)
  kmeans_model <- kmeans(x=iris[,3:4], centers=3, iter.max = 5, nstart = 20)

  kmeans_model_as_pfa <- pfa(kmeans_model)
  kmeans_engine <- pfa_engine(kmeans_model_as_pfa)
  
  expect_equal(kmeans_engine$action(input), '2')
})

test_that("Check kmeans models with cluster names", {
  
  input <- as.list(iris[73,3:4])
  names(input) <- gsub('\\.', '_', names(input))
  
  set.seed(20)
  kmeans_model <- kmeans(x=iris[,3:4], centers=3, iter.max = 5, nstart = 20)

  kmeans_model_as_pfa <- pfa(kmeans_model, 
                             cluster_names = c('setosa', 'versicolor', 'virginica'))
  kmeans_engine <- pfa_engine(kmeans_model_as_pfa)
  
  expect_equal(kmeans_engine$action(input), 'versicolor')
})
