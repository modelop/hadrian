context("pfa.kcca")

suppressMessages(suppressWarnings(library(flexclust)))

test_that("Check kcca model with family kmeans", {
  
  input <- as.list(iris[73,3:4])
  names(input) <- gsub('\\.', '_', names(input))
  
  kcca_kmeans_model <- kcca(iris[, 3:4], k = 3, family=kccaFamily("kmeans"))

  kcca_kmeans_model_as_pfa <- pfa(kcca_kmeans_model)
  kcca_kmeans_engine <- pfa_engine(kcca_kmeans_model_as_pfa)
  
  expect_equal(kcca_kmeans_engine$action(input),
               as.character(predict(kcca_kmeans_model, newdata=as.data.frame(input))))
})

test_that("Check kcca model with family kmeans and custom cluster names", {
  
  input <- as.list(iris[73,3:4])
  names(input) <- gsub('\\.', '_', names(input))
  
  set.seed(20)
  kcca_kmeans_model <- kcca(x=iris[,3:4], k=3, family=kccaFamily("kmeans"), control=list(iter=5))
  
  custom_cluster_names <- c('virginica', 'versicolor', 'setosa')
  kcca_kmeans_model_as_pfa <- pfa(kcca_kmeans_model, cluster_names = custom_cluster_names)
  kcca_kmeans_engine <- pfa_engine(kcca_kmeans_model_as_pfa)
  
  expect_equal(kcca_kmeans_engine$action(input), 
               custom_cluster_names[predict(kcca_kmeans_model, newdata=as.data.frame(input))])
})

test_that("Check kccasimple model", {
  
  input <- as.list(iris[73,3:4])
  names(input) <- gsub('\\.', '_', names(input))
  
  kccasimple_kmeans_model <- kcca(iris[, 3:4], k = 3, family=kccaFamily("kmeans"), simple=T)

  kccasimple_kmeans_model_as_pfa <- pfa(kccasimple_kmeans_model)
  kccasimple_kmeans_engine <- pfa_engine(kccasimple_kmeans_model_as_pfa)
  
  expect_equal(kccasimple_kmeans_engine$action(input),
               as.character(predict(kccasimple_kmeans_model, newdata=as.data.frame(input))))
  
})


test_that("Check kcca model with family kmedians", {
  
  input <- as.list(iris[73,3:4])
  names(input) <- gsub('\\.', '_', names(input))
  
  kcca_kmedians_model <- kcca(iris[, 3:4], k = 3, family=kccaFamily("kmedians"))

  kcca_kmedians_model_as_pfa <- pfa(kcca_kmedians_model)
  kcca_kmedians_engine <- pfa_engine(kcca_kmedians_model_as_pfa)
  
  expect_equal(kcca_kmedians_engine$action(input),
               as.character(predict(kcca_kmedians_model, newdata=as.data.frame(input))))
})

test_that("Check kcca model with family angle", {
  
  input <- as.list(iris[73,3:4])
  names(input) <- gsub('\\.', '_', names(input))
  
  kcca_angle_model <- kcca(iris[, 3:4], k = 3, family=kccaFamily("angle"))

  kcca_angle_model_as_pfa <- pfa(kcca_angle_model)
  kcca_angle_engine <- pfa_engine(kcca_angle_model_as_pfa)
  
  expect_equal(kcca_angle_engine$action(input),
               as.character(predict(kcca_angle_model, newdata=as.data.frame(input))))
})

test_that("Check kcca model with family jaccard", {
  
  input <- as.list(apply(iris[73,3:4], c(1,2), as.logical))
  names(input) <- gsub('\\.', '_', names(iris[73,3:4]))
  
  kcca_jaccard_model <- kcca(iris[, 3:4], k = 3, family=kccaFamily("jaccard"))

  kcca_jaccard_model_as_pfa <- pfa(kcca_jaccard_model)
  kcca_jaccard_engine <- pfa_engine(kcca_jaccard_model_as_pfa)
  
  expect_equal(kcca_jaccard_engine$action(input),
               as.character(predict(kcca_jaccard_model, newdata=as.data.frame(input))))
})

test_that("Check kcca model with family ejaccard", {
  
  kcca_ejaccard_model <- kcca(iris[, 3:4], k = 3, family=kccaFamily("ejaccard"))
  expect_error(pfa(kcca_ejaccard_model), 
             'Currently not supporting cluster models with distance metric ejaccard')
})
