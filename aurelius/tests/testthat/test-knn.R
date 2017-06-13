context("pfa.knn")

suppressMessages(suppressWarnings(library(caret)))
suppressMessages(suppressWarnings(library(ipred)))

test_that("Check knn3 model", {
  
  input <- as.list(iris[73,3:4])
  names(input) <- gsub('\\.', '_', names(input))
  
  knn_model <- knn3(iris[, 3:4], k = 10)

  knn_model_as_pfa <- pfa(knn_model)
  knn_engine <- pfa_engine(knn_model_as_pfa)
  
  expect_equal(knn_engine$action(input),
               as.character(predict(knn_model, newdata=as.data.frame(input))))
})

test_that("Check ipredknn model", {
  
  input <- as.list(iris[73,3:4])
  names(input) <- gsub('\\.', '_', names(input))
  
  knn_model <- knn3(iris[, 3:4], k = 10)
  
  knn_model_as_pfa <- pfa(knn_model)
  knn_engine <- pfa_engine(knn_model_as_pfa)
  
  expect_equal(knn_engine$action(input),
               as.character(predict(knn_model, newdata=as.data.frame(input))))
})




model <- knn3(Species ~ Petal_Length + Petal_Width, data = iris2)
model <- ipredknn(Species ~ Petal_Length + Petal_Width, data = iris2)
model <- knnreg(mpg ~ cyl + hp + am + gear + carb, data = mtcars)

test_that("Check knn model with family kmeans and custom cluster names", {
  
  input <- as.list(iris[73,3:4])
  names(input) <- gsub('\\.', '_', names(input))
  
  set.seed(20)
  knn_kmeans_model <- knn(x=iris[,3:4], k=3, family=knnFamily("kmeans"), control=list(iter=5))
  
  custom_cluster_names <- c('virginica', 'versicolor', 'setosa')
  knn_kmeans_model_as_pfa <- pfa(knn_kmeans_model, cluster_names = custom_cluster_names)
  knn_kmeans_engine <- pfa_engine(knn_kmeans_model_as_pfa)
  
  expect_equal(knn_kmeans_engine$action(input), 
               custom_cluster_names[predict(knn_kmeans_model, newdata=as.data.frame(input))])
})

test_that("Check knnsimple model", {
  
  input <- as.list(iris[73,3:4])
  names(input) <- gsub('\\.', '_', names(input))
  
  knnsimple_kmeans_model <- knn(iris[, 3:4], k = 3, family=knnFamily("kmeans"), simple=T)

  knnsimple_kmeans_model_as_pfa <- pfa(knnsimple_kmeans_model)
  knnsimple_kmeans_engine <- pfa_engine(knnsimple_kmeans_model_as_pfa)
  
  expect_equal(knnsimple_kmeans_engine$action(input),
               as.character(predict(knnsimple_kmeans_model, newdata=as.data.frame(input))))
  
})


test_that("Check knn model with family kmedians", {
  
  input <- as.list(iris[73,3:4])
  names(input) <- gsub('\\.', '_', names(input))
  
  knn_kmedians_model <- knn(iris[, 3:4], k = 3, family=knnFamily("kmedians"))

  knn_kmedians_model_as_pfa <- pfa(knn_kmedians_model)
  knn_kmedians_engine <- pfa_engine(knn_kmedians_model_as_pfa)
  
  expect_equal(knn_kmedians_engine$action(input),
               as.character(predict(knn_kmedians_model, newdata=as.data.frame(input))))
})

test_that("Check knn model with family angle", {
  
  input <- as.list(iris[73,3:4])
  names(input) <- gsub('\\.', '_', names(input))
  
  knn_angle_model <- knn(iris[, 3:4], k = 3, family=knnFamily("angle"))

  knn_angle_model_as_pfa <- pfa(knn_angle_model)
  knn_angle_engine <- pfa_engine(knn_angle_model_as_pfa)
  
  expect_equal(knn_angle_engine$action(input),
               as.character(predict(knn_angle_model, newdata=as.data.frame(input))))
})

test_that("Check knn model with family jaccard", {
  
  input <- as.list(apply(iris[73,3:4], c(1,2), as.logical))
  names(input) <- gsub('\\.', '_', names(iris[73,3:4]))
  
  knn_jaccard_model <- knn(iris[, 3:4], k = 3, family=knnFamily("jaccard"))

  knn_jaccard_model_as_pfa <- pfa(knn_jaccard_model)
  knn_jaccard_engine <- pfa_engine(knn_jaccard_model_as_pfa)
  
  expect_equal(knn_jaccard_engine$action(input),
               as.character(predict(knn_jaccard_model, newdata=as.data.frame(input))))
})

test_that("Check knn model with family ejaccard", {
  
  knn_ejaccard_model <- knn(iris[, 3:4], k = 3, family=knnFamily("ejaccard"))
  expect_error(pfa(knn_ejaccard_model), 
             'Currently not supporting cluster models with distance metric ejaccard')
})
