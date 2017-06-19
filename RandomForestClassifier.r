#importing the dataset
d = read.csv("thirtynorm.csv")
dim(d)
sum(is.na(d))

#creating factors for prediction column
a = factor(d$bb_pred, levels = c(0,1))
m = cbind(d[,-c(2,1)], a)
attach(m)

#Splitting into training and test
Train <- createDataPartition(m$a, p=0.7, list=FALSE)
training <- m[ Train, ]
testing <- m[ -Train, ]

#importing packages
library(caret)
library(mlbench)

#Model - Random Forest using caret package
model <- train(
  a~.,
  tuneGrid = data.frame(mtry = c(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)),
  data = training, method = "ranger", metric = "roc",
  trControl = trainControl(method = "cv", number = 10, verboseIter = TRUE)
)

pred = predict(model, newdata=m)
confusionMatrix(data=pred, m$a)

library(caTools)
colAUC(pred, testing$a, plotROC = TRUE)

table(pred, m$a)
