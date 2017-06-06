# Author: Andrew Minard
# Data Source: Undisclosed IR100 Mass Merchant / Dept. Store retailer
# Data Description: Web analytics data merged with Ratings & Reviews data from 3rd-party provider
# Description: Predict e-commerce orders based on a product's number of reviews, avg rating, pageviews, and item price.
#

# Read in data
data <- read.csv("~/may2013_clean2.csv")

# subset for just Men's category, then again to remove rows with 0 Orders to avoid log errors
m <- subset(data, data$name1 == "Men")
m1 <- subset(m, m$Orders > 0)

# summarize to explore each set
summary(data)
summary(m1)

# plot histograms to discover skewness
layout(matrix(c(1,2,3,4),2,2))

hist(m1$AvgRating)
hist(m1$NumApprovedReviews)
hist(log(m1$Average.Item.Price))
hist(m1$Orders)

# log-transformed histograms to identify log-normal distributions
hist(log(m1$NumApprovedReviews))
hist(log(m1$Revenue))
hist(log(m1$Orders))

plot(log(m1$Sessions), log(m1$Orders))
plot(log(m1$Average.Item.Price),log(m1$Orders))

# log-transformed scatterplot to showcase effects
plot(log(m1$NumApprovedReviews), log(m1$Orders), xlab="Review Volume", ylab="Orders", main="May 2013 - Mens")

# plot all combinations of variables to explore relationships
pairs(~ log(Orders) + log(m1$Product.Views) + log(m1$NumApprovedReviews) + m1$AvgRating + log(m1$Average.Item.Price),data=m1)

# plot all variables against Orders individually
plot(log(m1$NumApprovedReviews), log(m1$Orders), xlab="Review Volume", ylab="Orders")
plot(m1$AvgRating, log(m1$Orders), xlab="AvgRating", ylab="Orders")
plot(log(m1$Sessions), log(m1$Orders), xlab="Sessions", ylab="Orders")
plot(log(m1$Revenue), log(m1$Orders), xlab="Revenue", ylab="Orders")
plot(log(m1$Product.Views), log(m1$Orders), xlab="ProductViews", ylab="Orders")

plot(log(m1$NumApprovedReviews), log(m1$Orders), xlab="Review Volume", ylab="Orders")
cor.test(m1$Product.Views, m1$Orders)
names(m1)

# Try out different models for best r-squared and significant factors. Add and remove factors individually, optimizing for r-squared.
model0 <- lm(log(m1$Orders) ~ log(m1$Product.Views))
summary(model0)
plot(model0)

model0a <- lm(log(m1$Orders) ~ m1$Average.Item.Price)
summary(model0a)
plot(model0a)

model1 <- lm(log(m1$Orders) ~ log(m1$Product.Views) + log(m1$NumApprovedReviews) + m1$AvgRating + log(m1$Average.Item.Price))
summary(model1)
plot(model1)
   
# Best one! Now, use this to predict Orders by plugging in the other values. Start with medians, then change for further analysis.
# *** Need this syntax for predict.lm() to work correctly... No "$" character. Instead, use second parameter to designate the data.frame in use. ***
model1 <- lm(log(Orders) ~ log(Product.Views) + log(NumApprovedReviews) + AvgRating + log(Average.Item.Price), m1)
plot(model1)
summary(model1)

# Read in CSV of predictor X values into new data.frame.
newdata <- read.csv("~/ratingIncrease_Mens.csv")
# Predict Y (orders) using our best model,
predict.lm(model1, newdata)
# Add a new column of the predicted values back into data.frame
newdata$pOrders <- predict.lm(model1, newdata)

# Reverse the log-transform to get predicted Orders back to real values, 
# Add column to data.frame, then export CSV with results.
newdata$Orders <- exp(newdata$pOrders)
write.csv(newdata, file="predicted2.csv")

# Results can now be visualized in the analyst's tool of choice...
# ggplot (R), Python, Excel PivotChart, Tableau, etc.
