# File:  tweetR.R
# Description:  Access Twitter Streaming API or Search API to pull and analyze tweets.
# 

library(twitteR)
library(streamR)
library(ROAuth)
library(RCurl)
library(rjson)
library(qdap)
library(httr)
library(httpuv)

setwd("~/Desktop/twitteR")
consumer_key <- "xxxx"
consumer_secret <- "xxxx"

## STREAMING API:

# Download cert, setup OAuth and handshake
download.file(url="http://curl.haxx.se/ca/cacert.pem",
              destfile="cacert.pem")

twitCred <- OAuthFactory$new(consumerKey=consumer_key,
                             consumerSecret=consumer_secret,
                             requestURL="https://api.twitter.com/oauth/request_token",
                             accessURL="https://api.twitter.com/oauth/access_token",
                             authURL="https://api.twitter.com/oauth/authorize")

twitCred$handshake(cainfo = system.file("CurlSSL", "cacert.pem", package="RCurl"))

filterStream( file="output/output.json",
              #locations=c(-97.825270, 30.234782, -97.673521, 30.324765),
              track="#hashtag",
              timeout=5,
              oauth=twitCred)

tweets <- parseTweets("output/output.json", simplify=FALSE, verbose=TRUE)
names(tweets)
write.csv(tweets, file="raw_tweets.csv")

## SEARCH API:

# Set up OAuth, pull tweets, convert to DF, write to CSV
setup_twitter_oauth(consumer_key, consumer_secret)

tweetsList <- searchTwitter("#hashtag", 
                            n=10000,
                            #lang=NULL,
                            #locale=NULL,
                            #geocode="30.272323,-97.743784,10mi",
                            since="2015-06-01")

tweetsDF <- twListToDF(tweetsList)
names(tweetsDF)
write.csv(tweetsDF, file="raw_tweets.csv")

