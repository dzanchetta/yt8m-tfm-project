# Title     : TODO
# Objective : TODO
# Created by: daniel
# Created on: 11/09/2020

library("readtext")
library("jsonlite")
library("rjson")
library("parallel")

# This is a temporary solution to read the files from local repository
# Later they should be read from HDFS
# File location: https://drive.google.com/file/d/1ERz6um0Wy2Qzjh76AlHZ70dt1Dc26JZJ/view?usp=sharing
# Download and save in local machine. Replace the root_path directory.
root_path <- "/Users/daniel/LocalFiles for TFM/"
setwd(root_path)

cl <- makeCluster(detectCores() - 1)
json_files<-list.files(path =paste0(root_path,"Files/video-content/"),pattern="*.json",full.names = TRUE)
json_list<-parLapply(cl,json_files,function(x) rjson::fromJSON(file=x,method = "R"))
stopCluster(cl)

class(json_list)
# list
length(json_list)
# 72068
lengths(json_list)
# Each list with 64 elements

# Missing values
library("dplyr")
library("tidyr")

null_dislike_count <- 0
null_like_count <- 0
null_id_count <- 0
null_channel_id_count <- 0
null_categories_count <- 0
null_description_count <- 0
len <- length(json_list)

function_is_null <- function(json_list){
  for(i in 1:len){
    print(i)
    if (is.null(json_list[[i]][["dislike_count"]])){
      null_dislike_count <- null_dislike_count + 1
    }
    if (is.null(json_list[[i]][["like_count"]])){
      null_like_count <- null_like_count + 1
    }
    if (is.null(json_list[[i]][["id"]])){
      null_id_count <- null_id_count + 1
    }
    if (is.null(json_list[[i]][["channel_id"]])){
      null_channel_id_count <- null_channel_id_count + 1
    }
    if (is.null(json_list[[i]][["categories"]])){
      null_categories_count <- null_categories_count + 1
    }
    if (is.null(json_list[[i]][["description"]])){
      null_description_count <- null_description_count + 1
    }
  }
}

missing_values <- json_list %>% function_is_null()