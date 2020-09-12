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

jsonc <- toJSON(json_list)
write(jsonc, file = "jsonc")