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
#Parallel not functioning in RStudio https://github.com/rstudio/rstudio/issues/6692
json_files<-list.files(path =paste0(root_path,"Files/video-content/"),pattern="*.json",full.names = TRUE)
json_list<-parLapply(cl,json_files,function(x) rjson::fromJSON(file=x,method = "R"))
stopCluster(cl)

class(json_list)
# list
length(json_list)
# 72068
lengths(json_list)
# Each list with 64 elements

# Calculating missing values for the main elements in video metadata
library("dplyr")
library("tidyr")

#Function works only for list type variables
function_is_null <- function(x, var = " "){
  len <- length(x)
  null = 0
  for(i in 1:len){
    if (is.null(x[[i]][[var]])){
      null <- null + 1
    }
  }
  cat(var,i)
  return(null)
}

null_dislike_count <- function_is_null(json_list,var = "dislike_count")
null_like_count <- function_is_null(json_list,var = "like_count")
null_id_count_count <- function_is_null(json_list,var = "id")
null_channel_id_count <- function_is_null(json_list,var = "channel_id")
null_categories_count <- function_is_null(json_list,var = "categories")
null_description_count <- function_is_null(json_list,var = "description")
null_webpage_url_count <- function_is_null(json_list,var = "webpage_url")
null_upload_date_count <- function_is_null(json_list,var = "upload_date")
null_average_rating_count <- function_is_null(json_list,var = "average_rating")
null_view_count <- function_is_null(json_list,var = "view_count")
null_thumbnail_count <- function_is_null(json_list,var = "thumbnail")
null_duration_count <- function_is_null(json_list,var = "duration")
null_fulltitle_count <- function_is_null(json_list,var = "fulltitle")

col_names <- c("Dislike Count","Like Count","Video ID", "Channel ID","Categories","Description", "Webpage URL",
               "Upload Date","Average Rating", "View Count","Thumbnail","Duration","Full Title")
df <- data.frame(null_dislike_count,null_like_count,null_id_count_count,null_channel_id_count,null_categories_count,null_description_count,null_webpage_url_count,
          null_upload_date_count,null_average_rating_count,null_view_count,null_thumbnail_count,null_duration_count,null_fulltitle_count)
colnames(df) <- col_names

# Plotting the results
library("ggplot2")
library("reshape2")

cd274 <- melt(df[, 1:ncol(df)])
ggplot(cd274, aes(x = variable, y = value)) + geom_bar(stat = "identity",position = "dodge") +
  labs(x='variable', y="number of missing values", title='Missing Values Analysis Main Variables') +
  coord_flip() + geom_text(aes(label=value))

# Language Detection Analysis
library("cld3")

#Function works only for list type variables
# Test CLD3
fun_transf_df_language_detection_cld3 <- function(x,var1 = " ",var2=" "){
  len <- length(x)
  for(i in 1:len){
    text1<-x[[i]][[var1]]
    text2<-x[[i]][[var2]]
    combinedText<-paste(text1,text2)
    language <- cld3::detect_language(combinedText)
    if(i == 1){
      dataframe <- data.frame(combinedText,language)
      col <- c("Text","Language_Detected")
      colnames(dataframe) <- col
    } else{
      dataframe[nrow(dataframe)+1,] <- cbind(combinedText,language)
    }
  }
  return(dataframe)
}

df_language_fulltitle <- fun_transf_df_language_detection_cld3(json_list,"fulltitle","description")
summary_cld3 <- df_language_fulltitle %>%
  group_by(Language_Detected) %>%
  summarise(count=n()) %>% arrange(desc(count))

# Test Textcat
library("textcat")
fun_transf_df_language_detection_textcat <- function(x,var1 = " ",var2=" "){
  len <- length(x)
  for(i in 1:len){
    text1<-x[[i]][[var1]]
    text2<-x[[i]][[var2]]
    combinedText<-paste(text1,text2)
    language <- textcat::textcat(combinedText,p=textcat::TC_char_profiles)
    if(i == 1){
      dataframe <- data.frame(combinedText,language)
      col <- c("Text","Language_Detected")
      colnames(dataframe) <- col
    } else{
      dataframe[nrow(dataframe)+1,] <- cbind(combinedText,language)
    }
  }
  return(dataframe)
}

df_language_fulltitle_textcat <- fun_transf_df_language_detection_textcat(json_list,"fulltitle","description")
summary_textcat <- df_language_fulltitle_textcat %>%
  group_by(Language_Detected) %>%
  summarise(count=n()) %>% arrange(desc(count))

# Testing the results
# TO DO
sample <- sample(1:length(json_list),round(0.0001*length(json_list)))
for(i in 1:length(sample)){
  if(i == 1){
      test_result <- data.frame(sample[i],json_list[[sample[i]]][["description"]])
      col <- c("RowId","Text")
      colnames(test_result) <- col
    } else{
      test_result[nrow(test_result)+1,] <- cbind(sample[i],json_list[[sample[i]]][["description"]])
    }
}

# Verify how the dataset is balanced
definitive_ids <- read.delim(paste0(root_path,"Files/definitive_videoids_updated_20200901.txt"),header=FALSE,sep=" ",dec=" ")
beauty_fitness <- read.delim(paste0(root_path,"Files/Beauty_Fitness.txt"),header=FALSE,sep=" ",dec=" ")
books_literature <- read.delim(paste0(root_path,"Files/Books_Literature.txt"),header=FALSE,sep=" ",dec=" ")
business_industrial <- read.delim(paste0(root_path,"Files/Business_Industrial.txt"),header=FALSE,sep=" ",dec=" ")
computers_electronics <- read.delim(paste0(root_path,"Files/Computers_Electronics.txt"),header=FALSE,sep=" ",dec=" ")
food_drink <- read.delim(paste0(root_path,"Files/Food_Drink.txt"),header=FALSE,sep=" ",dec=" ")

beauty_fitness$check<- ifelse(beauty_fitness$V1 %in% definitive_ids$V1,"Yes", "No")
new_beauty_fitness <- beauty_fitness %>% filter(check == "Yes")

books_literature$check<- ifelse(books_literature$V1 %in% definitive_ids$V1,"Yes", "No")
new_books_literature <- books_literature %>% filter(check == "Yes")

business_industrial$check<- ifelse(business_industrial$V1 %in% definitive_ids$V1,"Yes", "No")
new_business_industrial <- business_industrial %>% filter(check == "Yes")

computers_electronics$check<- ifelse(computers_electronics$V1 %in% definitive_ids$V1,"Yes", "No")
new_computers_electronics <- computers_electronics %>% filter(check == "Yes")

food_drink$check<- ifelse(food_drink$V1 %in% definitive_ids$V1,"Yes", "No")
new_food_drink <- food_drink %>% filter(check == "Yes")

result_balance<-data.frame(nrow(new_beauty_fitness),nrow(new_books_literature),nrow(new_business_industrial),nrow(new_computers_electronics),nrow(new_food_drink))
colnames(result_balance) <- c("Beauty & Fitness","Books & Literature", "Business & Industrial","Computers & Electronics","Food & Drink")
View(result_balance)

# Check how many Ids are present in all categories and which are exclusive per category
'%notin%' <- Negate('%in%')
definitive_ids$check_all <- ifelse((definitive_ids$V1 %in% food_drink$V1 &
                                               definitive_ids$V1 %in% computers_electronics$V1 &
                                               definitive_ids$V1 %in% business_industrial$V1 &
                                               definitive_ids$V1 %in% books_literature$V1 &
                                               definitive_ids$V1 %in% beauty_fitness$V1),"Yes", "No")

definitive_ids$exclusive_fooddrink <- ifelse(definitive_ids$V1 %in% food_drink$V1 &
                                               definitive_ids$V1 %notin% computers_electronics$V1 &
                                               definitive_ids$V1 %notin% business_industrial$V1 &
                                               definitive_ids$V1 %notin% books_literature$V1 &
                                               definitive_ids$V1 %notin% beauty_fitness$V1,"Yes", "No")

definitive_ids$exclusive_compelect<- ifelse(definitive_ids$V1 %notin% food_drink$V1 &
                                               definitive_ids$V1 %in% computers_electronics$V1 &
                                               definitive_ids$V1 %notin% business_industrial$V1 &
                                               definitive_ids$V1 %notin% books_literature$V1 &
                                               definitive_ids$V1 %notin% beauty_fitness$V1,"Yes", "No")

definitive_ids$exclusive_busind<- ifelse(definitive_ids$V1 %notin% food_drink$V1 &
                                               definitive_ids$V1 %notin% computers_electronics$V1 &
                                               definitive_ids$V1 %in% business_industrial$V1 &
                                               definitive_ids$V1 %notin% books_literature$V1 &
                                               definitive_ids$V1 %notin% beauty_fitness$V1,"Yes", "No")

definitive_ids$exclusive_bookslit<- ifelse(definitive_ids$V1 %notin% food_drink$V1 &
                                               definitive_ids$V1 %notin% computers_electronics$V1 &
                                               definitive_ids$V1 %notin% business_industrial$V1 &
                                               definitive_ids$V1 %in% books_literature$V1 &
                                               definitive_ids$V1 %notin% beauty_fitness$V1,"Yes", "No")

definitive_ids$exclusive_beautyfit<- ifelse(definitive_ids$V1 %notin% food_drink$V1 &
                                               definitive_ids$V1 %notin% computers_electronics$V1 &
                                               definitive_ids$V1 %notin% business_industrial$V1 &
                                               definitive_ids$V1 %notin% books_literature$V1 &
                                               definitive_ids$V1 %in% beauty_fitness$V1,"Yes", "No")

exclusive_balance<-data.frame(nrow(definitive_ids %>% filter(check_all == "Yes")),
                              nrow(definitive_ids %>% filter(exclusive_fooddrink == "Yes")),
                              nrow(definitive_ids %>% filter(exclusive_compelect == "Yes")),
                              nrow(definitive_ids %>% filter(exclusive_busind == "Yes")),
                              nrow(definitive_ids %>% filter(exclusive_bookslit == "Yes")),
                              nrow(definitive_ids %>% filter(exclusive_beautyfit == "Yes")))
colnames(exclusive_balance) <- c("In All Cats","Food & Drink","Computers & Electronics","Business & Industrial","Books & Literature","Beauty & Fitness")

###
definitive_ids$inbeautyfitness<- ifelse(definitive_ids$V1 %in% beauty_fitness$V1,"Yes", "No")
new_beauty_fitness <- definitive_ids %>% filter(inbeautyfitness == "Yes")

definitive_ids$inbooksliterature<- ifelse(definitive_ids$V1 %in% books_literature$V1,"Yes", "No")
new_books_literature <- definitive_ids %>% filter(inbooksliterature == "Yes")

definitive_ids$inbusinessindustrial<- ifelse(definitive_ids$V1 %in% business_industrial$V1,"Yes", "No")
new_business_industrial <- definitive_ids %>% filter(inbusinessindustrial == "Yes")

definitive_ids$incomputer<- ifelse(definitive_ids$V1 %in% computers_electronics$V1,"Yes", "No")
new_computers_electronics <- definitive_ids %>% filter(incomputer == "Yes")

definitive_ids$infooddrink<- ifelse(definitive_ids$V1 %in% food_drink$V1,"Yes", "No")
new_food_drink <- definitive_ids %>% filter(infooddrink == "Yes")

third_balance_test<-data.frame(nrow(new_beauty_fitness),nrow(new_books_literature),nrow(new_business_industrial),nrow(new_computers_electronics),nrow(new_food_drink))
colnames(third_balance_test) <- c("Beauty & Fitness","Books & Literature", "Business & Industrial","Computers & Electronics","Food & Drink")
View(third_balance_test)

melted <- melt(third_balance_test[, 1:ncol(third_balance_test)])
ggplot(melted, aes(x = variable, y = value)) + geom_bar(stat = "identity",position = "dodge") +
  labs(x='YouTube-8M defined category name', y="number of video ids randomly selected", title='Number of IDs selected by Category in YouTube-8M') +
  coord_flip() + geom_text(aes(label=value))

# Check total number of video ids per each original file with category
original_datasets_balance<-data.frame(nrow(beauty_fitness),nrow(books_literature),nrow(business_industrial),nrow(computers_electronics),nrow(food_drink))
colnames(original_datasets_balance) <- c("Beauty & Fitness","Books & Literature", "Business & Industrial","Computers & Electronics","Food & Drink")
View(original_datasets_balance)

melted <- melt(original_datasets_balance[, 1:ncol(original_datasets_balance)])
ggplot(melted, aes(x = variable, y = value)) + geom_bar(stat = "identity",position = "dodge") +
  labs(x='YouTube-8M defined category name', y="Number of video ids", title='Number of Video IDs available per Category') +
  coord_flip() + geom_text(aes(label=value)) +
  theme(axis.text.x = element_blank(),axis.ticks = element_blank())

sum(nrow(beauty_fitness),nrow(books_literature),nrow(business_industrial),nrow(computers_electronics),nrow(food_drink))

# Duplicated analysis: 750 repeated ids
length(definitive_ids[duplicated(definitive_ids$V1),])

#To almost mimic the functionality of store or stores in R, you can do the following. Use save(x,y,z,file="Permdata") to save permanent objects in "permdata". When you exit R, do not save the workspace. Then all temporary objects will disappear. In your .Rprofile put the command load("Permdata") so that the next time you invoke R the permanent objects will be available.
save(json_list,df_language_fulltitle,df_language_fulltitle_textcat, file="Permanentdata")
# Load the saved data
load("Permanentdata")