# Title     : TODO
# Objective : TODO
# Created by: daniel
# Created on: 12/09/2020

# References:
# WordCloud: https://acadgild.com/blog/text-mining-using-r
# Reading multiple files: https://cran.r-project.org/web/packages/readtext/vignettes/readtext_vignette.html

# Importing the different libraries
# Reticulate purposes is to connect to Python script
library("reticulate")
library("readtext")
library("NLP")
library("tm")
library("RColorBrewer")
library("wordcloud")
library("wordcloud2")

root_path <- "/Users/daniel/LocalFiles for TFM/"
setwd(root_path)

# Process to clean up and read the Transcript Data

# Connecting to Python
#python_path="/usr/local/bin/python3"
#use_python(python = python_path, required = TRUE)
# Checking python configuration
#py_config()

#Run only in the first execution the python script from R. It will transform the vtt files to txt
#py_run_file("./youtubeProjectTFM/src/vtt2text.py")

# Reading and formatting the file
DATA_DIR <- system.file(root_path,package = "readtext")
file_df <- readtext(paste0(DATA_DIR,"Files/ALL_Transcripts/*.txt"))
#file_df<- data.frame(file)
# Drop doc_id column
file_df<- subset(file_df,select = -c(doc_id))
file_df<- data.frame(Text = file_df)
#head(file_df)

# Process to clean the text
print("Removing backslash")
file_df_collapse<-gsub("\\\\","",file_df)
#head(file_df_collapse)

print("To lower case")
file_df_collapse_lower <- tolower(file_df_collapse)
#head(file_df_collapse_lower)
#Remove punctuation
print("Removing punctuation")
file_df_collapse_lower_rp <- gsub(pattern = "\\W", " ", file_df_collapse_lower)
#Remove digit
print("Removing digit")
file_df_collapse_lower_rp_rd <- gsub(pattern = "\\d", " ", file_df_collapse_lower_rp)

# TO BE CHECKED it stopwords is the best function, or if it is removing more than actually needed
# stopwords()
print("Removing stop words")
# DOES NOT WORK, TAKING MORE THAN 6 HOURS TO EXECUTE AND NOT FINISHED
#file_df_collapse_lower_rp_rd_sw <- removeWords(file_df_collapse_lower_rp_rd, words = stopwords("english"))
library("parallel")
cl <- makeCluster(detectCores() - 1)
file_df_collapse_lower_rp_rd_sw<-parLapply(cl,file_df_collapse_lower_rp_rd,function(x) tm::removeWords(x,words = tm::stopwords("en")))
stopCluster(cl)

# Remove single letters
print("Removing single letters")
file_df_collapse_lower_rp_rd_sw_sl <- gsub(pattern = "\\b[A-z]\\b{1}"," ", file_df_collapse_lower_rp_rd_sw)

# Remove white spaces using stripWhitespace() function,which is a part of  tm library.
print("Removing white spaces")
file_df_collapse_lower_rp_rd_sw_sl_stp <- stripWhitespace(file_df_collapse_lower_rp_rd_sw_sl)

# Frequency of words:
# Split individual words and add space between them
print("Processing frequency words")
file_df_collapse_lower_rp_rd_sw_sl_stp_f <- strsplit(file_df_collapse_lower_rp_rd_sw_sl_stp, " ")
#head(file_df_collapse_lower_rp_rd_sw_sl_stp_f)

# Creation of word frequency
word_freq <- table(file_df_collapse_lower_rp_rd_sw_sl_stp_f)
#head(word_freq)

word_freq1 <- cbind(names(word_freq),as.integer(word_freq))
head(word_freq1)

# WordCloud
#The feature of words can be illustrated as a word cloud as follow :
# - Word clouds add clarity and simplicity.
# - The most used keywords stand out better in a word cloud.
# - Word clouds are a dynamic tool for communication.  Easy to understand, to be shared and are impressive words representation.

# Parameters:
# words: the words to be plotted i.e; word_cloud where we have saved the text-data.
# Freq: word frequencies
# min.freq: words with a frequency below min.freq will not be plotted.
# max .words: maximum number of words to be plotted
# random.order: plot words in random order. If false, then words will be plotted in decreasing frequency
# Rot.per: to adjust  proportion words with 90-degree rotation (vertical text)
# brewer.pal: ??brewer.pal,  ?? command to see the functionality in R
# colors: color words from least to most frequent. Use, for example, colors =“Red” for a single color or “random-dark”, “random-light”.
print("Processing Word Cloud")
word_cloud <- unlist(file_df_collapse_lower_rp_rd_sw_sl_stp_f)
#Option 1
wordcloud(word_cloud,min.freq = 50, max.words=500, random.order=F, rot.per=0.2, colors=brewer.pal(5, "Dark2"), scale=c(4,0.2))
#Option 2
wordcloud2(word_freq,color = "random-dark", backgroundColor = "white",size = 0.7)