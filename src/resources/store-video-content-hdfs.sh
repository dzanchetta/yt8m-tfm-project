#TO DO Improvement: separate the process into different shell scripts

#Change the paths below to the ones from your local machine
#Hadoop binary
HDFS_DIR=/Users/daniel/hadoop-2.7.3/bin
SEARCH_DIR_ALL=/Users/daniel/LocalFiles\ for\ TFM/Files/yt8m
DIR_FOR_HDFS_ALL=/Users/daniel/LocalFiles%20for%20TFM/Files/yt8m
DIR_DEST_MOVIE=/user/daniel/video-content/

for file in "$SEARCH_DIR_ALL"/*.info.json; do
    $HDFS_DIR/hdfs dfs -put $DIR_FOR_HDFS_ALL/"$(basename "$file")" $DIR_DEST_MOVIE
done