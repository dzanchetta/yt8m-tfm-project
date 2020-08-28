#Change the paths below to the ones from your local machine
#Hadoop binary
HDFS_DIR=/Users/daniel/hadoop-2.7.3/bin
#Dir to find the files (Mac accepted format)
SEARCH_DIR_COMMENTS=/Users/daniel/LocalFiles\ for\ TFM/Files/comments
DIR_FOR_HDFS_COMMENTS=/Users/daniel/LocalFiles%20for%20TFM/Files/comments
DIR_DEST_COMMENTS=/user/daniel/comments/

for file in $SEARCH_DIR_COMMENTS/*.comment.json; do
    $HDFS_DIR/hdfs dfs -put $DIR_FOR_HDFS_COMMENTS/"$(basename "$file")" $DIR_DEST_COMMENTS
done