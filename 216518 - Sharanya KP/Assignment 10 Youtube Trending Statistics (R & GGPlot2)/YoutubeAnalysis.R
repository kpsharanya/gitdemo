# Case Study - Youtube Trending Statistics Analysis

library(ggplot2)
library(dplyr)
library(lubridate)
library(tidyr)
library(DT)
library(ggthemes)
library(SnowballC)

## Explore & Read Uber NYC Trip data for year 2014
#IN_Video <- read.csv("E:/R/INvideos.csv")
#GB_Video <- read.csv("E:/R/GBvideos.csv")
#FR_Video <- read.csv("E:/R/FRvideos.csv")
#CA_Video <- read.csv("E:/R/CAvideos.csv")
#US_Video <- read.csv("E:/R/USvideos.csv")

## row wise concatination of records
#videos <- rbind(IN_Video,GB_Video,FR_Video,
#                CA_Video,US_Video)


#head(videos)

# a) Read the YouTube stat from locations = CA, FR, GB, IN, US and prepare the data. 


IN_Video = tail(read.csv("E:/R/INvideos.csv",encoding = "UTF-8"),20000)

head(IN_Video)

IN_Video$trending_date <- ydm(IN_Video$trending_date)
IN_Video$publish_time <- ydm(substr(IN_Video$publish_time, 
                                    start = 0, stop = 9))
tail(IN_Video)


GB_Video = tail(read.csv("E:/R/GBvideos.csv",encoding = "UTF-8"),20000)

head(GB_Video)

GB_Video$trending_date <- ydm(GB_Video$trending_date)
GB_Video$publish_time <- ydm(substr(GB_Video$publish_time, 
                                    start = 1, stop = 10))
tail(GB_Video)

FR_Video = tail(read.csv("E:/R/FRvideos.csv",encoding = "UTF-8"),20000)

head(FR_Video)

FR_Video$trending_date <- ydm(FR_Video$trending_date)
FR_Video$publish_time <- ydm(substr(FR_Video$publish_time, 
                                    start = 1, stop = 10))
tail(FR_Video)

CA_Video = tail(read.csv("E:/R/CAvideos.csv",encoding = "UTF-8"),20000)

head(CA_Video)

CA_Video$trending_date <- ydm(CA_Video$trending_date)
CA_Video$publish_time <- ydm(substr(CA_Video$publish_time, 
                                    start = 1, stop = 10))
tail(CA_Video)

US_Video = tail(read.csv("E:/R/USvideos.csv",encoding = "UTF-8"),20000)

head(US_Video)

US_Video$trending_date <- ydm(US_Video$trending_date)
US_Video$publish_time <- ydm(substr(US_Video$publish_time, 
                                    start = 1, stop = 10))
tail(US_Video)

## row wise concatination of records
videos <- rbind(IN_Video,GB_Video,FR_Video,
                       CA_Video,US_Video)

head(videos)
sapply(videos, class)


#b) Display the correlation plot between category_id, views, likes, dislikes, comment_count. Which two have stronger and weaker correlation
video_df <- videos[,8:11]
groups <- videos[,5]
head(video_df)

pairs(video_df,labels= colnames(video_df),
      pch = 21,
      bg = rainbow(4)[groups],
      col = rainbow(4)[groups])

corrplot(cor(video_df),method = 'number')
corrplot(cor(video_df),method = 'color')
corrplot(cor(video_df),method = 'pie')

#c) Display Top 10 most viewed videos of YouTube.
mostv <- head(videos %>%
                group_by(video_id,title) %>%
                dplyr::summarise(Total_v = sum(views)) %>%
              arrange(desc(Total_v)),10)
datatable(mostv)

ggplot(mostv,aes(video_id,Total_v))+
  geom_bar(stat = "identity",fill="blue")+
  ggtitle("Top 10 most viewed videos of youtube")

#d) Show Top 10 most liked videos on YouTube.
mostl <- head(videos %>%
                group_by(video_id,title) %>%
                dplyr::summarise(Total_likes = sum(likes)) %>%
                arrange(desc(Total_likes)),10)
datatable(mostl)
ggplot(mostl,aes(video_id,Total_likes))+
  geom_bar(stat = "identity",fill="blue")+
  ggtitle("Top 10 most liked videos of youtube")
 
#e) Show Top 10 most disliked videos on YouTube.
mostd <- head(videos %>%
                group_by(video_id,title) %>%
                dplyr::summarise(Total_dislikes = sum(dislikes)) %>%
                arrange(desc(Total_dislikes)),10)
datatable(mostd)
ggplot(mostd,aes(video_id,Total_dislikes))+
  geom_bar(stat = "identity",fill="blue")+
  ggtitle("Top 10 most disliked videos of youtube")

#f) Show Top 10 most commented video of YouTube
mostcmt <- head(videos %>%
                group_by(video_id,title) %>%
                dplyr::summarise(Total = sum(comment_count)) %>%
                arrange(desc(Total)),10)
datatable(mostcmt)

ggplot(mostcmt,aes(video_id,Total))+
  geom_bar(stat = "identity",fill="blue")+
  ggtitle("Top 10 most commented videos of youtube")

#g) Show Top 15 videos with maximum percentage (%) of Likes on basis of views on video. 

max_likes <- head(videos %>%
                group_by(video_id,title) %>%
                dplyr::summarise(Total =
                round (100* max (likes, na.rm = T)/ max (views, na.rm = T))) %>%
                arrange(desc(Total)),15)
datatable(max_likes)

ggplot(max_likes,aes(video_id,Total))+
  geom_bar(stat = "identity",fill="blue")+
  ggtitle("Top 15 videos with maximum percentage (%) of Likes on basis of views on video")

#h) Show Top 15 videos with maximum percentage (%) of Dislikes on basis of views on video.

max_dislikes <- head(videos %>%
                    group_by(video_id,title) %>%
                    dplyr::summarise(Total =
                                       round (100* max (dislikes, na.rm = T)/ max (views, na.rm = T))) %>%
                    arrange(desc(Total)),15)
datatable(max_dislikes)

ggplot(max_dislikes,aes(video_id,Total))+
  geom_bar(stat = "identity",fill="blue")+
  ggtitle("Top 15 videos with maximum percentage (%) of disLikes on basis of views on video")
              
# i)Show Top 15 videos with maximum percentage (%) of Comments on basis of views on video.
max_comment <- head(videos %>%
                       group_by(video_id,title) %>%
                       dplyr::summarise(Total =
                                          round (100* max (comment_count, na.rm = T)/ max (views, na.rm = T))) %>%
                       arrange(desc(Total)),15)
datatable(max_comment)

ggplot(max_comment,aes(video_id,Total))+
  geom_bar(stat = "identity",fill="blue")+
  ggtitle("Top 15 videos with maximum percentage (%) of Comments on basis of views on video")

#j) Top trending YouTube channels in all countries
trending <- head(videos %>%
                      group_by(channel_title,video_id) %>%
                      dplyr::summarise(Total = sum(views))%>%
                      arrange(desc(Total)),10)
datatable(trending)

ggplot(trending,aes(video_id,Total))+
  geom_bar(stat = "identity",fill="blue")+
  ggtitle(" Top trending YouTube channels in all countries")

#k) Top trending YouTube channels in India.
trending <- head(INvideos %>%
                   group_by(channel_title,video_id) %>%
                   dplyr::summarise(Total = sum(views))%>%
                   arrange(desc(Total)),10)
datatable(trending)

ggplot(trending,aes(video_id,Total))+
  geom_bar(stat = "identity",fill="blue")+
  ggtitle(" Top trending YouTube channels in India")

# l)Create a YouTube Title WordCloud.

# Corpus
library(wordcloud)
library(tm)

corpus = Corpus(VectorSource(list(sample(US_Video$title, size = 3000))))

corpus = tm_map(corpus, removePunctuation)
corpus = tm_map(corpus, content_transformer(tolower))
corpus = tm_map(corpus, removeNumbers)
corpus = tm_map(corpus, stripWhitespace)
corpus = tm_map(corpus, removeWords, stopwords('english'))

dtm_us = TermDocumentMatrix(corpus)
matrix <- as.matrix(dtm_us)

words <- sort(rowSums(matrix), decreasing = TRUE)
df <- data.frame(word = names(words), freq = words)

head(df)

wordcloud(words = df$word, freq = df$freq, min.freq = 5, 
          random.order = FALSE, colors = brewer.pal(6, "Dark2"))

#wordcloud(words = videos$title,
#          max.words = 200,
#          random.order = FALSE,
#          rot.per = 0.28,
#          colors = brewer.pal(8,"Dark2"))


#m) Show Top Category ID

#top_category <- head(videos %>%
#                       #group_by(category_id) %>%
#                       dplyr::count(category_id) %>%
#                       arrange(desc(n)),1)
#datatable(top_category)

top_category_id <- head(videos %>%
                          group_by(category_id) %>%
                          dplyr::summarise(Total = n()) %>%
                          arrange(desc(Total)), 10)

datatable(top_category_id)

ggplot(top_category_id, aes(category_id, Total)) +
  geom_bar( stat = "identity", fill = "blue") + 
  ylab("Count of Category ID") +
  ggtitle("Top Category ID")


# n) How much time passes between published and trending?

pub_trending <- head(videos %>%
                             group_by(video_id) %>%
                             dplyr::summarise(rslt = difftime(publish_time, trending_date, units = "days")) %>%
                             arrange(rslt), 20)

head(pub_trending, 10)

datatable(pub_trending)

ggplot(pub_trending, aes(video_id, rslt)) +
  geom_bar( stat = "identity", fill = "red") + 
  ylab("Difference") +
  scale_y_continuous() +
  ggtitle("Time Passes Between Published and Trending Date")

#o) Show the relationship plots between Views Vs. Likes on Youtube.
plot(x=videos$views, y=videos$likes,
     pch = 16,col="blue",
     xlab = "views", #for x-label
     ylab = "likes", #for y-label
     main="Scatter plot")

#p) Top Countries In total number of Views in absolute numbers
Countries <- c("CA","FR","GB","IN","US")
Views <- c(sum(CA_Video$views), sum(FR_Video$views), sum(GB_Video$views), 
           sum(IN_Video$views), sum(US_Video$views))
Likes <- c(sum(CA_Video$likes), sum(FR_Video$likes), sum(GB_Video$likes), 
           sum(IN_Video$likes), sum(US_Video$likes))
Dislikes <- c(sum(CA_Video$dislikes), sum(FR_Video$dislikes), sum(GB_Video$dislikes), 
              sum(IN_Video$dislikes), sum(US_Video$dislikes))
Comments <- c(sum(CA_Video$comment_count), sum(FR_Video$comment_count), sum(GB_Video$comment_count), 
              sum(IN_Video$comment_count), sum(US_Video$comment_count))

new_df <- data.frame(Countries, Views, Likes, Dislikes, Comments)
new_df
top_country <- head(new_df %>%
                      group_by(Countries) %>%
                      arrange(desc(Views)))
datatable(top_country)


ggplot(top_country, aes(x=Countries, y=Views)) +
  geom_bar( stat = "identity", fill = "red") +
  ggtitle("Top Countries In Total Number of Views")

# q) Top Countries In total number of Likes in absolute numbers

top_total_likes <- head(new_df %>%
                           group_by(Countries) %>%
                           arrange(desc(Likes)))
datatable(top_total_likes)

ggplot(top_total_likes, aes(x=Countries, y=Likes)) +
  geom_bar( stat = "identity", fill = "green") +
  ggtitle("Top Countries In Total Number of Likes")

# r) Top Countries In total number of Dislikes in absolute numbers

top_total_dislikes <- head(new_df %>%
                              group_by(Countries) %>%
                              arrange(desc(Dislikes)))
datatable(top_total_dislikes)

ggplot(top_total_dislikes, aes(x=Countries, y=Dislikes)) +
  geom_bar( stat = "identity", fill = "blue") +
  ggtitle("Top Countries In Total Number of Dislikes")

# s) Top Countries In total number of Comments in absolute numbers

top_total_comment <- head(new_df %>%
                              group_by(Countries) %>%
                              arrange(desc(Comments)))
datatable(top_total_comment)

ggplot(top_total_comment, aes(x=Countries, y=Comments)) +
  geom_bar( stat = "identity", fill = "blue") +
  ggtitle("Top Countries In Total Number of Comments")