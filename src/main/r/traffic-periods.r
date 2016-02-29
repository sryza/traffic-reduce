library("TSA")
file.path <- "~/blog/traffic/data/d07_text_station_hour_2013_01.txt"
data <- read.table(file.path, header=FALSE, sep=",")
first.station = data[data$V2 == data[1,2],]
periodogram(first.station$V10)


