# read a csv with lat, long, and class and create a map showing the
# geographic locations based on color

library(ggmap)
library(maps)
library(ggplot2)
library(cluster)

#read data
data = read.csv("/Users/saif/Documents/spring2016/FINAL_PROJECT_CMPT733/geo_mapping/boston_map", header=FALSE, sep=',')

#remove points not "directly" in the toronto region
#data = data[(data$V1 >= 79.7447 & data$V1 <= 79.1404 & data$V2 >= 43.9124),]
#data = data[(data$V2 <= 43.675),]
#data = data[(data$V1 <= -79.354513),]
#data = data[(data$V1 >= -79.434838),]

#get map of world
map <- get_map(location = "Boston MA", zoom=13, maptype="terrain")

#plot
p = ggmap(map)

p = p + geom_point(data=data, aes(x=V3, y=V2, colour= factor(V1, labels = c("Low Risk", "Medium Risk", "High Risk")))) + labs(color = "Risk Level") 
#+ scale_colour_manual(name="", values = c("1"="yellow", "0"="orange", "2"="red") 

#p = p + geom_point(data=data, aes(x=centers.V1, y=centers.V2, coluor=data$km.cluster, colour='Cluster Centers'))
#p = p + scale_colour_manual(name="",  values = c("Cluster Centers"="red", "Instagram Posts"="blue"))
#p = p + ggtitle("Instagram posts in Toronto")
#p = p + coord_map(xlim = c(-79.354513, -79.434838),ylim = c(43.6105,43.675))
#p = p+guides(colour=FALSE)
print(p)