#############################################################
#PREDICT HOW MANY BIKES FLOW ON THE STATION
#############################################################

library(randomForest)
library(miscTools)
library(XML)
library(RCurl)
library(stringr)
library(RMySQL)

#########################################################################################
#GET DATA FROM THE DATABASE

#connect to to data base
mydb<-dbConnect(MySQL(), user='', password='', dbname='', host='')

#get data from data base
bike.data.query<-dbSendQuery(mydb,"select f.id as 'Trip ID', f.duration as Duration, f.start_date as 'Start Date', st.station_name as 'Start Station', st.terminal_name as 'Start Terminal', f.end_date as 'End Date', st2.station_name as 'End Station', st2.terminal_name as 'End Terminal', e.entity_name as 'Bike #', e.entity_type as 'Subscriber Type', z.zip_code  as 'Zip Code' from fact_trips f 
join dim_stations st on f.start_station_id=st.id
join dim_stations st2 on f.end_station_id=st2.id
join dim_entity e on f.entity_id=e.id
join dim_entity_zip z on f.entity_id=z.entity_id")
bike.data<-fetch(bike.data.query, n=-1)

#create new variables
bike.data$Start.Date2<-as.POSIXct(as.character(bike.data$Start.Date), tz="America/Los_Angeles", format = "%m/%d/%Y %H:%M")
bike.data$End.Date2<-as.POSIXct(as.character(bike.data$End.Date), tz="America/Los_Angeles", format = "%m/%d/%Y %H:%M")
bike.data$hour.end<-as.numeric(format(bike.data$End.Date2, "%H"))
bike.data$hour.start<-as.numeric(format(bike.data$Start.Date2, "%H"))

#GET WEATHER DATA

#!!! ad script for db extraction
#for now I've just save the file locally to use in the prediction
weather_March_Aug<-read.csv('/Users/elenazadnepranets/Dropbox/UBER/weather_March_Aug.csv')

#########################################################################################


###############################################################################################
#PREDICT ARRIVALS
###############################################################################################

#GENERATE ARRIVAL AND DEPARTURE DATASET

#TERMINAL=70
bike.data.terminal.arrival<-bike.data[bike.data$End.Terminal==70 ,]
bike.data.terminal.arrival<-transform(bike.data.terminal.arrival, Zip.Code=as.character(Zip.Code))

#ADD VAIABLES 
bike.data.terminal.arrival$End.min<-as.numeric(format(as.POSIXct(bike.data.terminal.arrival$time.end,tz="America/Los_Angeles", '%H:%M'), '%M'))

#Add 10 minute segment
bike.data.terminal.arrival$End.min.segment<-0
time.segment<-seq(10, 60, by=10)
for (i in 1:length(time.segment)){
  if (i==1){
    bike.data.terminal.arrival$End.min.segment[bike.data.terminal.arrival$End.min<time.segment[i] ]<-i
  }else{
    bike.data.terminal.arrival$End.min.segment[bike.data.terminal.arrival$End.min>=time.segment[i-1] & bike.data.terminal.arrival$End.min<time.segment[i] ]<-i
  }
}


arrivals.freq<-aggregate(bike.data.terminal.arrival$Trip.ID, list(as.Date(bike.data.terminal.arrival$End.Date2), bike.data.terminal.arrival$hour.end, bike.data.terminal.arrival$End.min.segment), length)
colnames(arrivals.freq)<-c('Date', 'Hour', 'Minute.segement', 'Frequency.arrivals')
sortnames<-c('Date', 'Hour', 'Minute.segement')
arrivals.freq<-arrivals.freq[do.call(order, arrivals.freq[sortnames]),]
arrivals.freq$Hour<-as.numeric(arrivals.freq$Hour)
#head(arrivals.freq, n=20)

#FEATURE GENERATION

#weekday
arrivals.freq$weekday<-factor(weekdays(arrivals.freq$Date))

#past days weather
weather_March_Aug.t_1<-data.frame('Date'=as.Date(weather_March_Aug$Date)+1,'Hour'=weather_March_Aug$Hour,   'Temp.t_1'=weather_March_Aug$Temp, 'Humidity.t_1'=weather_March_Aug$Humidity, 'Pressure.t_1'=weather_March_Aug$Pressure)
weather_March_Aug.t_1$Hour<-as.numeric(weather_March_Aug.t_1$Hour)

weather_March_Aug.t_2<-data.frame('Date'=as.Date(weather_March_Aug$Date)+2,'Hour'=weather_March_Aug$Hour,   'Temp.t_2'=weather_March_Aug$Temp, 'Humidity.t_2'=weather_March_Aug$Humidity, 'Pressure.t_2'=weather_March_Aug$Pressure)
weather_March_Aug.t_2$Hour<-as.numeric(weather_March_Aug.t_2$Hour)

weather_March_Aug.t_3<-data.frame('Date'=as.Date(weather_March_Aug$Date)+3,'Hour'=weather_March_Aug$Hour,   'Temp.t_3'=weather_March_Aug$Temp, 'Humidity.t_3'=weather_March_Aug$Humidity, 'Pressure.t_3'=weather_March_Aug$Pressure)
weather_March_Aug.t_3$Hour<-as.numeric(weather_March_Aug.t_3$Hour)

weather_March_Aug.t_4<-data.frame('Date'=as.Date(weather_March_Aug$Date)+4,'Hour'=weather_March_Aug$Hour,   'Temp.t_4'=weather_March_Aug$Temp, 'Humidity.t_4'=weather_March_Aug$Humidity, 'Pressure.t_4'=weather_March_Aug$Pressure)
weather_March_Aug.t_4$Hour<-as.numeric(weather_March_Aug.t_4$Hour)

weather_March_Aug.t_5<-data.frame('Date'=as.Date(weather_March_Aug$Date)+5,'Hour'=weather_March_Aug$Hour,   'Temp.t_5'=weather_March_Aug$Temp, 'Humidity.t_5'=weather_March_Aug$Humidity, 'Pressure.t_5'=weather_March_Aug$Pressure)
weather_March_Aug.t_5$Hour<-as.numeric(weather_March_Aug.t_5$Hour)

weather_March_Aug.t_6<-data.frame('Date'=as.Date(weather_March_Aug$Date)+6,'Hour'=weather_March_Aug$Hour,   'Temp.t_6'=weather_March_Aug$Temp, 'Humidity.t_6'=weather_March_Aug$Humidity, 'Pressure.t_6'=weather_March_Aug$Pressure)
weather_March_Aug.t_6$Hour<-as.numeric(weather_March_Aug.t_6$Hour)

weather_March_Aug.t_7<-data.frame('Date'=as.Date(weather_March_Aug$Date)+7,'Hour'=weather_March_Aug$Hour,   'Temp.t_7'=weather_March_Aug$Temp, 'Humidity.t_7'=weather_March_Aug$Humidity, 'Pressure.t_7'=weather_March_Aug$Pressure)
weather_March_Aug.t_7$Hour<-as.numeric(weather_March_Aug.t_7$Hour)

weather_March_Aug.t_8<-data.frame('Date'=as.Date(weather_March_Aug$Date)+8,'Hour'=weather_March_Aug$Hour,   'Temp.t_8'=weather_March_Aug$Temp, 'Humidity.t_8'=weather_March_Aug$Humidity, 'Pressure.t_8'=weather_March_Aug$Pressure)
weather_March_Aug.t_8$Hour<-as.numeric(weather_March_Aug.t_8$Hour)

weather_March_Aug.t_9<-data.frame('Date'=as.Date(weather_March_Aug$Date)+9,'Hour'=weather_March_Aug$Hour,   'Temp.t_9'=weather_March_Aug$Temp, 'Humidity.t_9'=weather_March_Aug$Humidity, 'Pressure.t_9'=weather_March_Aug$Pressure)
weather_March_Aug.t_9$Hour<-as.numeric(weather_March_Aug.t_9$Hour)

weather_March_Aug.t_10<-data.frame('Date'=as.Date(weather_March_Aug$Date)+10,'Hour'=weather_March_Aug$Hour,   'Temp.t_10'=weather_March_Aug$Temp, 'Humidity.t_10'=weather_March_Aug$Humidity, 'Pressure.t_10'=weather_March_Aug$Pressure)
weather_March_Aug.t_10$Hour<-as.numeric(weather_March_Aug.t_10$Hour)

#past days arrivals
arrivals.freq.t_1<-data.frame('Date'=arrivals.freq$Date+1,'Hour'=arrivals.freq$Hour,'Minute.segement'=arrivals.freq$Minute.segement,   'Frequency.arrivals.t_1'=arrivals.freq$Frequency.arrivals)
arrivals.freq.t_1$Hour<-as.numeric(arrivals.freq.t_1$Hour)
arrivals.freq.t_2<-data.frame('Date'=arrivals.freq$Date+2,'Hour'=arrivals.freq$Hour,'Minute.segement'=arrivals.freq$Minute.segement,    'Frequency.arrivals.t_2'=arrivals.freq$Frequency.arrivals)
arrivals.freq.t_2$Hour<-as.numeric(arrivals.freq.t_2$Hour)
arrivals.freq.t_3<-data.frame('Date'=arrivals.freq$Date+3,'Hour'=arrivals.freq$Hour,'Minute.segement'=arrivals.freq$Minute.segement,  'Frequency.arrivals.t_3'=arrivals.freq$Frequency.arrivals)
arrivals.freq.t_3$Hour<-as.numeric(arrivals.freq.t_3$Hour)
arrivals.freq.t_4<-data.frame('Date'=arrivals.freq$Date+4,'Hour'=arrivals.freq$Hour,'Minute.segement'=arrivals.freq$Minute.segement,    'Frequency.arrivals.t_4'=arrivals.freq$Frequency.arrivals)
arrivals.freq.t_4$Hour<-as.numeric(arrivals.freq.t_4$Hour)
arrivals.freq.t_5<-data.frame('Date'=arrivals.freq$Date+5,'Hour'=arrivals.freq$Hour,'Minute.segement'=arrivals.freq$Minute.segement,   'Frequency.arrivals.t_5'=arrivals.freq$Frequency.arrivals)
arrivals.freq.t_5$Hour<-as.numeric(arrivals.freq.t_5$Hour)
arrivals.freq.t_6<-data.frame('Date'=arrivals.freq$Date+6,'Hour'=arrivals.freq$Hour,'Minute.segement'=arrivals.freq$Minute.segement,   'Frequency.arrivals.t_6'=arrivals.freq$Frequency.arrivals)
arrivals.freq.t_6$Hour<-as.numeric(arrivals.freq.t_6$Hour)
arrivals.freq.t_7<-data.frame('Date'=arrivals.freq$Date+7,'Hour'=arrivals.freq$Hour,'Minute.segement'=arrivals.freq$Minute.segement,   'Frequency.arrivals.t_7'=arrivals.freq$Frequency.arrivals)
arrivals.freq.t_7$Hour<-as.numeric(arrivals.freq.t_7$Hour)
arrivals.freq.t_8<-data.frame('Date'=arrivals.freq$Date+8,'Hour'=arrivals.freq$Hour,'Minute.segement'=arrivals.freq$Minute.segement,   'Frequency.arrivals.t_8'=arrivals.freq$Frequency.arrivals)
arrivals.freq.t_8$Hour<-as.numeric(arrivals.freq.t_8$Hour)
arrivals.freq.t_9<-data.frame('Date'=arrivals.freq$Date+9,'Hour'=arrivals.freq$Hour,'Minute.segement'=arrivals.freq$Minute.segement,   'Frequency.arrivals.t_9'=arrivals.freq$Frequency.arrivals)
arrivals.freq.t_9$Hour<-as.numeric(arrivals.freq.t_9$Hour)
arrivals.freq.t_10<-data.frame('Date'=arrivals.freq$Date+10,'Hour'=arrivals.freq$Hour,'Minute.segement'=arrivals.freq$Minute.segement,  'Frequency.arrivals.t_10'=arrivals.freq$Frequency.arrivals)
arrivals.freq.t_10$Hour<-as.numeric(arrivals.freq.t_10$Hour)

#zip code
all.zip.codes.arrival<-sort(table(bike.data.terminal.arrival$Zip.Code),decreasing = T)
top40.zip.codes.arrival<-names(all.zip.codes.arrival)[1:40]
zip.codes.arrival.aggreg<-aggregate(bike.data.terminal.arrival$Zip.Code, list(as.Date(bike.data.terminal.arrival$End.Date2), bike.data.terminal.arrival$hour.end, bike.data.terminal.arrival$End.min.segment), paste)
colnames(zip.codes.arrival.aggreg)<-c('Date', 'Hour', 'Minute.segement', 'All zip.codes')
zip.codes.arrival.aggreg<-zip.codes.arrival.aggreg[order(zip.codes.arrival.aggreg$Date),]
#create zip variables
m<-matrix(0, nrow=length(top40.zip.codes.arrival), ncol=nrow(zip.codes.arrival.aggreg))
top40.zip.codes.arrival.df<-as.data.frame(t(data.frame(top40.zip.codes.arrival, m)))
names<-t(top40.zip.codes.arrival.df[1,])
colnames(top40.zip.codes.arrival.df)<-names
top40.zip.codes.arrival.df<-top40.zip.codes.arrival.df[-1,]
row.names(top40.zip.codes.arrival.df)<-NULL
#aggregate and code
top40.zip.codes.arrival.df<-data.frame(zip.codes.arrival.aggreg, top40.zip.codes.arrival.df)
for (k in 5:ncol(top40.zip.codes.arrival.df)){
  top40.zip.codes.arrival.df[,k]<-as.numeric(grepl(gsub('X', '',names(top40.zip.codes.arrival.df)[k]), top40.zip.codes.arrival.df$All.zip.codes))
}

top40.zip.codes.arrival.df.t_1<-data.frame('Date'=top40.zip.codes.arrival.df$Date+1,subset(top40.zip.codes.arrival.df, select = -c(Date, All.zip.codes)))
names<-colnames(top40.zip.codes.arrival.df.t_1)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_1'), simplify=T)
colnames(top40.zip.codes.arrival.df.t_1)<-c("Date", "Hour",            "Minute.segement", names_part2)

top40.zip.codes.arrival.df.t_2<-data.frame('Date'=top40.zip.codes.arrival.df$Date+2,subset(top40.zip.codes.arrival.df, select = -c(Date, All.zip.codes)))
names<-colnames(top40.zip.codes.arrival.df.t_2)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_2'), simplify=T)
colnames(top40.zip.codes.arrival.df.t_2)<-c("Date", "Hour",            "Minute.segement", names_part2)


top40.zip.codes.arrival.df.t_3<-data.frame('Date'=top40.zip.codes.arrival.df$Date+3,subset(top40.zip.codes.arrival.df, select = -c(Date, All.zip.codes)))
names<-colnames(top40.zip.codes.arrival.df.t_3)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_3'), simplify=T)
colnames(top40.zip.codes.arrival.df.t_3)<-c("Date", "Hour",            "Minute.segement", names_part2)

top40.zip.codes.arrival.df.t_4<-data.frame('Date'=top40.zip.codes.arrival.df$Date+4,subset(top40.zip.codes.arrival.df, select = -c(Date, All.zip.codes)))
names<-colnames(top40.zip.codes.arrival.df.t_4)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_4'), simplify=T)
colnames(top40.zip.codes.arrival.df.t_4)<-c("Date", "Hour",            "Minute.segement", names_part2)

top40.zip.codes.arrival.df.t_5<-data.frame('Date'=top40.zip.codes.arrival.df$Date+5,subset(top40.zip.codes.arrival.df, select = -c(Date, All.zip.codes)))
names<-colnames(top40.zip.codes.arrival.df.t_5)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_5'), simplify=T)
colnames(top40.zip.codes.arrival.df.t_5)<-c("Date", "Hour",            "Minute.segement", names_part2)

top40.zip.codes.arrival.df.t_6<-data.frame('Date'=top40.zip.codes.arrival.df$Date+6,subset(top40.zip.codes.arrival.df, select = -c(Date, All.zip.codes)))
names<-colnames(top40.zip.codes.arrival.df.t_6)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_6'), simplify=T)
colnames(top40.zip.codes.arrival.df.t_6)<-c("Date", "Hour",            "Minute.segement", names_part2)

top40.zip.codes.arrival.df.t_7<-data.frame('Date'=top40.zip.codes.arrival.df$Date+7,subset(top40.zip.codes.arrival.df, select = -c(Date, All.zip.codes)))
names<-colnames(top40.zip.codes.arrival.df.t_7)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_7'), simplify=T)
colnames(top40.zip.codes.arrival.df.t_7)<-c("Date", "Hour",            "Minute.segement", names_part2)

top40.zip.codes.arrival.df.t_8<-data.frame('Date'=top40.zip.codes.arrival.df$Date+8,subset(top40.zip.codes.arrival.df, select = -c(Date, All.zip.codes)))
names<-colnames(top40.zip.codes.arrival.df.t_8)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_8'), simplify=T)
colnames(top40.zip.codes.arrival.df.t_8)<-c("Date", "Hour",            "Minute.segement", names_part2)

top40.zip.codes.arrival.df.t_9<-data.frame('Date'=top40.zip.codes.arrival.df$Date+9,subset(top40.zip.codes.arrival.df, select = -c(Date, All.zip.codes)))
names<-colnames(top40.zip.codes.arrival.df.t_9)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_9'), simplify=T)
colnames(top40.zip.codes.arrival.df.t_9)<-c("Date", "Hour",            "Minute.segement", names_part2)

top40.zip.codes.arrival.df.t_10<-data.frame('Date'=top40.zip.codes.arrival.df$Date+10,subset(top40.zip.codes.arrival.df, select = -c(Date, All.zip.codes)))
names<-colnames(top40.zip.codes.arrival.df.t_10)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_10'), simplify=T)
colnames(top40.zip.codes.arrival.df.t_10)<-c("Date", "Hour",            "Minute.segement", names_part2)


#subscriber type
subscriber.types<-unique(as.vector(bike.data.terminal.arrival$Subscriber.Type))
subscriber.type.aggreg<-aggregate(bike.data.terminal.arrival$Subscriber.Type, list(as.Date(bike.data.terminal.arrival$End.Date2), bike.data.terminal.arrival$hour.end, bike.data.terminal.arrival$End.min.segment), paste)
colnames(subscriber.type.aggreg)<-c('Date', 'Hour', 'Minute.segement', 'Subscriber.type')
subscriber.type.aggreg<-subscriber.type.aggreg[order(subscriber.type.aggreg$Date),]
m2<-matrix(0, nrow=length(subscriber.types), ncol=nrow(subscriber.type.aggreg))
subscriber.type.df<-as.data.frame(t(data.frame(subscriber.types, m2)))
names<-t(subscriber.type.df[1,])
colnames(subscriber.type.df)<-names
subscriber.type.df<-subscriber.type.df[-1,]
row.names(subscriber.type.df)<-NULL
#aggregate and code
subscriber.type.df<-data.frame(subscriber.type.aggreg, subscriber.type.df)
for (k in 5:ncol(subscriber.type.df)){
  subscriber.type.df[,k]<-as.numeric(grepl(names(subscriber.type.df)[k], subscriber.type.df$Subscriber.type))
}

subscriber.type.df.t_1<-data.frame('Date'=subscriber.type.df$Date+1,subset(subscriber.type.df, select = -c(Date, Subscriber.type)))
names<-colnames(subscriber.type.df.t_1)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_1'), simplify=T)
colnames(subscriber.type.df.t_1)<-c("Date", "Hour",            "Minute.segement", names_part2)

subscriber.type.df.t_2<-data.frame('Date'=subscriber.type.df$Date+2,subset(subscriber.type.df, select = -c(Date, Subscriber.type)))
names<-colnames(subscriber.type.df.t_2)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_2'), simplify=T)
colnames(subscriber.type.df.t_2)<-c("Date", "Hour",            "Minute.segement", names_part2)


subscriber.type.df.t_3<-data.frame('Date'=subscriber.type.df$Date+3,subset(subscriber.type.df, select = -c(Date, Subscriber.type)))
names<-colnames(subscriber.type.df.t_3)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_3'), simplify=T)
colnames(subscriber.type.df.t_3)<-c("Date", "Hour",            "Minute.segement", names_part2)

subscriber.type.df.t_4<-data.frame('Date'=subscriber.type.df$Date+4,subset(subscriber.type.df, select = -c(Date, Subscriber.type)))
names<-colnames(subscriber.type.df.t_4)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_4'), simplify=T)
colnames(subscriber.type.df.t_4)<-c("Date", "Hour",            "Minute.segement", names_part2)

subscriber.type.df.t_5<-data.frame('Date'=subscriber.type.df$Date+5,subset(subscriber.type.df, select = -c(Date, Subscriber.type)))
names<-colnames(subscriber.type.df.t_5)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_5'), simplify=T)
colnames(subscriber.type.df.t_5)<-c("Date", "Hour",            "Minute.segement", names_part2)

subscriber.type.df.t_6<-data.frame('Date'=subscriber.type.df$Date+6,subset(subscriber.type.df, select = -c(Date, Subscriber.type)))
names<-colnames(subscriber.type.df.t_6)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_6'), simplify=T)
colnames(subscriber.type.df.t_6)<-c("Date", "Hour",            "Minute.segement", names_part2)

subscriber.type.df.t_7<-data.frame('Date'=subscriber.type.df$Date+7,subset(subscriber.type.df, select = -c(Date, Subscriber.type)))
names<-colnames(subscriber.type.df.t_7)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_7'), simplify=T)
colnames(subscriber.type.df.t_7)<-c("Date", "Hour",            "Minute.segement", names_part2)

subscriber.type.df.t_8<-data.frame('Date'=subscriber.type.df$Date+8,subset(subscriber.type.df, select = -c(Date, Subscriber.type)))
names<-colnames(subscriber.type.df.t_8)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_8'), simplify=T)
colnames(subscriber.type.df.t_8)<-c("Date", "Hour",            "Minute.segement", names_part2)

subscriber.type.df.t_9<-data.frame('Date'=subscriber.type.df$Date+9,subset(subscriber.type.df, select = -c(Date, Subscriber.type)))
names<-colnames(subscriber.type.df.t_9)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_9'), simplify=T)
colnames(subscriber.type.df.t_9)<-c("Date", "Hour",            "Minute.segement", names_part2)

subscriber.type.df.t_10<-data.frame('Date'=subscriber.type.df$Date+10,subset(subscriber.type.df, select = -c(Date, Subscriber.type)))
names<-colnames(subscriber.type.df.t_10)
names_part2<-names[!(names %in% c("Date", "Hour",            "Minute.segement"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_10'), simplify=T)
colnames(subscriber.type.df.t_10)<-c("Date", "Hour",            "Minute.segement", names_part2)


################  
#MERGE
arrivals.freq.weather<-merge(arrivals.freq, weather_March_Aug, by=c('Date', 'Hour'), all.x=T)
arrivals.freq.weather<-merge(arrivals.freq.weather, weather_March_Aug.t_1, by=c('Date', 'Hour'), all.x=T)
arrivals.freq.weather<-merge(arrivals.freq.weather, weather_March_Aug.t_2, by=c('Date', 'Hour'), all.x=T)
arrivals.freq.weather<-merge(arrivals.freq.weather, weather_March_Aug.t_3, by=c('Date', 'Hour'), all.x=T)
arrivals.freq.weather<-merge(arrivals.freq.weather, weather_March_Aug.t_4, by=c('Date', 'Hour'), all.x=T)
arrivals.freq.weather<-merge(arrivals.freq.weather, weather_March_Aug.t_5, by=c('Date', 'Hour'), all.x=T)
arrivals.freq.weather<-merge(arrivals.freq.weather, weather_March_Aug.t_6, by=c('Date', 'Hour'), all.x=T)
arrivals.freq.weather<-merge(arrivals.freq.weather, weather_March_Aug.t_7, by=c('Date', 'Hour'), all.x=T)
arrivals.freq.weather<-merge(arrivals.freq.weather, weather_March_Aug.t_8, by=c('Date', 'Hour'), all.x=T)
arrivals.freq.weather<-merge(arrivals.freq.weather, weather_March_Aug.t_9, by=c('Date', 'Hour'), all.x=T)
arrivals.freq.weather<-merge(arrivals.freq.weather, weather_March_Aug.t_10, by=c('Date', 'Hour'), all.x=T)


arrivals.freq.weather<-merge(arrivals.freq.weather,arrivals.freq.t_1, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather<-arrivals.freq.weather[arrivals.freq.weather$Date!=min(arrivals.freq.weather$Date),]
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0


arrivals.freq.weather<-merge(arrivals.freq.weather,arrivals.freq.t_2, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather<-arrivals.freq.weather[arrivals.freq.weather$Date!=min(arrivals.freq.weather$Date),]
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,arrivals.freq.t_3, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather<-arrivals.freq.weather[arrivals.freq.weather$Date!=min(arrivals.freq.weather$Date),]
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,arrivals.freq.t_4, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather<-arrivals.freq.weather[arrivals.freq.weather$Date!=min(arrivals.freq.weather$Date),]
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,arrivals.freq.t_5, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather<-arrivals.freq.weather[arrivals.freq.weather$Date!=min(arrivals.freq.weather$Date),]
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,arrivals.freq.t_6, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather<-arrivals.freq.weather[arrivals.freq.weather$Date!=min(arrivals.freq.weather$Date),]
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,arrivals.freq.t_7, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather<-arrivals.freq.weather[arrivals.freq.weather$Date!=min(arrivals.freq.weather$Date),]
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,arrivals.freq.t_8, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather<-arrivals.freq.weather[arrivals.freq.weather$Date!=min(arrivals.freq.weather$Date),]
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,arrivals.freq.t_9, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather<-arrivals.freq.weather[arrivals.freq.weather$Date!=min(arrivals.freq.weather$Date),]
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,arrivals.freq.t_10, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather<-arrivals.freq.weather[arrivals.freq.weather$Date!=min(arrivals.freq.weather$Date),]
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

#zip codes
arrivals.freq.weather<-merge(arrivals.freq.weather,top40.zip.codes.arrival.df.t_1, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,top40.zip.codes.arrival.df.t_2, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,top40.zip.codes.arrival.df.t_3, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,top40.zip.codes.arrival.df.t_4, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,top40.zip.codes.arrival.df.t_5, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,top40.zip.codes.arrival.df.t_6, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,top40.zip.codes.arrival.df.t_7, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,top40.zip.codes.arrival.df.t_8, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,top40.zip.codes.arrival.df.t_9, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,top40.zip.codes.arrival.df.t_10, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

#subscriber type
arrivals.freq.weather<-merge(arrivals.freq.weather,subscriber.type.df.t_1, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,subscriber.type.df.t_2, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,subscriber.type.df.t_3, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,subscriber.type.df.t_4, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,subscriber.type.df.t_5, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,subscriber.type.df.t_6, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,subscriber.type.df.t_7, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,subscriber.type.df.t_8, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,subscriber.type.df.t_9, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

arrivals.freq.weather<-merge(arrivals.freq.weather,subscriber.type.df.t_10, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
arrivals.freq.weather[is.na(arrivals.freq.weather)]<-0

#test<-arrivals.freq.weather[is.na(arrivals.freq.weather$Subscriber),]

#ONLY WEEKDAYS
arrivals.freq.weather.weekdays<-arrivals.freq.weather[!(arrivals.freq.weather$weekday %in% c('Saturday', 'Sunday')),]


#REFORMAT VARIABLES
#arrivals.freq.weather.weekdays$Temp.<-as.numeric(arrivals.freq.weather.weekdays$Temp.)
#arrivals.freq.weather.weekdays$Dew.Point<-as.numeric(arrivals.freq.weather.weekdays$Dew.Point)
#arrivals.freq.weather.weekdays$Humidity<-as.numeric(arrivals.freq.weather.weekdays$Humidity)
#arrivals.freq.weather.weekdays$Pressure<-as.numeric(arrivals.freq.weather.weekdays$Pressure)
#arrivals.freq.weather.weekdays$Visibility<-as.numeric(arrivals.freq.weather.weekdays$Visibility)
#arrivals.freq.weather.weekdays$Wind.Speed<-as.numeric(arrivals.freq.weather.weekdays$Wind.Speed)
#arrivals.freq.weather.weekdays$Wind.Dir<-factor(arrivals.freq.weather.weekdays$Wind.Dir)
#arrivals.freq.weather.weekdays$Conditions<-factor(arrivals.freq.weather.weekdays$Conditions)

names(arrivals.freq.weather.weekdays)
for (t in 46:105){
  arrivals.freq.weather.weekdays[,t]<-factor(arrivals.freq.weather.weekdays[,t])
}

#TEST & TRAIN
####################################################
#Use only at the final step
idx2<-runif(nrow(arrivals.freq.weather.weekdays))>0.75
train.arrival <- arrivals.freq.weather.weekdays[idx2==FALSE,]
test.arrival<-arrivals.freq.weather.weekdays[idx2==TRUE,]
####################################################
train.nmr<-round(0.6*nrow(arrivals.freq.weather.weekdays),0)
validate.nmr<-round(0.8*nrow(arrivals.freq.weather.weekdays),0)
train.arrival<-arrivals.freq.weather.weekdays[1:train.nmr, ]
validate.arrival<-arrivals.freq.weather.weekdays[(train.nmr+1):validate.nmr, ]
test.arrival<-arrivals.freq.weather.weekdays[(validate.nmr+1):nrow(arrivals.freq.weather.weekdays), ]

variables<-paste(colnames(train.arrival), collapse='+')

rf <- randomForest(Frequency.arrivals ~  Hour + Minute.segement+weekday+
                     Temp.t_1+Humidity.t_1+Pressure.t_1+Temp.t_2+Humidity.t_2+Pressure.t_2+Temp.t_3+Humidity.t_3+Pressure.t_3+Temp.t_4+Humidity.t_4+Pressure.t_4+Temp.t_5+Humidity.t_5+Pressure.t_5+Temp.t_6+Humidity.t_6+Pressure.t_6+Temp.t_7+Humidity.t_7+Pressure.t_7+Temp.t_8+Humidity.t_8+Pressure.t_8+Temp.t_9+Humidity.t_9+Pressure.t_9+Temp.t_10+Humidity.t_10+Pressure.t_10+Frequency.arrivals.t_1+Frequency.arrivals.t_2+Frequency.arrivals.t_3+Frequency.arrivals.t_4+Frequency.arrivals.t_5+Frequency.arrivals.t_6+Frequency.arrivals.t_7+Frequency.arrivals.t_8+Frequency.arrivals.t_9+Frequency.arrivals.t_10,
                   data=train.arrival, importance=TRUE, na.action=na.omit,proximity=TRUE)

#ACCURACY
print(rf)
round(importance(rf), 2)
plot(rf)
prediction<-predict(rf,validate.arrival)
validate.arrival$prediction<-prediction
validate.arrival[is.na(validate.arrival$prediction),]
validate.arrival.no.na<-validate.arrival[!is.na(validate.arrival$prediction),]
r2 <- rSquared(validate.arrival.no.na$Frequency.arrivals, validate.arrival.no.na$Frequency.arrivals - validate.arrival.no.na$prediction)
mse<-mean((validate.arrival$Frequency.arrivals - prediction)^2, na.rm=T)




###############################################################################################
#PREDICT DEPARTURES
###############################################################################################


#TERMINAL=70
bike.data.terminal.departure.departure<-bike.data[bike.data$Start.Terminal==70 ,]
bike.data.terminal.departure.departure<-transform(bike.data.terminal.departure.departure, Zip.Code=as.character(Zip.Code))

#ADD VAIABLES 
bike.data.terminal.departure.departure$Start.min<-as.numeric(format(as.POSIXct(bike.data.terminal.departure.departure$time.start,tz="America/Los_Angeles", '%H:%M'), '%M'))

#Add minute segment
bike.data.terminal.departure.departure$Start.min.segment<-0
time.segment<-seq(10, 60, by=10)
for (i in 1:length(time.segment)){
  if (i==1){
    bike.data.terminal.departure.departure$Start.min.segment[bike.data.terminal.departure.departure$Start.min<time.segment[i] ]<-i
  }else{
    bike.data.terminal.departure.departure$Start.min.segment[bike.data.terminal.departure.departure$Start.min>=time.segment[i-1] & bike.data.terminal.departure.departure$Start.min<time.segment[i] ]<-i
  }
}



departures.freq<-aggregate(bike.data.terminal.departure.departure$Trip.ID, list(as.Date(bike.data.terminal.departure.departure$Start.Date2), bike.data.terminal.departure.departure$hour.start, bike.data.terminal.departure.departure$Start.min.segment), length)
colnames(departures.freq)<-c('Date', 'Hour', 'Minute.segement', 'Frequency.departures')
sortnames<-c('Date', 'Hour', 'Minute.segement')
departures.freq<-departures.freq[do.call(order, departures.freq[sortnames]),]
departures.freq$Hour<-as.numeric(departures.freq$Hour)

#FEATURE GENERATION
#weekday
departures.freq$weekday<-factor(weekdays(departures.freq$Date))

#past days departures
departures.freq.t_1<-data.frame('Date'=departures.freq$Date+1,'Hour'=departures.freq$Hour,'Minute.segement'=departures.freq$Minute.segement,   'Frequency.departures.t_1'=departures.freq$Frequency.departures)
departures.freq.t_1$Hour<-as.numeric(departures.freq.t_1$Hour)
departures.freq.t_2<-data.frame('Date'=departures.freq$Date+2,'Hour'=departures.freq$Hour,'Minute.segement'=departures.freq$Minute.segement,    'Frequency.departures.t_2'=departures.freq$Frequency.departures)
departures.freq.t_2$Hour<-as.numeric(departures.freq.t_2$Hour)
departures.freq.t_3<-data.frame('Date'=departures.freq$Date+3,'Hour'=departures.freq$Hour,'Minute.segement'=departures.freq$Minute.segement,  'Frequency.departures.t_3'=departures.freq$Frequency.departures)
departures.freq.t_3$Hour<-as.numeric(departures.freq.t_3$Hour)
departures.freq.t_4<-data.frame('Date'=departures.freq$Date+4,'Hour'=departures.freq$Hour,'Minute.segement'=departures.freq$Minute.segement,    'Frequency.departures.t_4'=departures.freq$Frequency.departures)
departures.freq.t_4$Hour<-as.numeric(departures.freq.t_4$Hour)
departures.freq.t_5<-data.frame('Date'=departures.freq$Date+5,'Hour'=departures.freq$Hour,'Minute.segement'=departures.freq$Minute.segement,   'Frequency.departures.t_5'=departures.freq$Frequency.departures)
departures.freq.t_5$Hour<-as.numeric(departures.freq.t_5$Hour)
departures.freq.t_6<-data.frame('Date'=departures.freq$Date+6,'Hour'=departures.freq$Hour,'Minute.segement'=departures.freq$Minute.segement,   'Frequency.departures.t_6'=departures.freq$Frequency.departures)
departures.freq.t_6$Hour<-as.numeric(departures.freq.t_6$Hour)
departures.freq.t_7<-data.frame('Date'=departures.freq$Date+7,'Hour'=departures.freq$Hour,'Minute.segement'=departures.freq$Minute.segement,   'Frequency.departures.t_7'=departures.freq$Frequency.departures)
departures.freq.t_7$Hour<-as.numeric(departures.freq.t_7$Hour)
departures.freq.t_8<-data.frame('Date'=departures.freq$Date+8,'Hour'=departures.freq$Hour,'Minute.segement'=departures.freq$Minute.segement,   'Frequency.departures.t_8'=departures.freq$Frequency.departures)
departures.freq.t_8$Hour<-as.numeric(departures.freq.t_8$Hour)
departures.freq.t_9<-data.frame('Date'=departures.freq$Date+9,'Hour'=departures.freq$Hour,'Minute.segement'=departures.freq$Minute.segement,   'Frequency.departures.t_9'=departures.freq$Frequency.departures)
departures.freq.t_9$Hour<-as.numeric(departures.freq.t_9$Hour)
departures.freq.t_10<-data.frame('Date'=departures.freq$Date+10,'Hour'=departures.freq$Hour,'Minute.segement'=departures.freq$Minute.segement,  'Frequency.departures.t_10'=departures.freq$Frequency.departures)
departures.freq.t_10$Hour<-as.numeric(departures.freq.t_10$Hour)

#zip code
all.zip.codes.departure<-sort(table(bike.data.terminal.departure$Zip.Code),decreasing = T)
top40.zip.codes.departure<-names(all.zip.codes.departure)[1:40]
zip.codes.departure.aggreg<-aggregate(bike.data.terminal.departure$Zip.Code, list(as.Date(bike.data.terminal.departure$Start.Date2), bike.data.terminal.departure$hour.start, bike.data.terminal.departure$Start.min.segment), paste)
colnames(zip.codes.departure.aggreg)<-c('Date', 'Hour', 'Minute.segement', 'All zip.codes')
zip.codes.departure.aggreg<-zip.codes.departure.aggreg[order(zip.codes.departure.aggreg$Date),]
#create zip variables
m<-matrix(0, nrow=length(top40.zip.codes.departure), ncol=nrow(zip.codes.departure.aggreg))
top40.zip.codes.departure.df<-as.data.frame(t(data.frame(top40.zip.codes.departure, m)))
names<-t(top40.zip.codes.departure.df[1,])
colnames(top40.zip.codes.departure.df)<-names
top40.zip.codes.departure.df<-top40.zip.codes.departure.df[-1,]
row.names(top40.zip.codes.departure.df)<-NULL
#aggregate and code
top40.zip.codes.departure.df<-data.frame(zip.codes.departure.aggreg, top40.zip.codes.departure.df)
for (k in 5:ncol(top40.zip.codes.departure.df)){
  top40.zip.codes.departure.df[,k]<-as.numeric(grepl(gsub('X', '',names(top40.zip.codes.departure.df)[k]), top40.zip.codes.departure.df$All.zip.codes))
}

#subscriber type
subscriber.types<-unique(as.vector(bike.data.terminal.departure$Subscriber.Type))
subscriber.type.depart.aggreg<-aggregate(bike.data.terminal.departure$Subscriber.Type, list(as.Date(bike.data.terminal.departure$Start.Date2), bike.data.terminal.departure$hour.start, bike.data.terminal.departure$Start.min.segment), paste)
colnames(subscriber.type.depart.aggreg)<-c('Date', 'Hour', 'Minute.segement', 'Subscriber.type')
subscriber.type.depart.aggreg<-subscriber.type.depart.aggreg[order(subscriber.type.depart.aggreg$Date),]
m2<-matrix(0, nrow=length(subscriber.types), ncol=nrow(subscriber.type.depart.aggreg))
subscriber.type.df<-as.data.frame(t(data.frame(subscriber.types, m2)))
names<-t(subscriber.type.df[1,])
colnames(subscriber.type.df)<-names
subscriber.type.df<-subscriber.type.df[-1,]
row.names(subscriber.type.df)<-NULL
#aggregate and code
subscriber.type.depart.df<-data.frame(subscriber.type.depart.aggreg, subscriber.type.df)
for (k in 5:ncol(subscriber.type.depart.df)){
  subscriber.type.depart.df[,k]<-as.numeric(grepl(names(subscriber.type.depart.df)[k], subscriber.type.depart.df$Subscriber.type))
}

#MERGE
departures.freq.weather<-merge(departures.freq, weather_March_Aug_new2, by=c('Date', 'Hour'), all.x=T)

departures.freq.weather<-merge(departures.freq.weather,departures.freq.t_1, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
departures.freq.weather<-departures.freq.weather[departures.freq.weather$Date!=min(departures.freq.weather$Date),]
departures.freq.weather[is.na(departures.freq.weather)]<-0


departures.freq.weather<-merge(departures.freq.weather,departures.freq.t_2, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
departures.freq.weather<-departures.freq.weather[departures.freq.weather$Date!=min(departures.freq.weather$Date),]
departures.freq.weather[is.na(departures.freq.weather)]<-0

departures.freq.weather<-merge(departures.freq.weather,departures.freq.t_3, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
departures.freq.weather<-departures.freq.weather[departures.freq.weather$Date!=min(departures.freq.weather$Date),]
departures.freq.weather[is.na(departures.freq.weather)]<-0

departures.freq.weather<-merge(departures.freq.weather,departures.freq.t_4, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
departures.freq.weather<-departures.freq.weather[departures.freq.weather$Date!=min(departures.freq.weather$Date),]
departures.freq.weather[is.na(departures.freq.weather)]<-0

departures.freq.weather<-merge(departures.freq.weather,departures.freq.t_5, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
departures.freq.weather<-departures.freq.weather[departures.freq.weather$Date!=min(departures.freq.weather$Date),]
departures.freq.weather[is.na(departures.freq.weather)]<-0

departures.freq.weather<-merge(departures.freq.weather,departures.freq.t_6, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
departures.freq.weather<-departures.freq.weather[departures.freq.weather$Date!=min(departures.freq.weather$Date),]
departures.freq.weather[is.na(departures.freq.weather)]<-0

departures.freq.weather<-merge(departures.freq.weather,departures.freq.t_7, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
departures.freq.weather<-departures.freq.weather[departures.freq.weather$Date!=min(departures.freq.weather$Date),]
departures.freq.weather[is.na(departures.freq.weather)]<-0

departures.freq.weather<-merge(departures.freq.weather,departures.freq.t_8, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
departures.freq.weather<-departures.freq.weather[departures.freq.weather$Date!=min(departures.freq.weather$Date),]
departures.freq.weather[is.na(departures.freq.weather)]<-0

departures.freq.weather<-merge(departures.freq.weather,departures.freq.t_9, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
departures.freq.weather<-departures.freq.weather[departures.freq.weather$Date!=min(departures.freq.weather$Date),]
departures.freq.weather[is.na(departures.freq.weather)]<-0

departures.freq.weather<-merge(departures.freq.weather,departures.freq.t_10, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)
departures.freq.weather<-departures.freq.weather[departures.freq.weather$Date!=min(departures.freq.weather$Date),]
departures.freq.weather[is.na(departures.freq.weather)]<-0

departures.freq.weather<-merge(departures.freq.weather,top40.zip.codes.departure.df, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)

departures.freq.weather<-merge(departures.freq.weather,subscriber.type.depart.df, by=c('Date', 'Hour', 'Minute.segement'), all.x=T)

#test<-departures.freq.weather[is.na(departures.freq.weather$Subscriber),]
#ONLY WEEKDAYS
departures.freq.weather.weekdays<-departures.freq.weather[!(departures.freq.weather$weekday %in% c('Saturday', 'Sunday')),]


#REFORMAT VARIABLES
departures.freq.weather.weekdays$Temp.<-as.numeric(departures.freq.weather.weekdays$Temp.)
departures.freq.weather.weekdays$Dew.Point<-as.numeric(departures.freq.weather.weekdays$Dew.Point)
departures.freq.weather.weekdays$Humidity<-as.numeric(departures.freq.weather.weekdays$Humidity)
departures.freq.weather.weekdays$Pressure<-as.numeric(departures.freq.weather.weekdays$Pressure)
departures.freq.weather.weekdays$Visibility<-as.numeric(departures.freq.weather.weekdays$Visibility)
departures.freq.weather.weekdays$Wind.Speed<-as.numeric(departures.freq.weather.weekdays$Wind.Speed)
departures.freq.weather.weekdays$Wind.Dir<-factor(departures.freq.weather.weekdays$Wind.Dir)
departures.freq.weather.weekdays$Conditions<-factor(departures.freq.weather.weekdays$Conditions)
departures.freq.weather.weekdays$Subscriber<-factor(departures.freq.weather.weekdays$Subscriber)
departures.freq.weather.weekdays$Customer<-factor(departures.freq.weather.weekdays$Customer)

names(departures.freq.weather.weekdays)
for (t in 31:70){
  departures.freq.weather.weekdays[,t]<-factor(departures.freq.weather.weekdays[,t])
}

#TEST & TRAIN
####################################################
#Use only at the final step
idx2<-runif(nrow(departures.freq.weather.weekdays))>0.75
train.depart <- departures.freq.weather.weekdays[idx2==FALSE,]
test.depart<-departures.freq.weather.weekdays[idx2==TRUE,]
####################################################
train.nmr<-round(0.6*nrow(departures.freq.weather.weekdays),0)
validate.nmr<-round(0.8*nrow(departures.freq.weather.weekdays),0)
train.departure<-departures.freq.weather.weekdays[1:train.nmr, ]
validate.departure<-departures.freq.weather.weekdays[(train.nmr+1):validate.nmr, ]
test.departure<-departures.freq.weather.weekdays[(validate.nmr+1):nrow(departures.freq.weather.weekdays), ]


variables<-paste(colnames(train.departure), collapse='+')

rf <- randomForest(Frequency.departures ~  Hour + Minute.segement+Frequency.departures.t_1+weekday+
                     Temp.+Humidity+Pressure+Conditions+
                     Frequency.departures.t_1+Frequency.departures.t_2+Frequency.departures.t_3+Frequency.departures.t_4+Frequency.departures.t_5+Frequency.departures.t_6+Frequency.departures.t_7+Frequency.departures.t_8+Frequency.departures.t_9+Frequency.departures.t_10+
                     X94107+X94105+X94158+X94025+X94403+X94070+X94404+X94133+X94062+X94061+X94040+X94086+X94010+X94402+X94030+X94303+X94111+X94306+X94087+X95014+X94103+X94002+X94041+X95051+X94611+X94301+X95112+X95110+X94109+X94024+X94108+X95124+nil+X94043+X94065+X95130+X94401+X94104+X94610+X95032+
                     Subscriber+Customer,
                   data=train.departure, importance=TRUE, na.action=na.omit,proximity=TRUE)

#ACCURACY
print(rf)
round(importance(rf), 2)
plot(rf)
prediction<-predict(rf,validate.departure)
validate.departure$prediction<-prediction
validate.departure[is.na(validate.departure$prediction),]
validate.departure.no.na<-validate.departure[!is.na(validate.departure$prediction),]
r2 <- rSquared(validate.departure.no.na$Frequency.departures, validate.departure.no.na$Frequency.departures - validate.departure.no.na$prediction)
mse<-mean((validate.departure$Frequency.departures - prediction)^2, na.rm=T)

