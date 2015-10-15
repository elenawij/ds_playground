
#PREDICTING PROBABILITY THAT A BIKE WILL ARRIVE WITHIN 5 MINUTES
#ASSUMPTION: ALGORITHM IS RUN EVERY MINUTE TO PREDICT PROBAILITY FOR A PERIOD 10 MINUTES AHEAD

library(ggplot2)
library(ROCR)
library(randomForest)
library(miscTools)
library(XML)
library(RCurl)
library(stringr)
library(RMySQL)

#1.MYSQL QUERY
#########################################################################################
#GET DATA FROM THE DATABASE

#connect to to data base
mydb<-dbConnect(MySQL(), user='', password='', dbname='', host='')

#get data from data base
bike.data.query<-dbSendQuery(mydb,"select f.id as 'Trip ID', f.duration as Duration, CONVERT_TZ(date(f.start_date),'UTC','America/Los_Angeles') as 'Start Date', st.station_name as 'Start Station', st.terminal_name as 'Start Terminal',CONVERT_TZ(date(f.end_date),'UTC','America/Los_Angeles')  as 'End Date', st2.station_name as 'End Station', st2.terminal_name as 'End Terminal', e.entity_name as 'Bike #', e.entity_type as 'Subscriber Type', z.zip_code  as 'Zip Code' from fact_trips f 
                             join dim_stations st on f.start_station_id=st.id
                             join dim_stations st2 on f.end_station_id=st2.id
                             join dim_entity e on f.entity_id=e.id
                             join dim_entity_zip z on f.entity_id=z.entity_id")
bike.data<-fetch(bike.data.query, n=-1)
#########################################################################################

#2.ANALYSIS
#########################################################################################
#for now we will use exteranally obtained data
bike.data<-read.csv('/Users/elenazadnepranets/Dropbox/UBER/201408_trip_data.csv')

#create new variables
bike.data$Start.Date2<-as.POSIXct(as.character(bike.data$Start.Date), tz="America/Los_Angeles", format = "%m/%d/%Y %H:%M")
bike.data$End.Date2<-as.POSIXct(as.character(bike.data$End.Date), tz="America/Los_Angeles", format = "%m/%d/%Y %H:%M")
bike.data$hour.end<-as.numeric(format(bike.data$End.Date2, "%H"))
bike.data$hour.start<-as.numeric(format(bike.data$Start.Date2, "%H"))

#get weathere data
#!!! ad script for db extraction
#for now I've just save the file locally to use in the prediction
weather_March_Aug<-read.csv('/Users/elenazadnepranets/Dropbox/UBER/weather_March_Aug.csv')


#for now we will analise data for terminal 70 and month June-July
bike.data.terminal<-bike.data[bike.data$End.Terminal==70,]
bike.data.terminal.weekday.peakhour<-bike.data.terminal[bike.data.terminal$weekday.dummy==1 & (as.numeric(bike.data.terminal$hour.end) %in% c(7,8,9,10,16,17,18,19)), ]
timeline.jun_jul<-seq(from=as.POSIXct("2014-6-1 0:00", tz="America/Los_Angeles"),to=as.POSIXct("2014-7-31 23:59", tz="America/Los_Angeles"), by="min")

#CREATE DATASET WITH BINARY OUTPUT WHETHER BIKE HAS ARRIVED WITHIN 5 MINUTES AT EVERY POINT OF TIME 
station.bike.got.avlbl.jun_jul<-data.frame('timeline.t.'=character(), 'bike.got.avlbl'=numeric())
colnames(station.bike.got.avlbl.jun_jul)<-c('timeline.t.', 'bike.got.avlbl')
for (t in 1:length(timeline.jun_jul)){
  trips<-bike.data.terminal[format(bike.data.terminal$End.Date2, '%Y-%m-%d %H:%M')>=format(timeline.jun_jul[t], '%Y-%m-%d %H:%M') & format(bike.data.terminal$End.Date2, '%Y-%m-%d %H:%M')<=format(timeline.jun_jul[t+5], '%Y-%m-%d %H:%M'),]
  count<-nrow(trips[!is.na(trips$End.Date),] )
  bike.got.avlbl.jun_jul<-ifelse(count>0,1,0)
  temp<-data.frame(timeline.jun_jul[t], bike.got.avlbl.jun_jul)
  if(t==1){
    station.bike.got.avlbl.jun_jul<-temp
  }else{
    station.bike.got.avlbl.jun_jul<-rbind(  station.bike.got.avlbl.jun_jul, temp)
    colnames(station.bike.got.avlbl.jun_jul)<-c('timeline.jun_jul.t.', 'bike.got.avlbl.jun_jul')
  }
  
}

#add variables
data.set_june_july<-station.bike.got.avlbl.jun_jul
data.set_june_july$month<-as.numeric(format(data.set_june_july$timeline.jun_jul.t., '%m'))
data.set_june_july$day<-as.numeric(format(data.set_june_july$timeline.jun_jul.t., '%d'))
data.set_june_july$weekday<-weekdays(data.set_june_july$timeline.jun_jul.t.)
data.set_june_july$hour<-as.numeric(format(data.set_june_july$timeline.jun_jul.t., '%H') )
data.set_june_july$min<-as.numeric(format(data.set_june_july$timeline.jun_jul.t., '%M'))
data.set_june_july$date<-format(data.set_june_july$timeline.jun_jul.t., '%Y-%m-%d')
data.set_june_july$datehour<-paste(data.set_june_july$date, data.set_june_july$hour)



##################################################################
#FEATURE GENERATION

##
#START STATIONS SET
#For now we will focus on deprature stations of incoming trips of terminal 70 within the last two weeks from current date
#!!Note: two weeks are chose arbitrary, requires deeper analysis of start stations deviation over time
#also we will focus only on weekdays and peak hours
current.date<-as.Date('2014-06-13')
bike.data.terminal.weekday.peakhour.Start.Stations<-as.vector(bike.data.terminal.weekday.peakhour$Start.Station[as.Date(bike.data.terminal.weekday.peakhour$Start.Date2)>=current.date-10
                                                                                                                & as.Date(bike.data.terminal.weekday.peakhour$Start.Date2)<=current.date])
bike.data.terminal.weekday.peakhour.Start.Stations.table<-sort(table(bike.data.terminal.weekday.peakhour.Start.Stations), decreasing = T)
Start.Stations.freq<-data.frame(Start_stations=names(bike.data.terminal.weekday.peakhour.Start.Stations.table), frequency=as.numeric(bike.data.terminal.weekday.peakhour.Start.Stations.table))
##

###
#GENERATE DATASET THAT SHOWS OUTBOUND TRIPS FROM 'START STATIONS SET' 5-10 MIN AGO
timeline.jun_jul.weekdays.peakhour<-timeline.jun_jul[!(weekdays(timeline.jun_jul) %in% c('Sunday', 'Saturday')) & (hour(timeline.jun_jul) %in% c(7,8,9,10,16,17,18,19))]
for (t in 1:4800){
  print(t)
  Start.stations.departures<-bike.data[format(bike.data$Start.Date2, '%Y-%m-%d %H:%M')>=format(timeline.jun_jul.weekdays.peakhour[t]-1800, '%Y-%m-%d %H:%M') & format(bike.data$Start.Date2, '%Y-%m-%d %H:%M')<=format(timeline.jun_jul.weekdays.peakhour[t]-300, '%Y-%m-%d %H:%M')
                                       & bike.data$Start.Station %in% as.vector(Start.Stations.freq$Start_stations),]
  trip.per.station<-aggregate(Start.stations.departures$Trip.ID, list(Start.stations.departures$Start.Station), length)
  colnames(trip.per.station)<-c('Stations', format(timeline.jun_jul.weekdays.peakhour[t], '%Y-%m-%d %H:%M'))
  if (t==1921){
    trip.per.station.t<-trip.per.station
  }else{
    trip.per.station.t<-merge(trip.per.station.t.2, trip.per.station, by='Stations', all=T)
  }
}

#temp2<-trip.per.station.t
#temp2[is.na(temp2)]<-0
trip.per.station.t.transpose<-t(trip.per.station.t)
names<- trip.per.station.t.transpose[1,]
colnames(trip.per.station.t.transpose)<-names
trip.per.station.t.transpose<-trip.per.station.t.transpose[-1,]
timeline.jun_jul.t.<-as.POSIXct(rownames(trip.per.station.t.transpose), tz="America/Los_Angeles",'%Y-%m-%d %H:%M')
trip.per.station.t.transpose2<-data.frame(timeline.jun_jul.t., trip.per.station.t.transpose)
###

#previous day departures
bike.data.terminal.Start<-bike.data[bike.data$Start.Terminal==70,]
total.departure.daily<- aggregate(bike.data.terminal.Start$Trip.ID, by=list(format(bike.data.terminal.Start$Start.Date2, '%Y-%m-%d')), length)
colnames(total.departure.daily)<-c('date.t_1','nmbr_of_depatures')
total.departure.daily$date<-format(as.POSIXct(as.character(total.departure.daily$date.t_1), tz="America/Los_Angeles", '%Y-%m-%d')+86400, '%Y-%m-%d')

#previous day arrivals
total.arrivals.daily<- aggregate(bike.data.terminal$Trip.ID, by=list(format(bike.data.terminal$End.Date2, '%Y-%m-%d')), length)
colnames(total.arrivals.daily)<-c('date.t_1','nmbr_of_arrivals')
total.arrivals.daily$date<-format(as.POSIXct(as.character(total.arrivals.daily$date.t_1), tz="America/Los_Angeles", '%Y-%m-%d')+86400, '%Y-%m-%d')

#previous day  Round trips
round.trips.daily<- aggregate(bike.data.terminal$Round.Tribp, by=list(format(bike.data.terminal$End.Date2, '%Y-%m-%d')), sum)
#round.trips.daily2<-table(format(bike.data.terminal$End.Date2[bike.data.terminal$Round.Tribp==1], '%Y-%m-%d'))
colnames(round.trips.daily)<-c('date.t_1','nmbr_of_round_trips')
round.trips.daily$date<-format(as.POSIXct(as.character(round.trips.daily$date.t_1), tz="America/Los_Angeles", '%Y-%m-%d')+86400, '%Y-%m-%d')


#hourly weather past hour
weather_March_Aug<-read.csv('/Users/elenazadnepranets/Dropbox/UBER/weather_March_Aug.csv')
weather_March_Aug.h_1<-data.frame('Hour'=weather_March_Aug$Hour+1,subset(weather_March_Aug, select = -c(Hour, X,Time,Time2)))
names<-colnames(weather_March_Aug.h_1)
names_part2<-names[!(names %in%  c("Hour",  "datehour", "Date"))]
names_part2<-sapply(names_part2, function(x) paste0(x, '.t_1'), simplify=T)
colnames(weather_March_Aug.h_1)<-c( "Hour", names_part2, "datehour", "Date")




####################################################
#MERGE ALL DATA
data.set_june<-merge(data.set_june_july,trip.per.station.t.transpose2, by='timeline.jun_jul.t.', all.y =T )

#merge with hourly weather
data.set_june<-merge(data.set_june, weather_March_Aug.h_1, by=c("datehour"), all.x=T)


#Reformat vriables
data.set_june$weekday<-factor(data.set_june$weekday)
data.set_june$min<-as.numeric(data.set_june$min)
names(data.set_june)
for (t in 10:43){
  data.set_june[,t]<-as.numeric(data.set_june[,t])
}

#Limit to peak hours and weekdays
data.set_june.peakhours.weekday<-data.set_june[!(data.set_june$weekday %in% c('Sunday', 'Saturday')) &
                                                 as.numeric(data.set_june$hour) %in% c(7,8,9,10,16,17,18,19),]



# TRAIN A MODEL
####################################################
#Use only at the final step
idx2<-runif(nrow(data.set_june.peakhours.weekday))>0.75
train <- data.set_june.peakhours.weekday[idx2==FALSE,]
test<-data.set_june.peakhours.weekday[idx2==TRUE,]


#Rerun to train and validate models
idx <- runif(nrow(train)) > 0.75
train2<-train[idx==F,]
validate <- train[idx==TRUE,]

variables<-paste(colnames(train), collapse='+')
#names(train)

rf <- randomForest(factor(bike.got.avlbl.jun_jul) ~  weekday+hour+min+
                     X2nd.at.Folsom+X2nd.at.South.Park+X2nd.at.Townsend+X5th.at.Howard+Beale.at.Market+Broadway.St.at.Battery.St+Civic.Center.BART..7th.at.Market.+Clay.at.Battery+Commercial.at.Montgomery+Davis.at.Jackson+Embarcadero.at.Bryant+Embarcadero.at.Folsom+Embarcadero.at.Sansome+Embarcadero.at.Vallejo+Grant.Avenue.at.Columbus.Avenue+Harry.Bridges.Plaza..Ferry.Building.+Howard.at.2nd+Market.at.10th+Market.at.4th+Market.at.Sansome+Mechanics.Plaza..Market.at.Battery.+Post.at.Kearny+Powell.Street.BART+Powell.at.Post..Union.Square.+San.Francisco.Caltrain..Townsend.at.4th.+San.Francisco.Caltrain.2..330.Townsend.+San.Francisco.City.Hall+South.Van.Ness.at.Market+Spear.at.Folsom+Steuart.at.Market+Temporary.Transbay.Terminal..Howard.at.Beale.+Townsend.at.7th+Washington.at.Kearny+Yerba.Buena.Center.of.the.Arts..3rd...Howard.+
                     Temp.t_1+Dew_Point.t_1+Humidity.t_1+Pressure.t_1+Visibility.t_1+Wind_Speed.t_1,
                   type="classification", data=train, importance=TRUE, na.action=na.omit)

#ACCURACY
prob_resp_default<-predict(rf, newdata=test, type="response")
table<-as.data.frame(table(as.numeric(as.character(prob_resp_default)), test$bike.got.avlbl.jun_jul))
print(rf)
error.rate<-(table[2,3]+table[3,3])/(table[1,3]+table[2,3]+table[2,3]+table[4,3])
error.rate2<-mean(as.numeric(as.character(prob_resp_default))==test$bike.got.avlbl,na.rm = T)


#PREDICTON
prob_default <- predict(rf, newdata=test, type="prob")[,2]
test$prob_default<-predict(rf, newdata=test, type="prob")[,2]

#VISUALISE
test.date<-test[test$date=='2014-06-02',]
plot.first.part<-ggplot(test.date[test.date$hour<=10,], aes(timeline.jun_jul.t., prob_default))+geom_line(color="lightpink4", lwd=1)+
  labs(x="Time", y="Probability")+theme(
    axis.title.x = element_text(color="lightpink4", vjust=-0.35),
    axis.title.y = element_text(color="lightpink4" , vjust=0.35)   
  )+scale_x_datetime(breaks = date_breaks("30 min"),minor_breaks = date_breaks("5 min"),labels=date_format("%H:%M", tz="America/Los_Angeles"))+
  ylim(0,1)

plot.second.part<-ggplot(test.date[test.date$hour>=16,], aes(timeline.jun_jul.t., prob_default))+geom_line(color="cadetblue", lwd=1)+
  labs(x="Time", y="Probability")+theme(
    axis.title.x = element_text(color="cadetblue", vjust=-0.35),
    axis.title.y = element_text(color="cadetblue" , vjust=0.35)   
  )+scale_x_datetime(breaks = date_breaks("30 min"),minor_breaks = date_breaks("5 min"),labels=date_format("%H:%M", tz="America/Los_Angeles"))+
  ylim(0,1)



#FEATURE IMPORTANCE
imp <- importance(rf, type=1)
featureImportance <- data.frame(Feature=row.names(imp), Importance=imp[,1])

p <- ggplot(featureImportance, aes(x=reorder(Feature, Importance), y=Importance)) +
  geom_bar(stat="identity", fill="#53cfff") +
  coord_flip() + 
  theme_light(base_size=10) +
  xlab("Importance") +
  ylab("") + 
  ggtitle("Random Forest Feature Importance\n") +
  theme(plot.title=element_text(size=12))
ggsave("2_feature_importance4.png", p)




#CREATE PREDICTON FOR THE WHOLE DATASET AND SAVE IT TO USE IN THE APP
#save file for usage in the app
data.set_june.peakhours.weekday$prob_default<-predict(rf, newdata=data.set_june.peakhours.weekday, type="prob")[,2]
write.csv(data.set_june.peakhours.weekday, '/Users/elenazadnepranets/Dropbox/UBER/App-1/data.set_june.peakhours.weekday.csv')



