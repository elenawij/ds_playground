#########################################################################################
#GET WEATHER DATA FROM www.wunderground.com WEBSITE
library(XML)
library(RCurl)
library(stringr)
library(RMySQL)

#source: http://www.wunderground.com/history/airport/KSFO/2014/6/18/DailyHistory.html?req_city=&req_state=&req_statename=&reqdb.zip=&reqdb.magic=&reqdb.wmo=&MR=1

#first define dates you want to collect the data for
#if the algorithm is running on a daily basis at 9 pm, then weather data will be collected at 8:30 pm to have a time buffer for the current day

year<-2015
month<-10
day<-13

URL <- getURL(paste0("http://www.wunderground.com/history/airport/KSFO/", year, "/",ifelse(month<10, "0",""), month,"/",ifelse(day<10, "0",""),day, "/DailyHistory.html?req_city=&req_state=&req_statename=&reqdb.zip=&reqdb.magic=&reqdb.wmo=&MR=1"))
rt <- readHTMLTable(URL, header = TRUE, stringsAsFactors = FALSE)
weather_day<-rt$obsTable
names(weather_day)<-c('Time', "Temp",      "Dew_Point",  "Humidity",   "Pressure",   "Visibility", "Wind_Dir",   "Wind_Speed", "Gust_Speed",
                      "Precip",     "Events",     "Conditions")
weather_day_part1<-weather_day[, c("Time","Wind_Dir", "Conditions")]
weather_day_part2<-apply(weather_day[, c("Temp", "Dew_Point", "Humidity", "Pressure", "Visibility", "Wind_Speed")],2, function(x) str_extract(x, "\\d+\\.*:*\\d*"))
Date<-as.Date(paste0(year, "-",ifelse(month<10, "0",""), month,"-",ifelse(day<10, "0",""),day))
weather_day_reformated<-data.frame('Date'=rep(Date, nrow(weather_day)),weather_day_part2, weather_day_part1)
weather_day_reformated$Time<-format(strptime(weather_day_reformated$Time,format="%I:%M %p"), "%H:%M")
weather_day_reformated$Hour<-as.numeric(format(as.POSIXct(weather_day_reformated$Time, tz="America/Los_Angeles",'%H') , '%H'))

#we will leave only one record per hour, so in case there are few records within an hour, we will remove it
weather_day_reformated$datehour<-paste(weather_day_reformated$Date, weather_day_reformated$Hour)
weather_day_reformated<-weather_day_reformated[!duplicated(weather_day_reformated$datehour),]

#INSERT a NEW RECORD INTO DATABASE (assuming there is table hourly_weather with variables (id, Date, Time, Hour, Temp, Dew_Point, Humidity, Pressure,
#Visibility, Wind_Speed, Wind_Dir, Conditions))

mydb<-dbConnect(MySQL(), user='', password='', dbname='', host='')
bike.data.query<-dbSendQuery(mydb,paste0("INSERT INTO hourly_weather( Date, Time, Hour, Temp, Dew_Point, Humidity, Pressure, Visibility, Wind_Speed, Wind_Dir, Conditions) values('", weather_day_reformated$Date, "', '", weather_day_reformated$Time, "', '",weather_day_reformated$Hour, "', '",
                             weather_day_reformated$Temp, "', '",weather_day_reformated$Dew_Point, "', '",weather_day_reformated$Humidity, "', '",weather_day_reformated$Pressure, "', '",
                             weather_day_reformated$Visibility, "', '",weather_day_reformated$Wind_Speed, "', '",weather_day_reformated$Wind_Dir, "', '",weather_day_reformated$Conditions, "')"))



##################################################################################################################################
#IF WEATHER DATA IS NEEDED TO BE COLLECTED FOR A PERIOD OF TIME, THE BELOW SCRIPT CAN BE USED
year<-2014
for (m in 5:8){
  print(paste0('m=',m))
  for (i in 1:ifelse(m %in% c(5,7,8),31,30)){
    print(paste0('i=',i))
    URL <- getURL(paste0("http://www.wunderground.com/history/airport/KSFO/",year, "/",m,"/",ifelse(i<10, "0",""),i, "/DailyHistory.html?req_city=&req_state=&req_statename=&reqdb.zip=&reqdb.magic=&reqdb.wmo=&MR=1"))
    rt <- readHTMLTable(URL, header = TRUE, stringsAsFactors = FALSE)
    weather_day<-rt$obsTable
    names(weather_day)<-c('Time', "Temp.",      "Dew Point",  "Humidity",   "Pressure",   "Visibility", "Wind Dir",   "Wind Speed", "Gust Speed",
                          "Precip",     "Events",     "Conditions")
    weather_day_part1<-weather_day[, c("Time","Wind Dir", "Conditions")]
    weather_day_part2<-apply(weather_day[, c("Temp.", "Dew Point", "Humidity", "Pressure", "Visibility", "Wind Speed")],2, function(x) str_extract(x, "\\d+\\.*:*\\d*"))
    Date<-as.Date(paste0(year, "-",ifelse(month<10, "0",""), month,"-",ifelse(day<10, "0",""),day))
    weather_day_reformated<-data.frame('Date'=rep(Date, nrow(weather_day)),weather_day_part2, weather_day_part1)
    if(m==5 & i==1){
      weather_March_Aug<-weather_day_reformated
    }else{
      weather_March_Aug<-as.data.frame(rbind(weather_March_Aug, weather_day_reformated))
    }
    Sys.sleep(10)
  }
}
weather_March_Aug$Time<-format(strptime(weather_March_Aug$Time,format="%I:%M %p"), "%H:%M")
weather_March_Aug$Hour<-as.numeric(format(as.POSIXct(weather_March_Aug$Time, tz="America/Los_Angeles",'%H') , '%H'))
weather_March_Aug$Date<-as.Date(weather_March_Aug$Date)
weather_March_Aug$datehour<-paste(weather_March_Aug$Date, weather_March_Aug$Hour)
weather_March_Aug<-weather_March_Aug[!duplicated(weather_March_Aug$datehour),]

#INSERT NEW RECORDS IT TO DATABASE (assuming there is table hourly_weather with variables (id, Date, Time, Hour, Temp, Dew_Point, Humidity, Pressure,
#Visibility, Wind_Speed, Wind_Dir, Conditions))
for (i in 1:nrow(weather_March_Aug)){
  bike.data.query<-dbSendQuery(mydb,paste0("INSERT INTO hourly_weather( Date, Time, Hour, Temp, Dew_Point, Humidity, Pressure, Visibility, Wind_Speed, Wind_Dir, Conditions) values('", weather_day_reformated$Date[i], "', '", weather_day_reformated$Time[i], "', '",weather_day_reformated$Hour[i], "', '",
                                           weather_day_reformated$Temp[i], "', '",weather_day_reformated$Dew_Point[i], "', '",weather_day_reformated$Humidity[i], "', '",weather_day_reformated$Pressure[i], "', '",
                                           weather_day_reformated$Visibility[i], "', '",weather_day_reformated$Wind_Speed[i], "', '",weather_day_reformated$Wind_Dir[i], "', '",weather_day_reformated$Conditions[i], "')"))
  
}


