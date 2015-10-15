#################################################################################
#server.R
library(shiny)
library(ggplot2)
library(scales)
data<-read.csv('data.set_june.peakhours.weekday.csv',stringsAsFactors = F)
#data$hour<-as.numeric(data$hour)
#data$timeline.jun_jul.t.<-as.POSIXct(data$timeline.jun_jul.t., tz="America/Los_Angeles", format = "%Y-%m-%d %H:%M")
#data$date<-format(as.POSIXct(data$date, tz="America/Los_Angeles", format = "%Y-%m-%d"), "%Y-%m-%d")
shinyServer(function(input, output) {
  

  output$text <- renderText({

   paste0("Probability that a bycicle will arrive within next 5 minutes is ",data[ format(as.POSIXct(data$date, tz="America/Los_Angeles", format = "%Y-%m-%d"), "%Y-%m-%d")==input$date & data$hour==input$hour & data$min==input$min  ,'prob_default']*100, "%")
  })
})
