#################################################################################
#server.R
library(shiny)
library(ggplot2)
library(scales)
data<-read.csv('data.set_june.peakhours.weekday.csv',stringsAsFactors = F)
#data$hour<-as.numeric(data$hour)
#data$timeline.jun_jul.t.<-as.POSIXct(data$timeline.jun_jul.t., , tz="America/Los_Angeles", format = "%Y-%m-%d %H:%M:%S")
shinyServer(function(input, output) {
  
  dataset <- reactive({
    if(input$partOfDay=='Morning'){
      data[data$date==input$date &  data$hour<=10 ,]

    }else{
      data[data$date==input$date &   data$hour>=16,]

    }
  })
    color<-reactive({
      if(input$partOfDay=='Morning'){
        color<-"lightpink4"
        
      }else{
        color<-"cadetblue"
      }
   
  })
  
  output$plot <- renderPlot({
    
    p <- ggplot(dataset(), aes(timeline.jun_jul.t., prob_default)) + geom_line(color=color(), lwd=1)+  labs(x="Time", y="Probability")+theme(
      axis.title.x = element_text(color=color(), vjust=-0.35),
      axis.title.y = element_text(color=color() , vjust=0.35)   
    )+scale_x_datetime(breaks = date_breaks("30 min"),minor_breaks = date_breaks("5 min"),labels=date_format("%H:%M", tz="America/Los_Angeles"))+
      ylim(0,1)
    #ggplot(validate[validate$hour<=10 & validate$date==date.chosen,], aes(timeline.jun_jul.t., prob_default))+geom_line(color="lightpink4", lwd=1)
    
    
    print(p)
    
  }, height=700)
  
})
