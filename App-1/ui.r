#################################################################################
#ui.R

library(shiny)


shinyUI(pageWithSidebar(
  
  titlePanel("Probability of bike arriving within next 5 minutes"),
  
  sidebarPanel(
    dateInput("date", "Date Input", value = "2014-06-02"), 
    
    selectInput('partOfDay', 'Part of the Day', c('Morning', 'Evening'))
    
  ),
  
  mainPanel(
    plotOutput('plot')
  )
))
