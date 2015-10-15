#################################################################################
#ui.R

library(shiny)


shinyUI(fluidPage(
  titlePanel("Probability of bicycle arriving within next 5 minutes"),
  fluidRow(
    column(4, wellPanel(
      #sliderInput('foo1', 'How many days after today?', min = -10, max = 10, value = 0),
      #dateInput('foo2', 'Input a date:', value = Sys.time())
      dateInput("date", "Choose date (covers period from 2014-06-02 till 2014-06-13 and only weekdays):", value = "2014-06-02"),
      selectInput("hour", label = "Choose hour (limited to peak hours):", 
                  choices = list("7" = 7, "8" = 8,
                                 "9" = 9, "10" = 10, "16" = 16, "17" = 17, 
                                 "18" = 18, "19" = 19), selected = 1),
      #numericInput(inputId = "hour",
                   #label = "Choose hour:",7,
                   #min=7,
                   #max=19
      #),
      numericInput(inputId = "min",
                  label = "Choose minute:",5,
                  min=0,
                  max=59
      )
    )),
    column(8,
           verbatimTextOutput("text"),
           br(),
           br(),
           p("Note: The probability shows what the chances are that a bicycle will arrive within next 5 minutes from the chosen time")
    )
  ))
)
