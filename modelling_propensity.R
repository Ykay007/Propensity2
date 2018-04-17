library(xlsx)
library("ggplot2")
df<-read.xlsx("C://Users/ogun9000/Documents/EmailCampaign/propensity_to_response.xlsx",sheetName="Sheet1")

df <- subset(df, select = c(2,3) )
