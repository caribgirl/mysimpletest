library(data.table)
library(RPostgreSQL)
library(lubridate)

GetConnection <- function(hostname, portnumber, databasename, username, passwd){
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, host=hostname, port=portnumber, dbname=databasename, user=username, password=passwd)
  return (con)
}

GetTotalSpots <- function(con){
  return(data.table(dbGetQuery(con, sprintf("select * from price_hist"))))
}

GetData <- function(address, port, database, user, passwd){
  con <- GetConnection(address, port, database, user, passwd)
  data <- GetTotalSpots(con)
  rt <- dbDisconnect(con)
  return (data)
}

data <- GetData("gbtestcluster.ctwefsfzej2f.us-west-2.redshift.amazonaws.com", 
                "5439","mydb", "gbuser", "M0nde11e$")

summary <- data[,list(total=length(source)), by=list(nickname, auth_id)]
write.table(summary, file=paste(getwd(), "/mysummary2.csv", sep=""), sep="|", col.names = FALSE, fileEncoding = "UTF-8")

