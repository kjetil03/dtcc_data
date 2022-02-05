#' Adds maturity categories
#'
#' @description 
#' 
#' \code{AddMaturityBuckets} legger til en kolonne med løpetidsbuckets for CP/CD-data fra DTCC
#' 
#' @param data (CP/CD) data set.
#' @export
#' 
#' @examples 
#' \dontrun{
#' dtcc_data = dtcc_data %>% AddMaturityBuckets()
#' }

AddMaturityBuckets <- function(data) {
  
  data$Maturity <- cut(data$OriginalMaturity,
                       c(1, 10, 30, 60, 80, 100, 150, 250, 381, 550, 10000),
                       c("1-10 d", "11-25 d", "25-60 d", "60-80 d", "81-100 d", "101-150 d", "151-250 d", "251-380 d", "381-550 d", "550+ d"))
  
  #La til dette, siden koden failer for data der maturity date er absurd høy
  data = data[!is.na(as.character(data$Maturity)),]
  
  return(data)
}