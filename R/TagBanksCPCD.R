#' Tag banker i CP/CD-data fra DTCC
#'
#' @param data datasett som skal tagges
#'
#' @export
#'
TagBanksCPCD <- function(data) {
  
  issuers$issuername <- trimws(issuers$issuername)
  issuers$issuername <- gsub("[[:space:]]", " ", issuers$issuername)
  
  data$IssuerName = trimws(data$IssuerName)
  
  data <- merge(data, issuers, by.x = "IssuerName", by.y = "issuername", all.x = TRUE)
  
  # Tag NIBOR banks
  
  # Remove leading/trailing white spaces from issuer names
  data$IssuerName <- trimws(data$IssuerName)
  data$ParentName <- ifelse(is.na(data$ParentName), data$IssuerName, data$ParentName)
  data$ParentName <- data$ParentName <- gsub("[[:space:]]", " ", data$ParentName)
  data$ParentName <- trimws(data$ParentName)
  
  return(data)
}
