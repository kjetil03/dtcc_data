#' Get CPCD-data from DTCC
#'
#'
#' @importFrom  dplyr filter mutate left_join rename select if_else group_by
#' @importFrom RODBC odbcDriverConnect sqlQuery
#' @import magrittr
#' @importFrom anytime anydate
#'
#' @export
#'
#'
#' @encoding UTF-8
#'
#' @examples
#' \dontrun{
#' dtcc_data = GetDTCCData()
#' }
#' 
GetDTCCData <- function() {
  

  sql_srch = "


select 

CUSIP,ProductType,IssuerName,IssueDate,SettlementDate,MaturityDate,InterestRateType,IncomePaymentType,

                PrincipalAmount,

                SettlementAmount,

                InterestRate,

				TimeToMaturity,
				
				MaxTimeToMaturity,

				min(SettlementDate)  over (partition by cusip, MaxTimeToMaturity) as EstIssueDate


from (

SELECT CUSIP,ProductType,IssuerName,IssueDate,SettlementDate,MaturityDate,InterestRateType,IncomePaymentType,

                PrincipalAmount,

                SettlementAmount,

                InterestRate,

                DATEDIFF(day, SettlementDate, MaturityDate) AS TimeToMaturity,

                MAX(DATEDIFF(day, SettlementDate, MaturityDate)) OVER (PARTITION BY Cusip, MaturityDate) AS MaxTimeToMaturity

       FROM (

             SELECT CUSIP,ProductType,IssuerName,IssueDate,SettlementDate,MaturityDate,InterestRateType,IncomePaymentType,

                       SUM([principalamount])  AS PrincipalAmount,

                       SUM([settlementamount]) AS SettlementAmount,

                       AVG([interestrate])     AS InterestRate      

             FROM   [NBDataHub].[DTCC].[CPCDtransactions]

         GROUP BY CUSIP,ProductType,IssuerName,IssueDate,SettlementDate,MaturityDate,InterestRateType,IncomePaymentType

  ) AS dtcc

  ) as dtcc2"


  dbhandle <- odbcDriverConnect('driver={SQL Server};SERVER=wm-x-s-31;database = NBDataHub;trusted_connection=true')
  
  dtcc_data <-  sqlQuery(dbhandle, sql_srch)
  
  close(dbhandle)

  dtcc_data = dtcc_data %>%
    mutate(IssuerName = trimws(IssuerName),
           CUSIP = as.character(CUSIP),
           SettlementDate = anytime::anydate(SettlementDate),
           MaturityDate = anytime::anydate(MaturityDate),
           IssueDate = anytime::anydate(IssueDate),
           EstIssueDate = anytime::anydate(EstIssueDate),
           OriginalMaturity = as.double(MaturityDate - EstIssueDate),
           TimeToMaturity = as.double(MaturityDate - SettlementDate),
           Yield = InterestRate,
           Yield = if_else(InterestRateType == "F" & IncomePaymentType == "Z",
                           ((PrincipalAmount - SettlementAmount)/SettlementAmount)*(360/TimeToMaturity)*100, Yield),
           Yield = if_else(InterestRateType == "F" & IncomePaymentType == "I",

                           (((1+(InterestRate/100)/360*MaxTimeToMaturity)*PrincipalAmount - SettlementAmount)/SettlementAmount)*360/TimeToMaturity*100, Yield),
           IssueDate = if_else(is.na(IssueDate), SettlementDate, IssueDate))


  # dtcc_data = dtcc_data %>%
  #   group_by(CUSIP, MaxTimeToMaturity) %>%
  #   mutate(EstIssueDate = min(SettlementDate)) %>%
  #   ungroup() %>%
  #   mutate(OriginalMaturity = as.double(MaturityDate - EstIssueDate))


  dtcc_data = dtcc_data %>%
    AddMaturityBuckets() %>%
    TagBanksCPCD()
  
  return(dtcc_data)
  
  
  
}