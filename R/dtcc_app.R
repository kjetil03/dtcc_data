
#' DTCC-app
#'
#' @param load_source_data Spesifiser om data skal lastes fra kilde. Tar T eller F
#' @param pre_loaded_data Hvis load_source_data == F, spesifiser navn på et datasett lastet inn med GetDTCCData-funksjonen
#'
#' @importFrom dplyr filter mutate left_join rename select if_else group_by summarise arrange bind_rows ungroup
#' @importFrom stringr str_replace str_replace_all str_trunc
#' @import magrittr
#' @importFrom shiny fluidRow sidebarLayout verbatimTextOutput splitLayout tags titlePanel column tabsetPanel radioButtons checkboxGroupInput selectInput div hr icon sidebarPanel renderUI mainPanel uiOutput fluidPage tabPanel dateRangeInput checkboxInput conditionalPanel reactive observeEvent downloadHandler shinyApp runApp downloadButton numericInput HTML textInput h1 br
#' @importFrom shinyWidgets pickerInput updatePickerInput pickerOptions
#' @importFrom htmlwidgets saveWidget
#' @importFrom plotly plotlyOutput renderPlotly as_widget
#' @importFrom shinyTime timeInput
#' @importFrom openxlsx write.xlsx
#' @importFrom tidyselect all_of
#' @importFrom shinyjs useShinyjs
#' @importFrom rlang expr exprs
#' @import nomafunctions
#' 
#' @return
#' @export
#'
#' @examples
#' \dontrun{
#' #Start app med innlasting av data
#' dtcc_app()
#' # Start app med ferdig datasett
#' d = GetDTCCData()
#' dtcc_app(F, pre_loaded_data = d)
#'
#' }
dtcc_app <- function(load_source_data = T,
                     pre_loaded_data = NULL) {
  
  
  
  if(load_source_data) {
    cat("Laster inn data, vennligst vent.
        \nDette kan ta litt tid, avhengig av hastighet på tilkobling etc., vanligvis
        \nopptil ett og et halvt minutt.
        \n 
        \nAlternativ kan man laste ned datasett på forhånd, og kjøre applikasjon
        \nmed 'load_source_data = F'. Man må da spesifisere navn på forhåndsinnlastet
        \ndatasett med parameteren pre_loaded_data")
    #Hent rådata
    raw_data = GetDTCCData()
    
    cat("\n\nData lastet inn, starter applikasjon.\n\n")
    
    
  } else{
    #Sjekk om pre_loaded_data er spesifisert riktig.
    if( is.data.frame(pre_loaded_data) && 
        names(pre_loaded_data) == c( "IssuerName",  
                                     "CUSIP",           
                                     "ProductType",     
                                     "IssueDate",        
                                     "SettlementDate", 
                                      "MaturityDate",
                                     "InterestRateType",
                                     "IncomePaymentType", 
                                     "PrincipalAmount", 
                                     "SettlementAmount",
                                      "InterestRate", 
                                     "TimeToMaturity",
                                     "MaxTimeToMaturity",
                                     "Yield",
                                     "EstIssueDate",
                                     "OriginalMaturity",
                                     "Maturity",
                                     "d_bank",
                                     "ParentName",
                                     "CountryCode",
                                     "NiborBank")) {
      
      raw_data = pre_loaded_data
      
      print("Datasett ser OK ut, starter applikasjon") 
      
    }  else{
      stop(cat("Nødvendig datasett ikke funnet. Spesifiser et datasett hentet
               n\ med GetDTCCData-funksjonen i pre_loaded_data_feltet"))
    }
    
  }
  
  
  
  bloom_choices = c(
    "Libor 1m" =  "us0001m_index_px_last",
    "Libor 2m" =  "us0002m_index_px_last",
    "Libor 3m" = "us0003m_index_px_last",
    "Libor 6m" = "us0006m_index_px_last",
    "DNB CP-indeks 1m" = "dnorus1m_index_px_last",
    "DNB CP-indeks 2m" = "dnorus2m_index_px_last",
    "DNB CP-indeks 3m" =  "dnorus3m_index_px_last",
    "DNB CP-indeks 6m" =  "dnorus6m_index_px_last",
    "BSBY 1m" =  "bsby1m_index_px_last",
    "BSBY 2m" = "bsby2m_index_px_last",
    "BSBY 3m" = "bsby3m_index_px_last",
    "BSBY 6m" = "bsby6m_index_px_last",
    "Kliem 1m" = "usdra_klmm_curncy_px_last",
    "Kliem 2m" = "usdrb_klmm_curncy_px_last",
    "Kliem 3m" = "usdrc_klmm_curncy_px_last",
    "Kliem 6m" = "usdrf_klmm_curncy_px_last",
    "1m EUR OIS swappet til USD" = "eur_1m_ois_swapped",
    "2m EUR OIS swappet til USD" = "eur_2m_ois_swapped",
    "3m EUR OIS swappet til USD" = "eur_3m_ois_swapped",
    "6m EUR OIS swappet til USD" = "eur_6m_ois_swapped",
    "Styringsrente i USA" = "fdtr_index_px_last",
    "US OIS 1m" = "ussoa_bgnl_curncy_px_last",
    "US OIS 2m" = "ussob_bgnl_curncy_px_last",
    "US OIS 3m" = "ussoc_bgnl_curncy_px_last",
    "US OIS 6m" = "ussof_bgnl_curncy_px_last",
    "IOER" = "irrbioer_index_px_last")
  
  
  
  maturities_names = c("volume" = "matured",
                       "cummulative volume" = "cumsum_matured",
                       "percent of outstanding" = "percent_matured",
                       "cummulative percent of outstanding" = "percent_matured_cumsum")
  
  textInputRow<-function (inputId, label, value = "")
  {
    div(style="display:inline-block",
        tags$label(label, `for` = inputId),
        tags$input(id = inputId, type = "text", value = value,class="input-small"))
  }
  
  
  
  ui <- fluidPage(
    
    # Header
    titlePanel(h1(div( "CP/CD: Amerikanske pengemarkedsdata"),
                   align = "center",
                   style = "background-color:#1d2f3e; padding: 20px; color:white"),
                windowTitle = "CP/CD"),
    
    hr(),
    
    sidebarLayout(
      
      #Velg utstedertype
      sidebarPanel(
        width= 2,
        checkboxGroupInput(
          "choose_issuer_type",
          "Velg utstedertyper",
          choices = c("Banker", "Foretak"),
          selected = c("Banker"),
          inline = T,
          width = NULL,
          choiceNames = NULL,
          choiceValues = NULL
        ),
        
        radioButtons("choose_issuer_level",
                     "Velg utstedernivå",
                     choices = c("Gruppe" = "ParentName",
                                 "Filial" = "IssuerName"),
                     inline = T,
                     selected = "ParentName"
                     
        ),
        
        #Velg produkttype
        uiOutput("choose_product_type"),
        
        #Velg produkttype
        checkboxGroupInput(
          "choose_interest_type",
          "Velg rentetype",
          choices = c("Fast" = "F",
                      "Flyt" = "V"),
          selected = c("F", "V"),
          inline = T,
          width = NULL
        ),
        
        uiOutput("choose_maturities"),
        
        #Velg utstederland
        uiOutput("choose_issuer_country"),
        
        
        
        #Velg utsteder
        uiOutput("choose_issuer"),
        
        
        selectInput("choose_sorting_var",
                    "Del opp etter",
                    choices = c("Løpetid" = "Maturity",
                                "Land på utsteder" = "CountryCode",
                                "Utsteder (gruppenivå)" = "ParentName",
                                "Utsteder (filialnivå)" = "IssuerName",
                                "Produkttype" = "ProductType",
                                "Rentetype" = "InterestRateType")
                    
                    
        )
        
        
        
      ),
      
      mainPanel(width = 9,
                tabsetPanel(id = "tabs",
                            tabPanel("Utstedt volum",
                                     
                                     dateRangeInput("issue_date",
                                                    "Velg datoer",
                                                    start = Sys.Date()-30,
                                                    end = Sys.Date(),
                                                    min = "2014-01-01",
                                                    max = Sys.Date(),
                                                    format = "dd-mm-yyyy"
                                     ),
                                     fluidRow(column(6,splitLayout(textInput("issue_title",
                                                                             "Velg tittel til plot",
                                                                             "Utstedt volum i CP/CD-markedet"),
                                                                   
                                                                   textInput("issue_subtitle",
                                                                             "Velg undertittel til plot",
                                                                             "")))),
                                     plotlyOutput("p_issue"),
                                     
                                     br(),
                                     
                                     checkboxInput("show_issued_code",
                                                   "Vis kode bak plot",
                                                   value = F),
                                     
                                     conditionalPanel("input.show_issued_code == 1",
                                                      tags$h5("Kode for å filtrere data:"),
                                                      verbatimTextOutput("data_issued_code"),
                                                      tags$h5("Kode for å generere plot:"),
                                                      verbatimTextOutput("plot_code_issued"))
                                     
                            ),
                            
                            tabPanel("Utestående volum",
                                     dateRangeInput("outstanding_date",
                                                    "Velg datoer",
                                                    start = as.Date("2016-01-01"),
                                                    end = Sys.Date(),
                                                    min = "2014-01-01",
                                                    max = Sys.Date(),
                                                    format = "dd-mm-yyyy"),
                                     fluidRow(column(6,splitLayout(textInput("outstanding_title",
                                                                             "Velg tittel til plot",
                                                                             "Utestående volum i CP/CD-markedet"),
                                                                   
                                                                   textInput("outstanding_subtitle",
                                                                             "Velg undertittel til plot",
                                                                             "")))),
                                     br(),
                                     
                                     plotlyOutput("p_outstanding"),
                                     selectInput("outstanding_frequency",
                                                 "Velg frekvens",
                                                 choices = c("Daglig" = "d",
                                                             "Business" = "b",
                                                             "Ukentlig" = "w",
                                                             "Månedlig" = "m",
                                                             "Kvartalsvis" = "q",
                                                             "Årlig" = "y"),
                                                 selected = "w"),
                                     
                                     selectInput("outstanding_plot_type",
                                                 "Velg plot type",
                                                 choices = c("Stacked area" = "stacked area",
                                                             "Linje" = "line",
                                                             "Stacked bar" = "stacked bar"),
                                                 selected = "stacked area"),
                                     
                                     br(),
                                     checkboxInput("show_outstanding_code",
                                                   "Vis kode bak plot",
                                                   value = F),
                                     
                                     conditionalPanel("input.show_outstanding_code == 1",
                                                      tags$h5("Kode for å filtrere data:"),
                                                      verbatimTextOutput("data_outstanding_code"),
                                                      tags$h5("Kode for å generere plot:"),
                                                      verbatimTextOutput("plot_code_outstanding"))
                                     
                            ),
                            
                            
                            tabPanel("Renter",
                                     dateRangeInput("rates_date",
                                                    "Velg datoer",
                                                    start = Sys.Date()-30,
                                                    end = Sys.Date(),
                                                    min = "2014-01-01",
                                                    max = Sys.Date(),
                                                    format = "dd-mm-yyyy"
                                     ),
                                     fluidRow(column(6,splitLayout(textInput("rates_title",
                                                                             "Velg tittel til plot",
                                                                             "Renter i CP/CD-markedet"),
                                                                   
                                                                   textInput("rates_subtitle",
                                                                             "Velg undertittel til plot",
                                                                             "")))),
                                     br(),
                                     
                                     plotlyOutput("p_rates"),
                                     br(),
                                     pickerInput("choose_index",
                                                 "Vis referansekurve",
                                                 choices = bloom_choices ,
                                                 selected = "",
                                                 multiple = T,
                                                 options = pickerOptions(
                                                   `actions-box` = TRUE,
                                                   liveSearch = T,
                                                   liveSearchNormalize = T,
                                                   selectAllText = "Velg alle",
                                                   deselectAllText = "Dropp alle")
                                     ),
                                     checkboxInput("choose_filtering",
                                                   "Filtrer ut uteliggere",
                                                   value = TRUE
                                     ),
                                     conditionalPanel("input.choose_filtering == 1",
                                                      numericInput("choose_filtering_level",
                                                                   "Tillatt absolutt avstand fra gjennomsnittsrente (i prosentpoeng)",
                                                                   value = 5,
                                                                   min = 0,
                                                                   max = 1000,
                                                                   step = 0.5)
                                     ),
                                     checkboxInput("show_rates_code",
                                                   "Vis kode bak plot",
                                                   value = F),
                                     
                                     conditionalPanel("input.show_rates_code == 1",
                                                      tags$h5("Kode for å filtrere data:"),
                                                      verbatimTextOutput("data_rates_code"),
                                                      tags$h5("Kode for å generere plot:"),
                                                      verbatimTextOutput("plot_code_rates"),
                                                      tags$h5("Kode for å importere bloombergdata:"),
                                                      verbatimTextOutput("bloomberg_code"),
                                                      tags$h5("Kode for å legge til indekser i plot"),
                                                      verbatimTextOutput("add_bloomberg_lines")
                                                      
                                                      
                                     )
                                     
                                     
                                     
                                     
                                     
                            ),
                            
                            tabPanel("Forfall",
                                     dateRangeInput("maturity_date",
                                                    "Velg datoer",
                                                    start = Sys.Date()-30,
                                                    end = Sys.Date() + 180,
                                                    min = "2014-01-01",
                                                    max = Sys.Date()+500,
                                                    format = "dd-mm-yyyy"
                                     ),
                                     fluidRow(column(6,splitLayout(textInput("maturity_title",
                                                                             "Velg tittel til plot",
                                                                             "Forfall i CP/CD-markedet"),
                                                                   
                                                                   textInput("maturity_subtitle",
                                                                             "Velg undertittel til plot",
                                                                             "")))),
                                     br(),
                                     
                                     plotlyOutput("p_maturity"),
                                     selectInput("maturity_plot_type",
                                                 "Velg plot-type",
                                                 choices = c("Stacked area" = "stacked area",
                                                             "Linje" = "line",
                                                             "Stacked bar" = "stacked bar",
                                                             "Bar" = "bar"),
                                                 selected = "stacked bar"),
                                     
                                     selectInput("maturity_type",
                                                 "Velg type beregning",
                                                 choices = c("Volum per dag" = "volume",
                                                             "Kummulativt volum per dag" = "cummulative volume",
                                                             "Prosent av utestående per dag" = "percent of outstanding",
                                                             "Kummulativ prosent av utestående per dag" = "cummulative percent of outstanding"),
                                                 selected = "raw"),
                                     br(),
                                     
                                     checkboxInput("show_maturity_code",
                                                   "Vis kode bak plot",
                                                   value = F),
                                     
                                     conditionalPanel("input.show_maturity_code == 1",
                                                      tags$h5("Kode for å filtrere data:"),
                                                      verbatimTextOutput("data_maturities_code"),
                                                      tags$h5("Kode for å generere plot:"),
                                                      verbatimTextOutput("plot_code_maturities"))
                                     
                                     
                            )
                )
      )
    )
  )
  
  
  server <- function(input, output, session) {
    
    
    # loud_load = function() {
    # 
    #   cat("Laster data")
    # 
    #   #raw_data = readRDS("F:/MB/MOA/Likviditet/Analyser/Dataprosjekt/App/NoMaDataHub - Kjetil/data.rds")
    #   #raw_data = UpdateData("2021-09-01")
    # 
    #   raw_data = d
    #   
    #   cat("Data ferdig lastet")
    # 
    #   return(raw_data)
    # 
    # }
    
    
    
    
    
    
    
    data = reactive({
      
      raw_data %>% filter(TimeToMaturity >= MaxTimeToMaturity/3,
                          PrincipalAmount >= 1*10^6) %>%
        mutate(issuertype = if_else(is.na(d_bank), "Foretak", "Banker"),
               CountryCode = if_else(is.na(CountryCode), "Ikke spesifisert", CountryCode)
               #,est_rate = if_else(Yield == 0 | abs(Yield - InterestRate) > 3, InterestRate, Yield)
        )
      
    })
    
    data_filtered_by_issuertype = reactive({
      
      data() %>%
        filter(issuertype %in% input$choose_issuer_type)
      
    })
    
    output$choose_product_type <- renderUI({
      
      product_choices = sort(unique(data_filtered_by_issuertype()$ProductType), decreasing = T)
      
      checkboxGroupInput(
        "choose_product_type",
        "Velg produkttype",
        choices = product_choices,
        selected = product_choices,
        inline = T,
        width = NULL
      )
      
      
      
    })
    
    
    output$choose_maturities = renderUI({
      
      maturities = levels(data_filtered_by_issuertype()$Maturity)
      
      pickerInput("choose_maturities",
                  "Velg løpetider",
                  choices = maturities,
                  selected = maturities[!maturities %in% c("381-550 d", "550+ d")],
                  multiple = T,
                  options = pickerOptions(
                    `actions-box` = TRUE,
                    liveSearch = T,
                    liveSearchNormalize = T,
                    selectAllText = "Velg alle",
                    deselectAllText = "Dropp alle")
      )
      
    })
    
    
    
    
    output$choose_issuer_country <- renderUI({
      
      pickerInput("choose_issuer_country",
                  "Velg land på utsteder",
                  choices = sort(unique(data_filtered_by_issuertype()$CountryCode)),
                  selected = sort(unique(data_filtered_by_issuertype()$CountryCode)),
                  multiple = T,
                  options = pickerOptions(
                    `actions-box` = TRUE,
                    liveSearch = T,
                    liveSearchNormalize = T,
                    selectAllText = "Velg alle",
                    deselectAllText = "Dropp alle")
      )
      
      
      
    })
    
    
    data_filtered_by_producttype_and_country = reactive({
      
      data_filtered_by_issuertype() %>% filter(ProductType %in% input$choose_product_type,
                                               CountryCode %in% input$choose_issuer_country,
                                               InterestRateType %in% input$choose_interest_type)
      
      
    })
    
    
    output$choose_issuer <- renderUI({
      
      issuer_choices = sort(unique(data_filtered_by_producttype_and_country()[[input$choose_issuer_level]]))
      
      pickerInput("choose_issuer",
                  "Velg utsteder",
                  choices = issuer_choices,
                  selected = issuer_choices,
                  multiple = T,
                  options = pickerOptions(
                    `actions-box` = TRUE,
                    liveSearch = T,
                    liveSearchNormalize = T,
                    selectAllText = "Velg alle",
                    deselectAllText = "Dropp alle"))
      
      
    })
    
    
    
    selected_data = reactive({
      
      data_filtered_by_producttype_and_country()[
        data_filtered_by_producttype_and_country()[[input$choose_issuer_level]] %in% input$choose_issuer, ] %>%
        filter(ProductType %in% input$choose_product_type,
               InterestRateType %in% input$choose_interest_type)
      
    })
    
    
    
    output$p_issue = renderPlotly({
      
      #req(selected_data())
      
      selected_data() %>%
        filter(SettlementDate %in% seq(as.Date(input$issue_date[1]),
                                       as.Date(input$issue_date[2]), by = "day"),
               Maturity %in% input$choose_maturities) %>%
        group_by(SettlementDate, across(input$choose_sorting_var)) %>%
        summarise(PrincipalAmount = sum(PrincipalAmount)/10^9) %>%
        noma_tidy_plot(xcol = "SettlementDate",
                       ycol = "PrincipalAmount",
                       plot_type = "stacked bar",
                       sorting_var = input$choose_sorting_var,
                       yname = "Utstedt volum",
                       xname = "Dato",
                       plot_title = input$issue_title,
                       plot_subtitle = input$issue_subtitle,
                       colors = nb_colors)
      
      
    })
    
    
    
    
    
    
    output$data_issued_code <- renderPrint({
      expr(selected_data <- data %>% filter(TimeToMaturity >= max_maturity/3 ) %>%
             mutate(issuertype = if_else(is.na(d_bank), "Foretak", "Banker"),
                    CountryCode = if_else(is.na(CountryCode), "Ikke spesifisert", CountryCode)) %>%
             filter(SettlementDate %in% seq(as.Date(!!input$issue_date[1], origin = "1970-01-01 UTC"),
                                            as.Date(!!input$issue_date[2], origin = "1970-01-01 UTC"), by = "day"),
                    Maturity %in% !!input$choose_maturities) %>%
             filter(issuertype %in% !!input$choose_issuer_type,
                    CountryCode %in% !!input$choose_issuer_country,
                    ProductType %in% !!input$choose_product_type) %>%
             filter(get(!!input$choose_issuer_level) %in% !!input$choose_issuer) %>%
             group_by(SettlementDate, across(!!input$choose_sorting_var)) %>%
             summarise(PrincipalAmount = sum(PrincipalAmount)/10^9))
      
      
    })
    
    
    output$plot_code_issued = renderPrint({
      
      expr(plot <- selected_data %>%
             noma_tidy_plot(xcol = "SettlementDate",
                            ycol = "PrincipalAmount",
                            plot_type = "stacked bar",
                            sorting_var = !!input$choose_sorting_var,
                            yname = "Utstedt volum",
                            xname = "Dato",
                            plot_title = !!input$issue_title,
                            plot_subtitle = !!input$issue_subtitle,
                            colors = nb_colors))
      
      
      
    })
    
    
    
    output$p_outstanding = renderPlotly({
      
      req(selected_data())
      
      outstanding <- selected_data() %>% mutate(PrincipalAmount = PrincipalAmount/10^9) %>%
        filter(Maturity %in% input$choose_maturities) %>%
        calculate_outstanding("SettlementDate",
                              "MaturityDate",
                              "PrincipalAmount",
                              "grouping_variables" = c(input$choose_sorting_var),
                              freq = input$outstanding_frequency) %>%
        filter(date >= input$outstanding_date[1],
               date <= input$outstanding_date[2])
      
      outstanding %>%  noma_tidy_plot(xcol = "date",
                                      ycol = "outstanding",
                                      plot_type = input$outstanding_plot_type,
                                      sorting_var = input$choose_sorting_var,
                                      yname = "Utestående volum",
                                      xname = "Dato",
                                      plot_title = input$outstanding_title,
                                      plot_subtitle = input$outstanding_subtitle,
                                      colors = nb_colors,
                                      xrange = c(input$outstanding_date[1],
                                                 input$outstanding_date[2]),
                                      yrange = c(0, NULL))
      
      
      
    })
    
    output$data_outstanding_code = renderPrint({
      
      
      expr(selected_data <- data %>% filter(TimeToMaturity >= max_maturity/3 ) %>%
             mutate(issuertype = if_else(is.na(d_bank), "Foretak", "Banker"),
                    CountryCode = if_else(is.na(CountryCode), "Ikke spesifisert", CountryCode)) %>%
             filter(issuertype %in% !!input$choose_issuer_type,
                    CountryCode %in% !!input$choose_issuer_country,
                    ProductType %in% !!input$choose_product_type) %>%
             filter(get(!!input$choose_issuer_level) %in% !!input$choose_issuer) %>%
             mutate(PrincipalAmount = PrincipalAmount/10^9) %>%
             filter(Maturity %in% !!input$choose_maturities) %>%
             calculate_outstanding("SettlementDate",
                                   "MaturityDate",
                                   "PrincipalAmount",
                                   "grouping_variables" = c(!!input$choose_sorting_var),
                                   freq = !!input$outstanding_frequency) %>%
             filter(date >= !!input$outstanding_date[1],
                    date <= !!input$outstanding_date[2])
           
      )
      
    })
    
    
    output$plot_code_outstanding = renderPrint({
      
      expr(plot <- selected_data %>%
             noma_tidy_plot(xcol = "date",
                            ycol = "outstanding",
                            plot_type = !!input$outstanding_plot_type,
                            sorting_var = !!input$choose_sorting_var,
                            yname = "Utestående volum",
                            xname = "Dato",
                            plot_title = !!input$outstanding_title,
                            plot_subtitle = !!input$outstanding_subtitle,
                            colors = nb_colors,
                            xrange = c(as.Date(!!input$outstanding_date[1], origin = "1970-01-01 UTC") ,
                                       as.Date(!!input$outstanding_date[2], origin = "1970-01-01 UTC") )))
      
      
    })
    
    
    
    bloom_data = reactive({
      
      b = importer_bloomberg_data("2015-01-01",
                                  tickers = c(
                                    "us0001m index",
                                    "us0002m index",
                                    "us0003m index",
                                    "us0006m index",
                                    "dnorus1m index",
                                    "dnorus2m index",
                                    "dnorus3m index",
                                    "dnorus6m index",
                                    "bsby1m index",
                                    "bsby2m index",
                                    "bsby3m index",
                                    "bsby6m index",
                                    "usdra klmm curncy",
                                    "usdrb klmm curncy",
                                    "usdrc klmm curncy",
                                    "usdrf klmm curncy",
                                    "euswea icpl curncy",
                                    "eusweb icpl curncy",
                                    "euswec icpl curncy",
                                    "euswef icpl curncy",
                                    "eur1m bgnl curncy",
                                    "eur2m bgnl curncy",
                                    "eur3m bgnl curncy",
                                    "eur6m bgnl curncy",
                                    "eur bgnl curncy",
                                    "fdtr index",
                                    "ussoa bgnl curncy",
                                    "ussob bgnl curncy",
                                    "ussoc bgnl curncy",
                                    "ussof bgnl curncy",
                                    "irrbioer index"))
      
      b = b %>%
        mutate(eur1m_rdff = ((eur1m_bgnl_curncy_px_last/ 10000) / eur_bgnl_curncy_px_last / 30) * 360 * 100,
               eur2m_rdff = ((eur2m_bgnl_curncy_px_last/ 10000) / eur_bgnl_curncy_px_last / 60) * 360 * 100,
               eur3m_rdff = ((eur3m_bgnl_curncy_px_last/ 10000) / eur_bgnl_curncy_px_last / 90) * 360 * 100,
               eur6m_rdff = ((eur6m_bgnl_curncy_px_last/ 10000) / eur_bgnl_curncy_px_last / 180) * 360 * 100,
               eur_1m_ois_swapped = euswea_icpl_curncy_px_last + eur1m_rdff,
               eur_2m_ois_swapped = eusweb_icpl_curncy_px_last + eur2m_rdff,
               eur_3m_ois_swapped = euswec_icpl_curncy_px_last + eur3m_rdff,
               eur_6m_ois_swapped = euswef_icpl_curncy_px_last + eur6m_rdff)
      
      b
      
    })
    
    
    
    
    rates_plot = reactive({
      
      
      d = selected_data() %>% ungroup() %>%
        filter(SettlementDate %in% seq(as.Date(input$rates_date[1]),
                                       as.Date(input$rates_date[2]), by = "day"),
               Maturity %in% input$choose_maturities) %>%
        filter(abs(Yield) <= mean(Yield) + input$choose_filtering_level)
      
      d = d %>% noma_tidy_plot(xcol = "SettlementDate",
                               ycol = "Yield",
                               sorting_var = input$choose_sorting_var,
                               plot_type = "dot",
                               xname = "Dato",
                               yname = "Rente",
                               #yrange = c(min(d$InterestRate) - 0.2, mean(d$InterestRate) +2),
                               marker_size = "PrincipalAmount",
                               legend_options = list(showlegend = T),
                               sizename = "Utstedt volum (mill USD)",
                               plot_title = input$rates_title,
                               plot_subtitle = input$rates_subtitle,
                               colors = nb_colors,
                               xrange = c(input$rates_date[1], input$rates_date[2]),
                               custom_hover_text = paste("Utsteder: ", d$ParentName,
                                                         "<br> Yield:", round(d$Yield, 2),
                                                         "<br> Dato:", d$SettlementDate,
                                                         "<br> Orginal løpetid (dager):", d$OriginalMaturity,
                                                         "<br> Gjenværende løpetid (dager): ", d$TimeToMaturity,
                                                         "<br> Volum (mill usd):", round(d$PrincipalAmount/10^6,2),
                                                         "<br> Produkttype:", d$ProductType,
                                                         "<br> Cusip:", d$CUSIP,
                                                         "<br> Rente ved utstedelse:", d$InterestRate,
                                                         "<br> Første dato:", d$EstIssueDate))
      
      
      if(length(input$choose_index) == 0) {
        d = d
      } else{
        
        colors = sort(nb_colors, decreasing = T)
        
        for(i in seq_along(input$choose_index)){
          
          d = d %>% noma_add_line(bloom_data(), "date", input$choose_index[i], p_xcol = "SettlementDate",
                                  name =  names(bloom_choices)[bloom_choices == input$choose_index[i]],
                                  color = colors[i])
          
        }
        
        
      }
      
      d
      
      
    })
    
    
    
    
    output$p_rates = renderPlotly({
      
      rates_plot()
      
    })
    
    
    output$bloomberg_code = renderPrint({
      
      expr(bloom_data <- importer_bloomberg_data("2015-01-01",
                                                 tickers = c(
                                                   "us0001m index",
                                                   "us0002m index",
                                                   "us0003m index",
                                                   "us0006m index",
                                                   "dnorus1m index",
                                                   "dnorus2m index",
                                                   "dnorus3m index",
                                                   "dnorus6m index",
                                                   "bsby1m index",
                                                   "bsby2m index",
                                                   "bsby3m index",
                                                   "bsby6m index",
                                                   "usdra klmm curncy",
                                                   "usdrb klmm curncy",
                                                   "usdrc klmm curncy",
                                                   "usdrf klmm curncy",
                                                   "euswea icpl curncy",
                                                   "eusweb icpl curncy",
                                                   "euswec icpl curncy",
                                                   "euswef icpl curncy",
                                                   "eur1m bgnl curncy",
                                                   "eur2m bgnl curncy",
                                                   "eur3m bgnl curncy",
                                                   "eur6m bgnl curncy",
                                                   "eur bgnl curncy",
                                                   "fdtr index",
                                                   "ussoa bgnl curncy",
                                                   "ussob bgnl curncy",
                                                   "ussoc bgnl curncy",
                                                   "ussof bgnl curncy",
                                                   "irrbioer index")) %>%
             mutate(eur1m_rdff = ((eur1m_bgnl_curncy_px_last/ 10000) / eur_bgnl_curncy_px_last / 30) * 360 * 100,
                    eur2m_rdff = ((eur2m_bgnl_curncy_px_last/ 10000) / eur_bgnl_curncy_px_last / 60) * 360 * 100,
                    eur3m_rdff = ((eur3m_bgnl_curncy_px_last/ 10000) / eur_bgnl_curncy_px_last / 90) * 360 * 100,
                    eur6m_rdff = ((eur6m_bgnl_curncy_px_last/ 10000) / eur_bgnl_curncy_px_last / 180) * 360 * 100,
                    eur_1m_ois_swapped = euswea_icpl_curncy_px_last + eur1m_rdff,
                    eur_2m_ois_swapped = eusweb_icpl_curncy_px_last + eur2m_rdff,
                    eur_3m_ois_swapped = euswec_icpl_curncy_px_last + eur3m_rdff,
                    eur_6m_ois_swapped = euswef_icpl_curncy_px_last + eur6m_rdff))
      
      
      
      
      
    })
    
    output$data_rates_code = renderPrint({
      
      expr(selected_data <- data %>% filter(TimeToMaturity >= max_maturity/3 ) %>%
             mutate(issuertype = if_else(is.na(d_bank), "Foretak", "Banker"),
                    CountryCode = if_else(is.na(CountryCode), "Ikke spesifisert", CountryCode)) %>%
             filter(issuertype %in% !!input$choose_issuer_type,
                    CountryCode %in% !!input$choose_issuer_country,
                    ProductType %in% !!input$choose_product_type) %>%
             filter(get(!!input$choose_issuer_level) %in% !!input$choose_issuer) %>%
             filter(SettlementDate >= !!input$rates_date[1] & SettlementDate <= !!input$rates_date[2] ,
                    Maturity %in% !!input$choose_maturities) %>%
             filter(abs(Yield) <= mean(Yield) + !!input$choose_filtering_level)
           
      )
      
    })
    
    
    output$plot_code_rates = renderPrint({
      
      
      expr(plot <- selected_data %>%
             noma_tidy_plot(xcol = "SettlementDate",
                            ycol = "Yield",
                            sorting_var = !!input$choose_sorting_var,
                            plot_type = "dot",
                            xname = "Dato",
                            yname = "Rente",
                            #yrange = c(min(d$InterestRate) - 0.2, mean(d$InterestRate) +2),
                            marker_size = "PrincipalAmount",
                            legend_options = list(showlegend = T),
                            sizename = "Utstedt volum (mill USD)",
                            plot_title = !!input$rates_title,
                            plot_subtitle = !!input$rates_subtitle,
                            colors = nb_colors,
                            xrange = c(as.Date(!!input$rates_date[1], origin = "1970-01-01 UTC"),
                                       as.Date(!!input$rates_date[2], origin = "1970-01-01 UTC")),
                            custom_hover_text = paste("Utsteder: ", selected_data$ParentName,
                                                      "<br> Yield:", round(selected_data$Yield, 2),
                                                      "<br> Dato:", selected_data$SettlementDate,
                                                      "<br> Orginal løpetid (dager):", selected_data$OriginalMaturity,
                                                      "<br> Gjenværende løpetid (dager): ", selected_data$TimeToMaturity,
                                                      "<br> Volum (mill usd):", round(selected_data$PrincipalAmount/10^6,2),
                                                      "<br> Produkttype:", selected_data$ProductType,
                                                      "<br> Cusip:", selected_data$CUSIP,
                                                      "<br> Rente ved utstedelse:", selected_data$InterestRate,
                                                      "<br> Første dato:", selected_data$EstIssueDate)))
      
      
      
    })
    
    output$add_bloomberg_lines = renderPrint({
      
      
      cat(paste(exprs(
        
        
        bloom_choices <- c(
          "Libor 1m" =  "us0001m_index_px_last",
          "Libor 2m" =  "us0002m_index_px_last",
          "Libor 3m" = "us0003m_index_px_last",
          "Libor 6m" = "us0006m_index_px_last",
          "DNB CP-indeks 1m" = "dnorus1m_index_px_last",
          "DNB CP-indeks 2m" = "dnorus2m_index_px_last",
          "DNB CP-indeks 3m" =  "dnorus3m_index_px_last",
          "DNB CP-indeks 6m" =  "dnorus6m_index_px_last",
          "BSBY 1m" =  "bsby1m_index_px_last",
          "BSBY 2m" = "bsby2m_index_px_last",
          "BSBY 3m" = "bsby3m_index_px_last",
          "BSBY 6m" = "bsby6m_index_px_last",
          "Kliem 1m" = "usdra_klmm_curncy_px_last",
          "Kliem 2m" = "usdrb_klmm_curncy_px_last",
          "Kliem 3m" = "usdrc_klmm_curncy_px_last",
          "Kliem 6m" = "usdrf_klmm_curncy_px_last",
          "1m EUR OIS swappet til USD" = "eur_1m_ois_swapped",
          "2m EUR OIS swappet til USD" = "eur_2m_ois_swapped",
          "3m EUR OIS swappet til USD" = "eur_3m_ois_swapped",
          "6m EUR OIS swappet til USD" = "eur_6m_ois_swapped",
          "Styringsrente i USA" = "fdtr_index_px_last",
          "US OIS 1m" = "ussoa_bgnl_curncy_px_last",
          "US OIS 2m" = "ussob_bgnl_curncy_px_last",
          "US OIS 3m" = "ussoc_bgnl_curncy_px_last",
          "US OIS 6m" = "ussof_bgnl_curncy_px_last",
          "IOER" = "irrbioer_index_px_last"),
        
        colors <- sort(nb_colors, decreasing = T),
        
        for(i in seq_along(!!input$choose_index)) {
          
          plot <- plot %>% noma_add_line(bloom_data, 'date', (!!input$choose_index)[i], p_xcol = 'SettlementDate',
                                         name =  names(bloom_choices)[bloom_choices == (!!input$choose_index)[i]],
                                         color = colors[i])
          
        }
        
        
        
        
      )), sep = "\n\n")
      
    })
    
    
    
    output$p_maturity = renderPlotly({
      
      #req(selected_data())
      
      selected_data() %>%
        filter(#MaturityDate <= input$maturity_date[2],
          Maturity %in% input$choose_maturities) %>%
        mutate(PrincipalAmount = PrincipalAmount/10^9) %>%
        calculate_maturities(start_date = input$maturity_date[1],
                             "SettlementDate",
                             "MaturityDate",
                             "PrincipalAmount",
                             input$choose_sorting_var,
                             type = input$maturity_type) %>%
        filter(date <= input$maturity_date[2]) %>%
        noma_tidy_plot(xcol = "date",
                       ycol = maturities_names[[input$maturity_type]],
                       plot_type = input$maturity_plot_type,
                       sorting_var = input$choose_sorting_var,
                       yname = "Forfallende volum",
                       xname = "Dato",
                       plot_title = input$maturity_title,
                       plot_subtitle = input$maturity_subtitle,
                       colors = nb_colors,
                       xrange = c(input$maturities_date[1], input$maturites_date[2]))
      
      
      
    })
    
    
    
    
    
    
    output$data_maturities_code = renderPrint({
      
      
      expr(selected_data <- data %>% filter(TimeToMaturity >= max_maturity/3 ) %>%
             mutate(issuertype = if_else(is.na(d_bank), "Foretak", "Banker"),
                    CountryCode = if_else(is.na(CountryCode), "Ikke spesifisert", CountryCode)) %>%
             filter(issuertype %in% !!input$choose_issuer_type,
                    CountryCode %in% !!input$choose_issuer_country,
                    ProductType %in% !!input$choose_product_type) %>%
             filter(get(!!input$choose_issuer_level) %in% !!input$choose_issuer) %>%
             filter(Maturity %in% !!input$choose_maturities) %>%
             mutate(PrincipalAmount = PrincipalAmount/10^9) %>%
             calculate_maturities(start_date = !!input$maturity_date[1],
                                  "SettlementDate",
                                  "MaturityDate",
                                  "PrincipalAmount",
                                  !!input$choose_sorting_var,
                                  type = !!input$maturity_type) %>%
             filter(date <= !!input$maturity_date[2])
           
      )
      
      
      
      
    })
    
    
    output$plot_code_maturities = renderPrint({
      
      expr(plot <- selected_data %>%
             noma_tidy_plot(xcol = "date",
                            ycol = (!!maturities_names)[!!input$maturity_type],
                            plot_type = !!input$maturity_plot_type,
                            sorting_var = !!input$choose_sorting_var,
                            yname = "Forfallende volum",
                            xname = "Dato",
                            plot_title = !!input$maturity_title,
                            plot_subtitle = !!input$maturity_subtitle,
                            colors = nb_colors,
                            xrange = c(as.Date(!!input$maturities_date[1], origin = "1970-01-01 UTC"),
                                       as.Date(!!input$maturites_date[2], origin = "1970-01-01 UTC") )))
      
      
      
      
    })
    
    
    
    
  }
  
   app = shinyApp(ui, server)
  
  
   runApp(app, launch.browser = T)
   
}


