library(arrow)
library(dplyr)

con <- DBI::dbConnect(odbc::odbc(), 
                      .connection_string = paste0(
                        "Driver={Microsoft Access Driver (*.mdb, *.accdb)};", 
                        "DBQ=data-raw/Waterplanten en Waterkwaliteit 2015-2021_def.accdb;" 
                      ))

tabellen_raw <- DBI::dbListTables(con)

tabellen <- tabellen_raw[stringr::str_detect(tabellen_raw, "^MSys", negate = TRUE)]

for (tabelnaam in tabellen){

# tabelnaam <- "Vegetation data"
tabel <- dplyr::tbl(con, tabelnaam)

tabelnaam2 <- stringr::str_remove(tabelnaam, ":")

tabel %>% 
  collect() %>% 
  rename_all(stringr::str_to_lower) %>% 
  write_parquet(paste0("data-raw/parquet/",tabelnaam2, ".parquet"))


}
DBI::dbDisconnect(con)

# open_dataset("data-raw/Vegetation data.parquet") %>% collect() %>% View()
