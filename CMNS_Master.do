* ---------------------------------------------------------------------------------------------------
* CMNS_Master.do: This file dispatches Stata jobs: please execute CMNS_Master.sh in order to run all
* the jobs in this replication package in order
* ---------------------------------------------------------------------------------------------------
clear
version 15
set more off
set excelxlsxlargefile on

* ---------------------------------------------------------------------------------------------------
* Core globals
* ---------------------------------------------------------------------------------------------------

* List of countries classified as tax havens
global tax_haven_1 `""ABW","AIA","AND","ANT","ATG","BHR","BHS" "'
global tax_haven_2 `""BLZ","BMU","BRB","COK","CRI","CUW","CYM" "'
global tax_haven_3 `""CYP","DJI","DMA","FSM","GGY","GIB","GRD" "'
global tax_haven_4 `""HKG","IMN","IRL","JEY","JOR","KNA","LBN" "'
global tax_haven_5 `""LBR","LCA","LIE","LUX","MAC","MAF","MCO" "'
global tax_haven_6 `""MDV","MHL","MLT","MSR","MUS","NIU","NLD" "'
global tax_haven_7 `""NRU","PAN","SMR","SYC","TCA","TON","VCT" "'
global tax_haven_8 `""VGB","VUT","WSM","SGP" "'

* List of EU countries
global  eu1  `""LUX","IRL","ITA","DEU","FRA","ESP","GRC","NLD","AUT" "'
global  eu2  `""BEL","FIN","PRT","CYP","EST","LAT","LTU","SVK","SVN" "'
global  eu3  `""MLT","EMU","LVA" "'

* Root paths
global cmns1_code "<CODE_PATH>"
global data_path "<DATA_PATH>"

* Other core paths
global raw "$data_path/raw"
global temp "$cmns1/temp"
global scratch "$cmns1/temp/scratch"
global cmns1 "$data_path/cmns1"

* ---------------------------------------------------------------------------------------------------
* Install required Stata packages
* ---------------------------------------------------------------------------------------------------

cap ssc install carryforward
cap ssc install distinct
cap ssc install egenmore
cap ssc install freduse
cap ssc install fs
cap ssc install missings
cap ssc install mmerge
cap ssc install stcmd
cap ssc install sxpose
cap ssc install tabout
cap ssc install unique
cap ssc install labutil
cap ssc install rsource
cap ssc install margeff
cap ssc install winsor
cap ssc install moremata
cap ssc install ftools
cap ssc install carryforward
cap ssc install texsave
cap ssc install jarowinkler

local github "https://raw.githubusercontent.com"
cap net from http://www-personal.umich.edu/~nwasi/programs
cap net install gtools, from(`github'/mcaceresb/stata-gtools/master/build/)

* ---------------------------------------------------------------------------------------------------
* Create folder structure
* ---------------------------------------------------------------------------------------------------

global sales $cmns1/temp/sales
global sales_matrices $cmns1/temp/sales/matrices
global sdc $raw/SDC
global sdc_bonds $raw/SDC/bonds
global sdc_temp $cmns1/temp/sdc
global sdc_additional $sdc_temp/additional
global sdc_dta $sdc_temp/dta
global sdc_datasets $sdc_temp/datasets
global sdc_equities $raw/SDC/equities
global sdc_eqdta $sdc_temp/eqdta
global sdc_loans $raw/SDC/loans
global sdc_loandta $sdc_temp/loandta

cap mkdir $cmns1/country_master
cap mkdir $cmns1/holdings_master
cap mkdir $cmns1/industry_master
cap mkdir $cmns1/issuance_master
cap mkdir $cmns1/graphs
cap mkdir $cmns1/logs
cap mkdir $cmns1/tables
cap mkdir $cmns1/temp
cap mkdir $cmns1/temp/aggregation_sources
cap mkdir $cmns1/aggregation_sources
cap mkdir $cmns1/temp/dealogic
cap mkdir $cmns1/temp/equity_issuance
cap mkdir $cmns1/equity_issuance_master
cap mkdir $cmns1/temp/ER_Data
cap mkdir $cmns1/exchange_rates
cap mkdir $cmns1/temp/norway_swf
cap mkdir $cmns1/temp/norway_swf/swf_matrices
cap mkdir $cmns1/temp/vie_consolidate
cap mkdir $cmns1/china_master
cap mkdir $cmns1/alternative_restatements
cap mkdir $cmns1/temp/scratch
cap mkdir $cmns1/temp/fund_shares_corrections
cap mkdir $cmns1/temp/th_home_bias
cap mkdir $cmns1/temp/th_home_bias/regressions
cap mkdir $cmns1/temp/cgs
cap mkdir $cmns1/temp/factset
cap mkdir $cmns1/temp/morningstar_country
cap mkdir $cmns1/temp/orbis
cap mkdir $cmns1/temp/orbis/compact
cap mkdir $cmns1/temp/orbis/country

cap mkdir $cmns1/temp/tic_data
cap mkdir $cmns1/temp/tic_disaggregated
cap mkdir $cmns1/temp/tic_disaggregated/equity
cap mkdir $cmns1/temp/tic_disaggregated/private_debt
cap mkdir $cmns1/temp/tic_disaggregated/abs
cap mkdir $cmns1/temp/tic_disaggregated/currency
cap mkdir $cmns1/temp/compustat
cap mkdir $cmns1/temp/factset
cap mkdir $cmns1/temp/industry
cap mkdir $cmns1/temp/industry/ciq
cap mkdir $cmns1/industry_master

cap mkdir $cmns1/issuance_based_matrices
cap mkdir $cmns1/issuance_based_matrices/Corporate_Bonds_xls
cap mkdir $cmns1/issuance_based_matrices/Corporate_Bonds_dta
cap mkdir $cmns1/issuance_based_matrices/All_Bonds_xls
cap mkdir $cmns1/issuance_based_matrices/All_Bonds_dta
cap mkdir $cmns1/issuance_based_matrices/Equity_xls
cap mkdir $cmns1/issuance_based_matrices/Equity_dta
cap mkdir $cmns1/tables/issuer_reallocations
cap mkdir $cmns1/tables/issuer_reallocations/tex

cap mkdir $sales
cap mkdir $sales_matrices
cap mkdir $sdc_temp
cap mkdir $sdc_additional
cap mkdir $sdc_dta
cap mkdir $sdc_datasets
cap mkdir $sdc_eqdta
cap mkdir $sdc_loandta

cap mkdir $cmns1/temp/security_master
cap mkdir $cmns1/security_master
cap mkdir $cmns1/temp/mns_holdings
cap mkdir $cmns1/temp/mns_holdings

cap mkdir $cmns1/temp/revision_estimates
cap mkdir $cmns1/reallocation_matrices
cap mkdir $temp/insurance
cap mkdir $cmns1/holdings_based_restatements

cap mkdir $cmns1/insurance
cap mkdir $cmns1/insurance/temp
cap mkdir $cmns1/insurance/temp/market_shares
cap mkdir $cmns1/insurance/reallocation_matrices

cap mkdir $cmns1/insurance/temp/life
cap mkdir $cmns1/insurance/temp/life/bonds
cap mkdir $cmns1/insurance/temp/life/common_equities
cap mkdir $cmns1/insurance/temp/life/preferred_equities
cap mkdir $cmns1/insurance/temp/life/other
cap mkdir $cmns1/insurance/temp/life/loans

cap mkdir $cmns1/insurance/temp/health
cap mkdir $cmns1/insurance/temp/health/bonds
cap mkdir $cmns1/insurance/temp/health/common_equities
cap mkdir $cmns1/insurance/temp/health/preferred_equities
cap mkdir $cmns1/insurance/temp/health/other
cap mkdir $cmns1/insurance/temp/health/loans

cap mkdir $cmns1/insurance/temp/pc
cap mkdir $cmns1/insurance/temp/pc/bonds
cap mkdir $cmns1/insurance/temp/pc/common_equities
cap mkdir $cmns1/insurance/temp/pc/preferred_equities
cap mkdir $cmns1/insurance/temp/pc/other
cap mkdir $cmns1/insurance/temp/pc/loans

* ---------------------------------------------------------------------------------------------------
* Job execution: aggregation data sources
* ---------------------------------------------------------------------------------------------------

if "`2'"=="Process_CGS" {
	do $cmns1_code/aggregation_data_sources/CGS.do
}

if "`2'"=="Process_Morningstar_Country" {
	do $cmns1_code/aggregation_data_sources/Morningstar_Country.do
}

if "`2'"=="Process_Dealogic" {
	do $cmns1_code/aggregation_data_sources/Dealogic.do
}

if "`2'"=="Process_SDC" {
	do $cmns1_code/aggregation_data_sources/SDC.do
}

if "`2'"=="Process_CIQ" {
	do $cmns1_code/aggregation_data_sources/Capital_IQ.do
}

if "`2'"=="Process_Factset" {
	do $cmns1_code/aggregation_data_sources/Factset.do
}

if "`2'"=="Process_Orbis" {
	do $cmns1_code/aggregation_data_sources/Orbis.do
}

* ---------------------------------------------------------------------------------------------------
* Job execution: further data source build
* ---------------------------------------------------------------------------------------------------

if "`2'"=="Import_Insurance_Data" {
	do $cmns1_code/main_analysis/Import_Insurance_Data.do
}

if "`2'"=="Morningstar_Asset_Classes" {
	do $cmns1_code/main_analysis/Morningstar_Asset_Classes.do
}

if "`2'"=="Security_Master" {
	do $cmns1_code/main_analysis/Security_Master.do
}

if "`2'"=="Build_Exchange_Rates" {
	do $cmns1_code/main_analysis/Build_Exchange_Rates.do
}

if "`2'"=="Build_Dealogic_Master" {
	do $cmns1_code/main_analysis/Build_Dealogic_Master.do
}

if "`2'"=="Build_Factset_Master" {
	do $cmns1_code/main_analysis/Build_Factset_Master.do
}

if "`2'"=="Build_Bond_Issuance" {
	do $cmns1_code/main_analysis/Build_Bond_Issuance.do
}

if "`2'"=="Build_Equity_Issuance" {
	do $cmns1_code/main_analysis/Build_Equity_Issuance.do
}

if "`2'"=="Import_TIC" {
	do $cmns1_code/main_analysis/Import_TIC.do 
}

if "`2'"=="Import_CPIS" {
	do $cmns1_code/main_analysis/Import_CPIS.do 
}

if "`2'"=="Process_GeoRev" {
	do $cmns1_code/main_analysis/Process_GeoRev.do 
}

if "`2'"=="Process_Insurance_Data" {
	do $cmns1_code/main_analysis/Process_Insurance_Data.do 
}

if "`2'"=="Build_Norway_SWF_Data_1" {
	do $cmns1_code/main_analysis/Build_Norway_SWF_Data_1.do 
}

if "`2'"=="Build_Norway_SWF_Data_3" {
	do $cmns1_code/main_analysis/Build_Norway_SWF_Data_3.do 
}

* ---------------------------------------------------------------------------------------------------
* Job execution: analysis
* ---------------------------------------------------------------------------------------------------

if "`2'"=="Summary_Data" {
	do $cmns1_code/main_analysis/Summary_Data.do
}

if "`2'"=="Merge_TIC_CPIS" {
	do $cmns1_code/main_analysis/Merge_TIC_CPIS.do 
}

if "`2'"=="Reallocation_Matrices" {
	do $cmns1_code/main_analysis/Reallocation_Matrices.do 
}

if "`2'"=="Fund_Shares_Estimation" {
	do $cmns1_code/main_analysis/Fund_Shares_Estimation.do 
}

if "`2'"=="Issuance_Distribution_Matrices" {
	do $cmns1_code/main_analysis/Issuance_Distribution_Matrices.do 
}

if "`2'"=="Restated_TIC_CPIS" {
	do $cmns1_code/main_analysis/Restated_TIC_CPIS.do 
}

if "`2'"=="China_VIE_NFA" {
	do $cmns1_code/main_analysis/China_VIE_NFA.do 
}

if "`2'"=="Cross_Country_Graphs" {
	do $cmns1_code/main_analysis/Cross_Country_Graphs.do 
}

if "`2'"=="Sales_Analysis" {
	do $cmns1_code/main_analysis/Sales_Analysis.do 
}

if "`2'"=="Credit_Parent_Analysis" {
	do $cmns1_code/main_analysis/Credit_Parent_Analysis.do 
}

if "`2'"=="Restatement_Tables" {
	do $cmns1_code/main_analysis/Restatement_Tables.do 
}

if "`2'"=="Firm_Level_Tables" {
	do $cmns1_code/main_analysis/Firm_Level_Tables.do 
}

if "`2'"=="Representativeness_Analysis" {
	do $cmns1_code/main_analysis/Representativeness_Analysis.do 
}

if "`2'"=="Home_Bias" {
	do $cmns1_code/main_analysis/Home_Bias.do 
}

if "`2'"=="Currency_Analysis" {
	do $cmns1_code/main_analysis/Currency_Analysis.do 
}
