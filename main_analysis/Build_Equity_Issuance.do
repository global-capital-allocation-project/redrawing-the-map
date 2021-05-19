* ---------------------------------------------------------------------------------------------------
* Build_Equity_Issuance: This job produces the internal security-level equity issuance data masterfile
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Build_Equity_Issuance, replace

* ---------------------------------------------------------------------------------------------------
* Worldscope equities: keeping variables of interest
* ---------------------------------------------------------------------------------------------------

* creating auxiliar table with ADR ratios
use $raw/worldscope/wsndata.dta, clear
keep if item == 5577
keep code date value
duplicates drop
save $cmns1/temp/wsndata_ratio.dta, replace

* import worldscope stocks data: keeping variables of interest
use $raw/worldscope/wrds_ws_stock.dta, clear
display c(k)
drop freq

mmerge code using $cmns1/temp/wsndata_ratio.dta
drop if _merge == 2
rename value ADR_ratio
rename date ADR_date

* identifiers
rename item5601 ticker
rename item6004 cusip
rename item6006 sedol
rename item6008 isin
rename item6035 ws_identifier
rename item6105 ws_perm_id
rename item6038 ibes_id

order ticker cusip isin sedol ws_identifier ws_perm_id ibes_id ADR_ratio ADR_date, before(year_)
gen exchange_listed_code = length(item5427)
keep code ticker cusip isin sedol ws_identifier ws_perm_id year_ item8001 item5001 item5651 item5450 item5301 item5427 exchange_listed_code ADR_ratio ADR_date

rename item8001 market_cap_LC
rename item5001 mkt_price
rename item5651 common_shares_traded
rename item5450 shareholders 
rename item5301 common_shares_out
rename item5427 exchange_listed

* corrections
replace common_shares_out = 495311083 if isin == "US7156841063" & (year == 2017)
replace market_cap_LC = common_shares_out*mkt_price if isin == "US7156841063" & (year == 2017)
replace market_cap_LC = market_cap_LC/1000 if isin == "BRBSCTACNOR1" & inlist(year,2006,2007)

save $cmns1/temp/equity_issuance/ws_stocks_temp.dta, replace

* ---------------------------------------------------------------------------------------------------
* Worldscope fundamentals: company info
* ---------------------------------------------------------------------------------------------------

* import worldscope fundamentals
cap restore
use $cmns1/temp/equity_issuance/ws_stocks_temp.dta, clear
drop if market_cap_LC == .
duplicates drop
drop if missing(ws_identifier)
mmerge ws_identifier year_ using $raw/worldscope/wrds_ws_fundamentals_annual.dta, umatch(item6035 year_) ukeep(item6001 item6027 item6100 item7210 item6099 item6026)
drop if _merge != 3

drop _merge

rename item6027 nation_code
rename item6100 entity_type
drop if market_cap_LC <= 0
gsort -market_cap_LC

drop sedol item7210
rename item6099 currency
rename item6001 company_name
rename item6026 nation
drop nation

mmerge nation_code using $raw/worldscope/ws_countries.dta

replace nation = "russian federation" if nation == "Russia"
replace nation = lower(nation)
keep if _merge == 3 | _merge == 1

gen month = 12
gen date = mdy(month,1,year)
format %td date

mmerge nation using $raw/Factset/fds_stata/country_map.dta, umatch(country_desc) ukeep(iso_currency)
drop if _merge == 2
tab nation if _merge != 3, sort mi
keep if _merge == 3
replace iso_currency = currency if iso_currency == ""
drop currency

gen date_m = mofd(date)
format %tm date_m
mmerge iso_currency date_m using $cmns1/exchange_rates/IFS_ERdata.dta, umatch(iso_currency_code date_m)
format lcu_per %9.0g
drop if _merge != 3
drop _merge
gen marketcap_usd = market_cap_LC / lcu_per_usd
gsort -marketcap_usd

drop if iso_code == "ZWE"
drop if iso_code == "IDN"
drop if iso_code == ""
duplicates drop
drop if market_cap_LC == .
replace marketcap_usd = marketcap_usd / 1e12
gsort -marketcap_usd
duplicates drop
save $cmns1/temp/equity_issuance/worldscope_equity.dta, replace

* ---------------------------------------------------------------------------------------------------
* Auxiliary table: checking if we have ETFs in Worldscope
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/equity_issuance/worldscope_equity.dta, clear
drop if isin == ""
duplicates drop
mmerge isin using $raw/figi/figi_master_compact_isin_unique.dta, ukeep(marketsector securitytype securitytype2) unmatched(m)

keep if _merge == 3
drop if securitytype2 == "Common Stock" | securitytype2 == "Depositary Receipt" 
keep isin
save $cmns1/temp/equity_issuance/isin_non_common_stock.dta, replace

use $cmns1/temp/equity_issuance/worldscope_equity.dta, clear
keep cusip
drop if cusip == ""
duplicates drop
mmerge cusip using $raw/figi/figi_master_compact_cusip_unique.dta, ukeep(marketsector securitytype securitytype2) unmatched(m)

keep if _merge == 3
drop if securitytype2 == "Common Stock" | securitytype2 == "Depositary Receipt" 
keep cusip
save $cmns1/temp/equity_issuance/cusip_non_common_stock.dta, replace

* ---------------------------------------------------------------------------------------------------
* Keeping common stocks and ADRs only
* ---------------------------------------------------------------------------------------------------

cap restore
use $cmns1/temp/equity_issuance/worldscope_equity.dta, clear
drop common_shares_traded shareholders
duplicates drop
mmerge isin using $cmns1/temp/equity_issuance/isin_non_common_stock.dta
drop if _merge == 2 | _merge == 3

mmerge cusip using $cmns1/temp/equity_issuance/cusip_non_common_stock.dta
drop if _merge == 2 | _merge == 3
drop _merge
save $cmns1/temp/equity_issuance/worldscope_equity_m.dta, replace

* ---------------------------------------------------------------------------------------------------
* Creating security-level file
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/equity_issuance/worldscope_equity_m.dta, clear
gen _company_name = subinstr(company_name,"- ADR","",.)
replace _company_name = trim(_company_name)
drop company_name
rename _company_name company_name
drop if isin == "" & cusip == ""

* initially ignore ADRS
drop if entity_type == "A"

gen aux = 1
bys company_name year: egen n_S=total(aux) if entity_type == "S"
bys company_name year: egen n_C=total(aux) if entity_type == "C"

sort company_name year n_C
by company_name year: replace n_C = n_C[1] if n_C == .
sort company_name year n_S
by company_name year: replace n_S = n_S[1] if n_S == .

replace n_C = 0 if n_C == .
replace n_S = 0 if n_S == .
gen tot_N =  n_S + n_C
replace entity_type = "S" if tot_N == 1 & n_C == 1 & entity_type == "C"

preserve 
keep if entity_type == "C"
save $cmns1/temp/equity_issuance/ws_entity_type_C.dta, replace
restore

drop if entity_type == "C"
mmerge isin year using $cmns1/temp/equity_issuance/ws_entity_type_C.dta, umatch(isin year) uname(C_)
replace ticker = C_ticker if _merge == 2
replace cusip = C_cusip if _merge == 2
replace market_cap_LC = C_market_cap_LC if _merge == 2
replace entity_type = "S" if _merge == 2
replace iso_code = C_iso_code if _merge == 2
replace iso_currency = C_iso_currency if _merge == 2
replace lcu_per_usd = C_lcu_per_usd if _merge == 2
replace marketcap_usd = C_marketcap_usd if _merge == 2
replace company_name = C_company_name if _merge == 2
drop aux-C_tot_N
save $cmns1/equity_issuance_master/equity_issuance_no_adr.dta, replace

* ---------------------------------------------------------------------------------------------------
* Aggregate market cap and benchmarking with WFE
* ---------------------------------------------------------------------------------------------------

* ADRs in the security-level file
use $cmns1/temp/equity_issuance/worldscope_equity_m.dta, clear
keep if entity_type == "A"
gen _company_name = subinstr(company_name,"- ADR","",.)
replace _company_name = trim(_company_name)
drop company_name
rename _company_name company_name

append using $cmns1/equity_issuance_master/equity_issuance_no_adr.dta

gsort year company_name entity_type marketcap_usd
by year company_name entity_type: gen aux3 = _N if entity_type == "A"
by year company_name entity_type: gen aux4 = _n if entity_type == "A"

gsort year company_name -entity_type marketcap_usd
by year company_name: gen aux = _N
by year company_name: gen aux2 = _n
by year company_name: gen aux_adr = 1 if entity_type[1] == "A"

keep if (entity_type != "A") | (aux_adr == 1)
gsort -marketcap_usd
keep ticker cusip isin year company_name entity_type iso_code marketcap_usd common_shares_out
save $cmns1/temp/equity_issuance_master.dta, replace

* Getting CUSIPs since many are missing
cap restore
use $cmns1/temp/equity_issuance_master.dta, clear
drop entity_type
mmerge isin using $cmns1/temp/cgs/isin_to_cusip, umatch(isin) uname(master_)
drop if _merge ==2
replace cusip = master_cusip if cusip == ""
drop master_cusip
mmerge isin using $cmns1/temp/cgs/isin_to_cusip, uname(master_)
drop if _merge ==2

replace cusip = master_cusip if cusip == ""
drop master_cusip
save $cmns1/temp/equity_issuance/ws_equity_security.dta, replace

gen has_cusip = 0
replace has_cusip = 1 if cusip != ""
tabstat marketcap_usd, s(sum) by(has_cusip)

preserve 
keep if has_cusip == 0
drop _merge has_cusip
drop year marketcap_usd
duplicates drop
save $cmns1/temp/isin_no_cusip_equity.dta, replace
restore

drop _merge
gen cusip6 = substr(cusip,1,6)
mmerge cusip6 using $cmns1/country_master/cmns_aggregation, umatch(issuer_number) ukeep(cgs_domicile country_bg)
drop if _merge == 2
tabstat marketcap_usd, s(sum) by(_merge)
gsort -year -marketcap_usd
keep ticker cusip isin year marketcap_usd company_name cusip6 cgs_domicile country_bg iso_code
rename year_ year 

* ---------------------------------------------------------------------------------------------------
* Getting parent information
* ---------------------------------------------------------------------------------------------------

replace cusip6 = substr(cusip,1,6) if cusip6 == ""
qui mmerge cusip6 using $cmns1/country_master/cmns_aggregation, umatch(issuer_number) ukeep(cusip6_up_bg issuer_name_up)
drop if _merge == 2
drop _merge

order year, first
gsort -marketcap_usd

replace cgs_domicile = "ARE" if cgs_domicile == "UAE"
replace country_bg = "ARE" if country_bg == "UAE"
replace cgs_domicile = "ROU" if cgs_domicile == "ROM"
replace country_bg = "ROU" if country_bg == "ROM"

drop if country_bg == "YUG" | cgs_domicile == "YUG"
drop if country_bg == "ANX" | cgs_domicile == "SU"
drop if country_bg == "IOM" | cgs_domicile == "IOM"
drop if country_bg == "CHI" | cgs_domicile == "CHI"
duplicates drop

preserve
collapse (sum) marketcap_usd, by(year iso_code)
drop if marketcap_usd == .
drop if iso_code == ""
save $cmns1/temp/market_cap_local_markets.dta, replace
restore

preserve 
keep year iso_code cusip6
duplicates drop
bys year iso_code: gen n_companies = _N
drop cusip6
duplicates drop
save $cmns1/temp/ws_number_companies.dta, replace
restore

preserve
use $cmns1/temp/market_cap_local_markets.dta, clear
mmerge year iso_code using $cmns1/temp/ws_number_companies.dta
drop _merge
save $cmns1/temp/ws_local_markets.dta, replace
restore
save $cmns1/equity_issuance_master/equity_issuance_master.dta, replace

log close
