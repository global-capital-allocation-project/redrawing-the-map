* ---------------------------------------------------------------------------------------------------
* China_VIE_NFA: This job carries out the analysis of China's VIEs and their impact on China's net
* foreign asset positions, and produces the corresponding figures and tables
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/China_VIE_NFA, replace

* ---------------------------------------------------------------------------------------------------
* Import CDIS data for FDI
* ---------------------------------------------------------------------------------------------------

import delimited "$raw/CDIS/CDIS_11-13-2019 16-40-47-29_timeSeries.csv", clear 
kountry countrycode, from(imfn) to(iso3c)
drop countrycode
rename _ISO3C_ codei
kountry counterpartcountrycode, from(imfn) to(iso3c)
rename _ISO3C_ codej
drop counterpartcountrycode
rename ïcountryname countryname
mmerge countryname using $raw/CDIS/cdis_code_list, umatch(country) ukeep(wbcode)
replace codei=wbcode if codei==""
drop wbcode
drop if _merge==2
mmerge counterpartcountryname using $raw/CDIS/cdis_code_list, umatch(country) ukeep(wbcode)
replace codej=wbcode if codej==""
drop wbcode
drop if _merge==2
replace codei="VGB" if countryname=="Virgin Islands, British"
replace codei="VIR" if countryname=="US Virgin Islands"
replace codej="VGB" if counterpartcountryname=="Virgin Islands, British"
replace codej="VIR" if counterpartcountryname=="US Virgin Islands"
replace codej="WLD" if counterpartcountryname=="World"
replace codej="NSP" if regexm(counterpartcountryname,"Not Specified")==1
drop countryname counterpartcountryname 

cap drop v18
replace indicatorcode=subinstr(indicatorcode,"_BP6_USD","",.)
split indicatorcode, p("_")
gen measure="direct" if indicatorcode2==""
replace measure="derived" if indicatorcode2=="DV"
drop indicatorcode indicatorcode2
rename indicatorcode indicatorcode
order indicatorcode

* IIWF = Inward/Outward Direct Investment Positions (Net) with Fellow Enterprises
drop if indicatorcode=="IIWF" | indicatorcode=="IOWF"

* Outward/Inward Debt Positions (Net): Resident Enterprises that are not Financial Intermediaries
drop if indicatorcode=="IOWDN" | indicatorcode=="IIWDN"

* Outward Direct Investment Liabilities/Assets Positions (Gross) with Fellow Enterprises
drop if indicatorcode=="IOWFL" | indicatorcode=="IOWFA" 

* Only keep gross
drop if regexm(indicatorname,"(Net)")==1

* Classify direction
gen direction="in"
replace direction="out" if regexm(indicatorname,"Outward")==1
drop _merge
keep if indicatorcode=="IIW"
drop if codei=="" | codej==""
drop indicatorcode indicatorname direction
reshape long v, i(attribute codei codej measure) j(year)
replace year=year+2000
rename v value
destring value, replace force
replace value=value/(10^9)
drop if attribute=="Status"
drop att
reshape wide value, i(codei codej year) j(measure) str
renpfix value
save $cmns1/temp/cdis_iiw_long.dta, replace

* ---------------------------------------------------------------------------------------------------
* Read and merge data from Bloomberg and Factset on VIE market capitalizations and prices
* ---------------------------------------------------------------------------------------------------

* Static fields from Bloomberg
import excel using $raw/Bloomberg/vie_bloomberg.xlsx, clear sheet("static") firstrow
save $cmns1/temp/vie_consolidate/bloomberg_static, replace

* Static fields from Factset
import excel using $raw/Factset/workstation/vie_factset.xlsx, clear sheet("static") firstrow
save $cmns1/temp/vie_consolidate/factset_static, replace

* Dynamic fields from Bloomberg: market cap
import excel using $raw/Bloomberg/vie_bloomberg.xlsx, clear sheet("market_cap")
foreach var of varlist * {
    cap replace `var' = "cusip" + `var' if _n == 1
}
foreach var of varlist * {
     local try = strtoname(strtrim(`var'[1]))
     rename `var'  `try'
}
drop if _n == 1
rename cusipdate date
reshape long cusip, i(date) j(marketcap) string
rename cusip market_cap
rename marketcap cusip
replace market_cap = "" if market_cap == "#N/A Invalid Security"
cap drop _date
destring date, force replace
format %td date
replace date = date - 21916
destring market_cap, force replace
gen source = "bbg"
cap drop _date
gen _date = qofd(date)
format %tq _date
drop date
rename _date date
save $cmns1/temp/vie_consolidate/bloomberg_marketcap, replace

* Dynamic fields from Bloomberg: prices
import excel using $raw/Bloomberg/vie_bloomberg.xlsx, clear sheet("price")
foreach var of varlist * {
    cap replace `var' = "cusip" + `var' if _n == 1
}
foreach var of varlist * {
     local try = strtoname(strtrim(`var'[1]))
     rename `var'  `try'
}
drop if _n == 1
rename cusipdate date
reshape long cusip, i(date) j(_price) string
rename cusip price
rename _price cusip
replace price = "" if price == "#N/A Invalid Security"
cap drop _date
destring date, force replace
format %td date
replace date = date - 21916
destring price, force replace
gen source = "bbg"
cap drop _date
gen _date = qofd(date)
format %tq _date
drop date
rename _date date
save $cmns1/temp/vie_consolidate/bloomberg_price, replace

* Dynamic fields from Bloomberg: market cap
import excel using $raw/Factset/workstation/vie_factset.xlsx, clear sheet("market_cap") firstrow
cap drop cusip6_up_bg issuer_name_up
reshape long value_, i(cusip) j(date) string
cap drop _date
gen _date = quarterly(date, "YQ")
format %tq _date
drop date
rename _date date
rename value_ market_cap
gen source = "factset"
save $cmns1/temp/vie_consolidate/factset_marketcap, replace

* Dynamic fields from Bloomberg: price
import excel using $raw/Factset/workstation/vie_factset.xlsx, clear sheet("price") firstrow
cap drop cusip6_up_bg issuer_name_up
reshape long value_, i(cusip) j(date) string
cap drop _date
gen _date = quarterly(date, "YQ")
format %tq _date
drop date
rename _date date
rename value_ price
gen source = "factset"
save $cmns1/temp/vie_consolidate/factset_price, replace

* ---------------------------------------------------------------------------------------------------
* Consolidate VIE static data from Bloomberg and Factset
* ---------------------------------------------------------------------------------------------------

* Merge static data from Bloomberg and Factset
use $cmns1/temp/vie_consolidate/bloomberg_static.dta, clear
mmerge cusip using $cmns1/temp/vie_consolidate/factset_static.dta, unmatched(b) uname(f_)

* Harmonize fields: IPO date
cap drop _ipo_date
gen _ipo_date = date(ipo_date, "MDY")
format %td _ipo_date
order ipo_date _ipo_date
drop ipo_date
rename _ipo_date ipo_date
cap drop _f_ipo_date
gen _f_ipo_date = date(f_ipo_date, "MDY")
format %td _f_ipo_date
order f_ipo_date _f_ipo_date
drop f_ipo_date
rename _f_ipo_date f_ipo_date

* Harmonize fields: IPO prices, shares offered
order f_ipo_price ipo_price
order f_ipo_shares_offered ipo_shares_offered
destring ipo_price, force replace
destring f_ipo_price, force replace
destring ipo_shares_offered, force replace
destring f_ipo_shares_offered, force replace

* Harmonize fields: other
replace currrency_iso_code = "" if currrency_iso_code == "#N/A Invalid Security"
rename currrency_iso_code currency_iso_code
cap drop f_cusip6_up_bg f_issuer_name_up
drop f_currency_iso_code
drop _merge

* Merge in exchange rates and convert everything to USD
gen month = mofd(ipo_date)
format %tm month
mmerge month currency_iso_code using $cmns1/exchange_rates/IFS_ERdata.dta, umatch(date_m iso_currency_code) unmatched(m)
replace ipo_price = . if currency_iso_code != "USD" & ~missing(currency_iso_code) & ~missing(ipo_price) & missing(lcu_per_usd)
replace ipo_price = ipo_price / lcu_per_usd if currency_iso_code != "USD" & ~missing(currency_iso_code) & ~missing(ipo_price) 

replace ipo_shares_offered = f_ipo_shares_offered if missing(ipo_shares_offered)
drop f_ipo_shares_offered

replace ipo_price = f_ipo_price if missing(ipo_price)
drop f_ipo_price

replace ipo_date = f_ipo_date if missing(ipo_date)
drop f_ipo_date

cap drop issuer_number
gen issuer_number = substr(cusip, 1, 6)
qui mmerge issuer_number using $cmns1/country_master/cmns_aggregation.dta, unmatched(m)
drop _merge month lcu_per_usd *source*

rename currency_iso_code bbg_currency
save $cmns1/temp/vie_consolidate/vie_static_consolidated, replace

* ---------------------------------------------------------------------------------------------------
* Consolidate VIE market caps from Bloomberg and Factset
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/vie_consolidate/bloomberg_marketcap, clear
mmerge cusip date using $cmns1/temp/vie_consolidate/factset_marketcap, unmatched(b) uname(f_)
mmerge cusip using $cmns1/temp/vie_consolidate/vie_static_consolidated, unmatched(m) ukeep(bbg_currency cusip6_up_bg issuer_name_up)

cap drop _merge
cap drop month
gen month = mofd(dofq(date))
format %tm month

mmerge month bbg_currency using $cmns1/exchange_rates/IFS_ERdata.dta, umatch(date_m iso_currency_code) unmatched(m)
replace market_cap = market_cap / lcu_per_usd if bbg_currency != "USD" & ~missing(bbg_currency)
replace market_cap = . if bbg_currency != "USD" & ~missing(bbg_currency) & missing(lcu_per_usd)
cap drop _merge

cap drop *source
cap drop bbg_currency month lcu_per_usd
order cusip cusip6_up_bg issuer_name_up date
save $cmns1/temp/vie_mcap_prelim, replace

use $cmns1/temp/vie_mcap_prelim, clear
cap drop _market_cap
cap drop N
gen _market_cap = floor(market_cap / 1e2)
bys cusip6_up_bg issuer_name_up date _market_cap: gen N = _N if ~missing(_market_cap)
replace market_cap = market_cap / N if ~missing(N)
drop N _market_cap
collapse (sum) market_cap (firstnm) issuer_name_up, by(cusip6_up_bg date)
gsort -market_cap
save $cmns1/temp/vie_mcap_bbg, replace

use $cmns1/temp/vie_mcap_prelim, clear
cap drop _f_market_cap
cap drop N

* Ensure we get Tencent right
gen value_tencent = f_market_cap if cusip6_up_bg == "G87572"
bys date: egen tencent_max = max(value_tencent)
replace f_market_cap = tencent_max if cusip6_up_bg == "G87572" & ~missing(f_market_cap)
drop value_tencent tencent_max
gen _f_market_cap = floor(f_market_cap / 1e3) if market_cap > 1000
replace _f_market_cap = floor(f_market_cap / 1e2) if market_cap <= 1000
bys cusip6_up_bg issuer_name_up date _f_market_cap: gen N = _N if ~missing(_f_market_cap)
replace f_market_cap = f_market_cap / N if ~missing(N)
drop N _f_market_cap
collapse (sum) f_market_cap (firstnm) issuer_name_up, by(cusip6_up_bg date)
gsort -f_market_cap
save $cmns1/temp/vie_mcap_factset, replace

* Merge BBG and Factset
use $cmns1/temp/vie_mcap_bbg, clear
mmerge cusip6_up_bg date using $cmns1/temp/vie_mcap_factset, unmatched(b)

replace market_cap = market_cap / 1e3
replace f_market_cap = f_market_cap / 1e3

gen consolidated_market_cap = f_market_cap
replace consolidated_market_cap = market_cap if missing(consolidated_market_cap)
drop _merge
drop market_cap f_market_cap
rename consolidated_market_cap market_cap
drop if missing(cusip6_up_bg)
gsort -market_cap
save $cmns1/temp/vie_consolidate/vie_marketcap_consolidated, replace

* ---------------------------------------------------------------------------------------------------
* Market capitalizations at first offering, for cost-basis time series
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/vie_consolidate/vie_marketcap_consolidated, clear
drop if market_cap == 0
sort cusip6_up_bg date
by cusip6_up_bg: keep if _n == 1
rename date first_date
rename market_cap initial_market_cap
save $cmns1/temp/vie_consolidate/initial_market_cap, replace

* ---------------------------------------------------------------------------------------------------
* Full price panel for VIEs
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/vie_consolidate/vie_static_consolidated.dta, clear
drop if missing(ipo_shares_offered)
gen ipo_offered = ipo_price * ipo_shares_offered
bys cusip6_up_bg ipo_shares_offered ipo_date: gen N = _N
bys cusip6_up_bg ipo_shares_offered ipo_date: gen n = _n
drop if n > 1
drop n N
bys cusip6_up_bg ipo_date: gen N = _N
bys cusip6_up_bg ipo_date: egen max_shares = max(ipo_shares_offered)
drop if N > 1 & ipo_shares_offered != max_shares
bys cusip6_up_bg ipo_date: gen n = _n
drop if n > 1
drop n N
bys cusip6_up_bg: gen N = _N

replace ipo_offered = ipo_offered / 1e6
drop max_shares

gen ipo_month = mofd(ipo_date)
format %tm ipo_month
cap drop N
bys cusip6_up_bg ipo_month: gen N = _N
bys cusip6_up_bg ipo_month: egen max_shares = max(ipo_shares_offered)
drop if N > 1 & ipo_shares_offered != max_shares
bys cusip6_up_bg ipo_month: gen n = _n
drop if n > 1
drop n N

bys cusip6_up_bg: gen N = _N
keep cusip6_up_bg issuer_name_up ipo_shares_offered ipo_price ipo_date ipo_month ipo_offered
rename ipo_offered ipo_offered_million

* Add in the ones for which we have no IPO data
mmerge cusip6_up_bg using $cmns1/temp/vie_consolidate/initial_market_cap, unmatched(b)

gen no_mcap_data = 0
replace no_mcap_data = 1 if _merge == 1

gen first_date_mo = mofd(dofq(first_date))
format %tm first_date_mo

replace initial_market_cap = initial_market_cap * 1e3
gen mcap_filled = 0
replace mcap_filled = 1 if ~missing(initial_market_cap) & _merge == 2
replace ipo_offered_million = initial_market_cap if _merge == 2
replace ipo_month = first_date_mo if _merge == 2
drop initial_market_cap _merge first_date*
save $cmns1/temp/vie_consolidate/vie_offerings_consolidated, replace

use $cmns1/temp/vie_consolidate/bloomberg_price, clear
mmerge cusip date using $cmns1/temp/vie_consolidate/factset_price, unmatched(b) uname(f_)
mmerge cusip using $cmns1/temp/vie_consolidate/vie_static_consolidated, unmatched(m) ukeep(bbg_currency cusip6_up_bg issuer_name_up)

cap drop _merge
cap drop month
gen month = mofd(dofq(date))
format %tm month

mmerge month bbg_currency using $cmns1/exchange_rates/IFS_ERdata.dta, umatch(date_m iso_currency_code) unmatched(m)
replace price = price / lcu_per_usd if bbg_currency != "USD" & ~missing(bbg_currency)
replace price = . if bbg_currency != "USD" & ~missing(bbg_currency) & missing(lcu_per_usd)

cap drop _merge
cap drop *source
cap drop bbg_currency month lcu_per_usd
order cusip cusip6_up_bg issuer_name_up date
save $cmns1/temp/vie_price_prelim, replace

use $cmns1/temp/vie_price_prelim, clear
cap drop _price
cap drop N
gen _price = floor(price * 100)
drop if strpos(cusip, "_")
bys cusip6_up_bg issuer_name_up date: gen N = _N if ~missing(_price)
bys cusip6_up_bg issuer_name_up date: egen max_price = max(price)
drop if N > 1 & floor(price) != floor(max_price)
bys cusip6_up_bg issuer_name_up date: keep if _n == 1
keep cusip6_up_bg issuer_name_up date price
save $cmns1/temp/vie_price_bbg, replace

use $cmns1/temp/vie_price_prelim, clear
cap drop _f_price
cap drop N
gen _f_price = floor(f_price * 100)
drop if strpos(cusip, "_")
bys cusip6_up_bg issuer_name_up date: gen N = _N if ~missing(_f_price)
bys cusip6_up_bg issuer_name_up date: egen max_f_price = max(f_price)
drop if N > 1 & floor(f_price) != floor(max_f_price)
bys cusip6_up_bg issuer_name_up date: keep if _n == 1
keep cusip6_up_bg issuer_name_up date f_price
save $cmns1/temp/vie_price_factset, replace

use $cmns1/temp/vie_price_bbg, clear
mmerge cusip6_up_bg issuer_name_up date using $cmns1/temp/vie_price_factset
replace price = f_price if missing(f_price)
drop f_price
save $cmns1/temp/vie_consolidate/vie_price_consolidated, replace

use $cmns1/temp/vie_consolidate/vie_marketcap_consolidated, clear
mmerge cusip6_up_bg issuer_name_up date using $cmns1/temp/vie_consolidate/vie_price_consolidated
drop _merge
gen shares_outstanding = market_cap * 1e9 / price
save $cmns1/temp/vie_consolidate/vie_shares_outstanding_consolidated, replace

use $cmns1/temp/vie_consolidate/vie_marketcap_consolidated, clear
collapse (sum) market_cap, by(date)
save $cmns1/temp/vie_mcap_q, replace

use $cmns1/temp/vie_consolidate/vie_offerings_consolidated.dta, clear
gen date = qofd(dofm(ipo_month))
format %tq date
collapse (sum) ipo_offered_million, by(date)
gen ipo_cost = ipo_offered_million / 1e3
drop if date < tq(2000q1)
gen cum_ipo_cost = sum(ipo_cost)
drop ipo_offered_million ipo_cost
save $cmns1/temp/vie_cost_q, replace

use $cmns1/temp/vie_consolidate/vie_offerings_consolidated.dta, clear
gen date = qofd(dofm(ipo_month))
format %tq date
gen year = year(dofq(date))
gsort cusip6_up_bg year -ipo_date
by cusip6_up_bg year: keep if _n == 1
keep cusip6_up_bg ipo_price year issuer_name_up
drop if year < 2004
drop if missing(year)
reshape wide ipo_price, i(cusip6_up_bg issuer_name_up) j(year)
replace ipo_price2005 = ipo_price2004 if missing(ipo_price2005)
replace ipo_price2006 = ipo_price2005 if missing(ipo_price2006)
replace ipo_price2007 = ipo_price2006 if missing(ipo_price2007)
replace ipo_price2008 = ipo_price2007 if missing(ipo_price2008)
replace ipo_price2009 = ipo_price2008 if missing(ipo_price2009)
replace ipo_price2010 = ipo_price2009 if missing(ipo_price2010)
replace ipo_price2011 = ipo_price2010 if missing(ipo_price2011)
replace ipo_price2012 = ipo_price2011 if missing(ipo_price2012)
replace ipo_price2013 = ipo_price2012 if missing(ipo_price2013)
replace ipo_price2014 = ipo_price2013 if missing(ipo_price2014)
replace ipo_price2015 = ipo_price2014 if missing(ipo_price2015)
replace ipo_price2016 = ipo_price2015 if missing(ipo_price2016)
replace ipo_price2017 = ipo_price2016 if missing(ipo_price2017)
replace ipo_price2018 = ipo_price2017 if missing(ipo_price2018)
replace ipo_price2019 = ipo_price2018 if missing(ipo_price2019)
reshape long
rename ipo_price latest_ipo_price
save $cmns1/temp/vie_consolidate/vie_latest_ipo_price, replace

use $cmns1/temp/vie_consolidate/vie_shares_outstanding_consolidated.dta, clear
gen year = year(dofq(date))
drop if year < 2005
mmerge cusip6_up_bg year using $cmns1/temp/vie_consolidate/vie_latest_ipo_price, unmatched(m)
gen c_market_cap = shares_outstanding * latest_ipo_price
collapse (sum) c_market_cap, by(date)
replace c_market_cap = c_market_cap / 1e9
save $cmns1/temp/vie_c_cap_q, replace

use $cmns1/temp/vie_consolidate/vie_offerings_consolidated.dta, clear
gen date = qofd(dofm(ipo_month))
format %tq date
collapse (sum) ipo_offered_million, by(date)
gen ipo_cost = ipo_offered_million / 1e3
drop if date < tq(2000q1)
gen cum_ipo_cost = sum(ipo_cost)
drop ipo_offered_million ipo_cost
save $cmns1/temp/vie_cost_q, replace

use $cmns1/temp/vie_mcap_q, clear
mmerge date using $cmns1/temp/vie_cost_q
drop if missing(date)
drop if date < tq(2005q1)
ipolate cum_ipo_cost date, gen(_ip)
replace cum_ipo_cost = _ip if missing(cum_ipo_cost)
drop _merge _ip
gen date_int = date
drop if date > tq(2019q2)
drop date_int
save $cmns1/china_master/vie_timeseries, replace

use $cmns1/china_master/vie_timeseries, clear
mmerge date using $cmns1/temp/vie_c_cap_q, unmatched(m)
drop _merge
rename cum_ipo_cost cost_raised
rename c_market_cap cost_marked
save $cmns1/china_master/vie_timeseries_consolidated, replace

* ---------------------------------------------------------------------------------------------------
* Import BoP data from SAFE as well as Chinese GDP
* ---------------------------------------------------------------------------------------------------

* BoP data from SAFE: yearly
import excel $raw/SAFE/SAFE_IIP.xlsx, sheet("Annual(USD)") clear
drop if _n<=2 | _n==4
foreach x of varlist _all {
	replace `x'=subinstr(`x',"end-","y",.) if _n==1
	local temp=`x'[1]
	rename `x' `temp'
}
drop if _n==1
gen al=""
replace al="a" if _n<=28
replace al="l" if _n>=29
drop if Item==""
replace Item=trim(Item)
gen item_code = ""
keep if al == "l" | _n == 1
drop if _n >= 6 & _n <=11
drop if _n == 10
replace item_code = "net_iip" if _n == 1
replace item_code = "tot_liabilities" if _n == 2
replace item_code = "fdi" if _n == 3
replace item_code = "fdi_equity" if _n == 4
replace item_code = "fdi_debt" if _n == 5
replace item_code = "portfolio" if _n == 6
replace item_code = "portfolio_equity" if _n == 7
replace item_code = "portfolio_debt" if _n == 8
replace item_code = "derivatives" if _n == 9
replace item_code = "other_equity" if _n == 10
replace item_code = "currency" if _n == 11
replace item_code = "loans" if _n == 12
replace item_code = "insurance" if _n == 13
replace item_code = "trade_credits" if _n == 14
replace item_code = "accounts_payable" if _n == 15
replace item_code = "sdr" if _n == 16
drop Item
drop al
order item_code
reshape long y, i(item_code) j(year)
rename y value
destring value, force replace
replace value = value / 10
reshape wide value, i(year) j(item_code) string
rename value* *
save $cmns1/temp/safe_iip_liabilities, replace

* BoP data from SAFE: quarterly
import excel using $raw/SAFE/SAFE_IIP.xlsx, sheet("Quarterly(USD)") clear allstring
foreach var of varlist * {
    qui replace `var' = "value" + `var' if _n == 3
}
drop if _n < 3
foreach var of varlist * { 
     local try = strtoname(`var'[1]) 
     capture rename `var'  `try' 
}
drop if _n < 3
drop if _n > 50
rename valueItem item
replace item = "A_" + item if _n < 29
replace item = "L_" + item if _n >= 29
keep if _n == 1 | _n >= 29
drop if _n >= 6 & _n <=11
drop if _n == 10
gen item_code = ""
replace item_code = "net_iip" if _n == 1
replace item_code = "tot_liabilities" if _n == 2
replace item_code = "fdi" if _n == 3
replace item_code = "fdi_equity" if _n == 4
replace item_code = "fdi_debt" if _n == 5
replace item_code = "portfolio" if _n == 6
replace item_code = "portfolio_equity" if _n == 7
replace item_code = "portfolio_debt" if _n == 8
replace item_code = "derivatives" if _n == 9
replace item_code = "other_equity" if _n == 10
replace item_code = "currency" if _n == 11
replace item_code = "loans" if _n == 12
replace item_code = "insurance" if _n == 13
replace item_code = "trade_credits" if _n == 14
replace item_code = "accounts_payable" if _n == 15
replace item_code = "sdr" if _n == 16
order item_code item
drop item
reshape long value, i(item) j(date) string
cap drop _date
gen _date = date(date, "DMY")
format %td _date
gen date_q = qofd(_date)
format %tq date_q
drop date _date
destring value, force replace
replace value = value / 10
save $cmns1/china_master/safe_iip_liabilities_quarterly, replace

* Time series for Chinese GDP 
import excel using $raw/World_Bank/China_GDP_Yearly.xlsx, clear firstrow
keep year gdp
sort year
save $cmns1/temp/china_gdp, replace

* ---------------------------------------------------------------------------------------------------
* Restatement of China's NFA
* ---------------------------------------------------------------------------------------------------

use $cmns1/china_master/vie_timeseries, clear
mmerge date using $cmns1/temp/vie_c_cap_q, unmatched(m)
gen year = year(dofq(date))
gen quarter = quarter(dofq(date))
keep if quarter == 4
drop date quarter
drop _merge
mmerge year using $cmns1/temp/safe_iip_liabilities, unmatched(m)
mmerge year using $cmns1/temp/china_gdp, unmatched(m)
drop _merge

preserve
keep market_cap cum_ipo_cost c_market_cap year net_iip gdp
order year
save $cmns1/temp/china_nfa_data, replace
restore

gen net_iip_restate_ipo = net_iip - market_cap + cum_ipo_cost
gen net_iip_restate_c = net_iip - market_cap + c_market_cap

preserve
gen iip_gap_ipo = net_iip - net_iip_restate_ipo
gen iip_gap_c = net_iip - net_iip_restate_c
keep year iip_gap*
save $cmns1/temp/china_iip_gaps, replace
restore

* Current account data
use $raw/World_Bank/World_Bank_Current_Accounts, clear 
keep if countrycode=="CHN"
rename bn_cab_xoka_cd ca
rename ny_gdp_mktp_cd gdp
replace ca=ca/(10^9)
replace gdp=gdp/(10^9)
keep if ca~=.
gen ca_gdp=ca/gdp
gen cum=sum(ca)
gen cum_ca_gdp=cum/gdp
keep year cum_ca_gdp
save $cmns1/temp/china_cum_ca_gdp, replace

* ---------------------------------------------------------------------------------------------------
* NFA plot
* ---------------------------------------------------------------------------------------------------

* Parameters: Chinese ownership shares (as estimated via asset-side analysis)
local insider_share = .184
local insider_share_conservative = .184 + .154 + .007

* Construct NFA restatement time series
use $cmns1/temp/china_nfa_data, clear
gen net_iip_by_gdp = net_iip / gdp
gen net_iip_restate_c = net_iip - (1 - `insider_share') * (market_cap - cum_ipo_cost)
gen net_iip_restate_c_zcost = net_iip - (1 - `insider_share') * (market_cap)
gen net_iip_restate_c_aggressive = net_iip - (market_cap - cum_ipo_cost)
gen net_iip_restate_c_conservative = net_iip - (1 - `insider_share_conservative') * (market_cap - cum_ipo_cost)
gen net_iip_by_gdp_restate_c = net_iip_restate_c / gdp
gen net_iip_by_gdp_restate_c_zcost = net_iip_restate_c_zcost / gdp
gen net_iip_by_gdp_restate_c_agg = net_iip_restate_c_agg / gdp
gen net_iip_by_gdp_restate_c_cons = net_iip_restate_c_cons / gdp
qui mmerge year using $cmns1/temp/china_cum_ca_gdp, unmatched(m)
drop _merge

cap drop gap
gen gap = (net_iip_by_gdp - net_iip_by_gdp_restate_c_zcost) * gdp

cap drop gap_agg
gen gap_agg = (net_iip_by_gdp - net_iip_by_gdp_restate_c_agg) * gdp

cap drop gap_cons
gen gap_cons = (net_iip_by_gdp - net_iip_by_gdp_restate_c_cons) * gdp

cap drop area_min area_max
gen area_min = net_iip_by_gdp_restate_c_agg + .001
gen area_max = net_iip_by_gdp_restate_c_cons - .001

* Prepare plot elements
cap drop y1 y2 x1 x2
cap drop x1b x2b
cap drop yM
cap drop xM xMb
gen y1 = .0732
gen y2 = .156
gen x1 = 2018.22
gen x2 = 2018.22
gen x1b = x1 - .1
gen x2b = x2 - .1
gen yM = ((y2 - y1) / 2) + y1
gen xM = x1
gen xMb = xM + .1
qui replace y1 = . if year < 2018
qui replace y2 = . if year < 2018
qui replace x1 = . if year < 2018
qui replace x2 = . if year < 2018

* NFA adjustment plot
line net_iip_by_gdp net_iip_by_gdp_restate_c net_iip_by_gdp_restate_c_cons net_iip_by_gdp_restate_c_agg year, xlab(2005(2)2018) graphregion(color(white))     xtitle("") ytitle("China NFA, Share of GDP") lpattern(dash solid shortdash longdash)     lwidth(medthick medthick medium medium)     lcolor(black red gray gray)     legend(label(1 "Official NFA Position") label(2 "With VIE Adjustment, Baseline") label(3 "Upper Bound on Chinese Holdings")     label(4 "Assets Reflect Listed Value") order(1 2 3 4) cols(2)) graphregion(margin(3 16 1 3))     xsize(6.8) ylab(0(.05).3) text(.1161 2019.5 "{c $|}1.1T", place(w) size(medium))     || pcarrow y1 x1 y2 x2, mcolor(black%0) mlcolor(black%0) mfcolor(black%0) lcolor(black%80)     || pcarrow y1 x1 y1 x1b, mcolor(black%0) mlcolor(black%0) mfcolor(black%0) lcolor(black%80)     || pcarrow y2 x2 y2 x2b, mcolor(black%0) mlcolor(black%0) mfcolor(black%0) lcolor(black%80)     || pcarrow yM xM yM xMb, mcolor(black%0) mlcolor(black%0) mfcolor(black%0) lcolor(black%80) lwidth(thin)

graph export $cmns1/graphs/china_nfa_all_cases_bracket_paper.pdf, as(pdf) replace

* ---------------------------------------------------------------------------------------------------
* Counterexample: US equity holdings in Caymans
* ---------------------------------------------------------------------------------------------------

* VIE time series data at a yearly frequency
use $cmns1/china_master/vie_timeseries, clear
gen year = year(dofq(date))
gen quarter = quarter(dofq(date))
keep if quarter == 4
drop quarter date
save $cmns1/china_master/vie_timeseries_y, replace

* US holdings of Chinese equities in the Cayman Islands (share)
use $cmns1/holdings_master/mns_issuer_summary.dta, clear
keep if asset_class == "Equity"
keep if DomicileCountryId == "USA" & cgs_domicile == "CYM"
collapse (sum) marketvalue_usd, by(year country_bg)
bys year: egen totVal = total(marketvalue_usd)
gen share = marketvalue_usd / totVal
keep if country_bg == "CHN"
rename share cym_china_share
keep year cym_china_share
save $cmns1/temp/cym_china_share, replace

* Prepare data for US counterexample plot
use $cmns1/temp/tic_disaggregated/equity/tic_equity, clear
cap drop F-M
keep if iso == "CYM"
local new = _N + 1
set obs `new'
replace year = 2018 if _n == _N
replace Common = 482901 if _n == _N
replace iso = "CYM"
mmerge year using $cmns1/temp/cym_china_share, unmatched(m)
replace cym_china_share = .87203074 if _n == _N
gen china_position = Common
replace china_position = china_position / 1e3
mmerge year using $cmns1/china_master/vie_timeseries_y, unmatched(m)
cap drop *_index
gen p_index = china_position / china_position[_N] * 100
gen m_index = market_cap / market_cap[_N] * 100
drop _merge
keep p_index m_index year
save $cmns1/temp/usa_cym_vie_plot_data, replace

* US counterexample plot
use $cmns1/temp/usa_cym_vie_plot_data, clear
line p_index m_index year, xlab(2007(2)2017) graphregion(color(white))     lcolor(blue red) lpattern(solid dash) legend(label(1 "U.S. Common Equity Investment in CYM (TIC)") label(2 "VIE Market Cap"))     xtitle("") ytitle("Index (2018 = 100)") xsize(6.2)
graph export $cmns1/graphs/usa_cym_investment_vs_vie_equity_index_improved_nomatrix.pdf, as(pdf) replace

* ---------------------------------------------------------------------------------------------------
* Plotting time series of VIE values together with FDI flows
* ---------------------------------------------------------------------------------------------------

* Composition of inward FDI into China: time series
use $cmns1/temp/cdis_iiw_long, clear
keep if codei == "CHN"
keep if year >= 2009 & year <= 2017
drop if codej == "WLD"
bys year: egen totValue = total(direct)
gen share = direct / totValue
keep if inlist(codej, "NSP", "HKG", "VGB", "CYM")
keep codej year share
reshape wide share, i(year) j(codej) string
gen share_offshore = shareHKG + shareVGB + shareCYM
gen share_offshore_nsp = shareHKG + shareVGB + shareCYM + shareNSP
local new = _N + 1
set obs `new'
foreach var of varlist * {
    replace `var' = `var'[_N - 1] if _n == _N
}
replace year = 2018 if _N == _n
save $cmns1/temp/china_inward_fdi_composition, replace

use $cmns1/china_master/safe_iip_liabilities_quarterly, clear
gen year = year(dofq(date_q))
mmerge year using $cmns1/temp/china_inward_fdi_composition, unmatched(m)
keep if _merge == 3
drop _merge

preserve
keep if inlist(item_code, "fdi")
replace value = value * share_offshore
replace item_code = "fdi_offshore"
keep item_code value date_q
save $cmns1/temp/china_l_fdi_offshore, replace
restore

keep if inlist(item_code, "fdi")
replace value = value * share_offshore_nsp
replace item_code = "fdi_offshore_nsp"
keep item_code value date_q
save $cmns1/temp/china_l_fdi_offshore_nsp, replace

use $cmns1/china_master/safe_iip_liabilities_quarterly, clear
append using $cmns1/temp/china_l_fdi_offshore
append using $cmns1/temp/china_l_fdi_offshore_nsp
reshape wide value, i(date_q) j(item_code) string
rename value* *
mmerge date_q using $cmns1/china_master/vie_timeseries_consolidated.dta, umatch(date)
keep if _merge == 3
drop _merge

gen fdi_onshore = fdi - fdi_offshore
gen fdi_onshore_nsp = fdi - fdi_offshore_nsp

label var fdi_offshore "FDI from HKG, VGB, CYM"
label var fdi_onshore "FDI ex. HKG, VGB, CYM"
label var market_cap "VIE Market Cap"
label var cost_marked "VIE Cost (Mark at Offering)"

gen _date = date_q
format %8.0f _date
save $cmns1/temp/vie_graph_quarterly_fdi_line, replace

use $cmns1/china_master/vie_timeseries_consolidated.dta, clear
gen year = year(dofq(date))
gen quarter = quarter(dofq(date))
keep if quarter == 4
drop date
save $cmns1/china_master/vie_timeseries_consolidated_annual, replace

use $cmns1/china_master/vie_timeseries, clear
qui mmerge date using $cmns1/temp/vie_c_cap_q, unmatched(m)
drop _merge
mmerge date using $cmns1/temp/vie_graph_quarterly_fdi_line, umatch(date_q) ukeep(fdi_offshore)

line market_cap cum_ipo_cost fdi_offshore date, graphregion(color(white)) xtitle("")     ytitle("USD Billions" "")     legend(label(1 "VIE Market Value") label(2 "VIE Equity Offered") cols(3)) xsize(6.9)     lpattern(longdash solid  shortdash) lcolor(red blue  green)     xlabel(180(8)237)
graph export $cmns1/graphs/vie_timeseries_value_vs_cost_with_fdi.pdf, as(pdf) replace

* ---------------------------------------------------------------------------------------------------
* Bar chart: deltas in BoP categories during VIE value jump
* ---------------------------------------------------------------------------------------------------

use $cmns1/china_master/vie_timeseries, clear
gen year = year(dofq(date))
gen quarter = quarter(dofq(date))
gen to_keep = 0
replace to_keep = 1 if quarter == 4 & ~inlist(year, 2018)
replace to_keep = 1 if quarter == 1 & inlist(year, 2018)
keep if to_keep == 1
keep year market_cap
save $cmns1/temp/vie_mcap_for_barchart, replace

use $cmns1/temp/safe_iip_liabilities, clear
mmerge year using $cmns1/temp/vie_mcap_for_barchart, unmatched(m)
drop _merge
rename market_cap vie_marketcap
keep if year >= 2005 & year <= 2018
replace tot_liabilities = fdi + portfolio + loans
keep year fdi fdi_debt fdi_equity portfolio_equity portfolio_debt tot_liabilities loans vie_marketcap 
gen iso = "CHN"
mmerge year using $cmns1/temp/china_inward_fdi_composition, unmatched(m)
keep if _merge == 3
gen fdi_offshore = fdi * share_offshore
drop fdi
drop _merge share*
reshape wide fdi_debt fdi_equity fdi_offshore portfolio_equity portfolio_debt tot_liabilities     loans vie_marketcap, i(iso) j(year)
foreach var in fdi_debt fdi_equity fdi_offshore portfolio_equity portfolio_debt tot_liabilities loans vie_marketcap {
	gen return_`var' = (`var'2018 - `var'2016)
}
keep return_*
gen iso = "CHN"
reshape long return_, i(iso) j(category) string

gen rank = .
replace rank = 1 if category == "vie_marketcap"
replace rank = 2 if category == "fdi_equity"
replace rank = 3 if category == "fdi_debt"
replace rank = 3.5 if category == "fdi_offshore"
replace rank = 4 if category == "portfolio_equity"
replace rank = 5 if category == "portfolio_debt"
replace rank = 6 if category == "loans"
replace rank = 7 if category == "total_liabilities"
rename *_ *
separate return, by(category == "vie_marketcap")

replace category = "VIE Market Cap" if category == "vie_marketcap"
replace category = "FDI Equity" if category == "fdi_equity"
replace category = "FDI Debt" if category == "fdi_debt"
replace category = "Portfolio Equity" if category == "portfolio_equity"

graph bar (asis) return0 return1, over(category, sort(rank) relabel(1 `""FDI Debt" "Liabilities" "' 2 `""FDI Equity" "Liabilities" "'     3 `""Portfolio" "Equity" "Liabilities" "'  4 `""VIE" "Market Value""'     5 `""FDI Liabilities" "From HKG," " VGB, or CYM""'     6 `""Loan" "Liabilities""'     7 `""Portfolio" "Debt" "Liabilities" "' 8 `""Tot. Liabilities" "(Ex. Reserves, TCs)""')) graphregion(color(white)) ytitle("Growth in USD Billions, 2016Q4 - 2018Q1") legend(off) nofill xsize(7.7) scale(.8)
graph export $cmns1/graphs/vie_vs_china_liabilities_barchart_levels_with_offshore.pdf, as(pdf) replace 

* ---------------------------------------------------------------------------------------------------
* Counterexample: South African holdings in Tencent
* ---------------------------------------------------------------------------------------------------

* South Africa's FDI into China
use $cmns1/temp/cdis_iiw_long.dta, clear
keep if codej == "ZAF" & codei == "CHN"
keep if year >= 2009 & year <= 2017
keep year derived
rename derived south_africa_outward_fdi
save $scratch/south_africa_outward_fdi, replace

* Tencent marketcap
use $cmns1/temp/vie_consolidate/vie_marketcap_consolidated, clear
keep if cusip6_up_bg == "G87572"
gen year = year(dofq(date))
gsort year -date
by year: keep if _n == 1
keep if year >= 2009 & year <= 2017
drop date issuer_name_up cusip6_up_bg
rename market_cap tencent_marketcap
save $scratch/tencent_marketcap, replace

* Prepare data for South Africa counterexample
use $scratch/south_africa_outward_fdi, clear
qui mmerge year using $scratch/tencent_marketcap
drop _merge
gen p_index = south_africa_outward_fdi / south_africa_outward_fdi[_N] * 100
gen share = tencent_marketcap * 0.31

* Plot South Africa counterexample
line south_africa_outward_fdi share year, xlab(2009(2)2017) graphregion(color(white)) lcolor(blue red) ///
    lpattern(solid dash) legend(label(1 "South African FDI into China (CDIS)") ///
    label(2 "31% of Tencent Market Cap")) xtitle("") ytitle("USD Billions") xsize(6.2) ylab(0(50)150, gmin gmax)

log close
