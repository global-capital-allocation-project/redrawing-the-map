* ---------------------------------------------------------------------------------------------------
* Build_Bond_Issuance: This job produces the internal security-level bond issuance data masterfile,
* combining issuance data from both Dealogic and Factset
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Build_Bond_Issuance, replace

* ---------------------------------------------------------------------------------------------------
* Generate auxiliary tables
* ---------------------------------------------------------------------------------------------------

* sector crosswalk using CUSIP codes
use $raw/figi/figi_master_compact.dta, clear 
keep cusip marketsector
drop if cusip == ""
duplicates drop
save $cmns1/temp/fig_master_cusip_sector.dta, replace

* sector crosswalk using ISIN codes
use $raw/figi/figi_master_compact.dta, clear
keep isin marketsector
drop if isin == ""
duplicates drop
save $cmns1/temp/fig_master_isin_sector.dta, replace

* ---------------------------------------------------------------------------------------------------
* Quarterly exchange rates
* ---------------------------------------------------------------------------------------------------

use $cmns1/exchange_rates/IFS_ERdata.dta, clear
gen date_q = qofd(dofm(date_m))
format %tq date_q
gsort iso_currency_code date_q -date_m
by iso_currency_code date_q: keep if _n == 1
drop date_m
save $cmns1/temp/quarterly_fx_rates, replace

* ---------------------------------------------------------------------------------------------------
* Dealogic: transform data to time series
* ---------------------------------------------------------------------------------------------------

cap restore
use $cmns1/issuance_master/dealogic_dcm_issuance_complete.dta, clear
rename _merge_cusip merge_cusip
replace takeoutdate = substr(takeoutdate,1,10)
gen _takeoutdate = date(takeoutdate,"YMD")

gen mdate = mofd(_takeoutdate)
format mdate %tm 
drop takeoutdate
rename mdate takeoutdate

format %tm takeoutdate
format %td _takeoutdate

replace expectedmaturitydate = substr(expectedmaturitydate,1,10)
gen _expectedmaturitydate = date(expectedmaturitydate,"YMD")

gen mdate = mofd(_expectedmaturitydate)
format mdate %tm 
drop expectedmaturitydate
rename mdate expectedmaturitydate

format %tm takeoutdate expectedmaturitydate
format %td _takeoutdate _expectedmaturitydate

gen start_date = qofd(_pricingdate)
gen end_date = qofd(_maturitydate)
gen takeout_date = qofd(_takeoutdate)

replace start_date = qofd(_settlementdate) if start_date == . & qofd(_settlementdate) != .
replace end_date = qofd(_expectedmaturitydate) if end_date == .

replace end_date = takeout_date if takeout_date < end_date & takeout_date != . & end_date != . 
replace end_date = takeout_date if takeout_date < end_date & takeout_date != . & end_date == . 
replace end_date = tq(2099q4) if end_date == . 

format start_date end_date takeout_date %tq

* drops perpetual bonds
drop if start_date == . | end_date == . | value == .

keep dcmdealdealid trancheid cusip cusip6 isin residency nationality start_date end_date value ///
currencyisocode currency_issued conversion_rate_at_issuance  typeid merge_cusip pricingdate announcementdate settlementdate maturitydate

duplicates drop

bys dcmdealdealid trancheid: gen n_values = _n
bys dcmdealdealid trancheid: gen N_values = _N

gen duration = end_date - start_date + 1 
drop if duration == .
drop if duration < 0
expand duration

* removes duplicates
gsort dcmdealdealid	trancheid -cusip
by dcmdealdealid trancheid: gen datewanted = start_date + _n - 1
drop if datewanted > tq(2019q4)
format datewanted %tq

* ---------------------------------------------------------------------------------------------------
* Dealogic: currency adjustments
* ---------------------------------------------------------------------------------------------------

drop currencyisocode
rename currency_issued currencyisocode
replace currencyisocode = "EUR" if currencyisocode == "XBB" | currencyisocode == "XBD" | currencyisocode == "XBC"
replace currencyisocode = "AFN" if currencyisocode == "AFA"

mmerge currencyisocode datewanted using $cmns1/temp/quarterly_fx_rates, umatch(iso_currency_code date_q)
rename lcu_per_usd fx_rate
keep if _merge != 2
sort dcmdealdealid trancheid
drop if _merge == 1 & currencyisocode != "USD"
replace fx_rate = 1 if _merge == 1
gen value_cur_adj = value*conversion_rate_at_issuance/fx_rate
drop value currencyisocode	conversion_rate_at_issuance n_values N_values duration _merge
save $cmns1/issuance_master/dealogic_dcm_issuance_timeseries.dta, replace

* ---------------------------------------------------------------------------------------------------
* Add Factset and BBG data to Dealogic
* ---------------------------------------------------------------------------------------------------

cap restore
use $cmns1/issuance_master/dealogic_dcm_issuance_timeseries.dta, clear
gen datewanted_year = year(dofq(datewanted))
gen datewanted_quarter = quarter(dofq(datewanted))
keep if datewanted_quarter == 4
keep if datewanted_year > 2000
sort dcmdealdealid trancheid
mmerge cusip datewanted_year using $cmns1/temp/facset_issuance_all_bonds.dta, umatch(cusip year)
sort dcmdealdealid trancheid
drop if residency == "NLD" & nationality == "VEN"
replace value_cur_adj = os_amount_usd if _merge == 2
replace residency = cgs_domicile if _merge == 2
replace nationality = country_bg if _merge == 2

* ---------------------------------------------------------------------------------------------------
* Getting CMNS currency
* ---------------------------------------------------------------------------------------------------

preserve
use $cmns1/temp/cgs/ALLMASTER_ISIN, clear
gen cusip = issuer_num+issue_num+issue_check
keep cusip isin currency_code
duplicates drop
drop if isin == ""
drop if missing(cusip)
save $cmns1/temp/allmaster_currency.dta, replace

restore
mmerge cusip using $cmns1/temp/allmaster_currency.dta, umatch(cusip)
drop if _merge == 2

* ---------------------------------------------------------------------------------------------------
* Getting CMNS asset class
* ---------------------------------------------------------------------------------------------------

* first check the ones that already have a mns_class defined
mmerge cusip using $cmns1/holdings_master/Internal_Class_NonUS_US.dta
keep if _merge != 2

preserve 
keep if _merge != 3

* for the ones where we can't get an MNS class, first try to merge by CUSIP
mmerge cusip using $cmns1/temp/fig_master_cusip_sector.dta
keep if _merge != 2
rename marketsector marketsector_cusip

* now use isin
mmerge isin using $cmns1/temp/fig_master_isin_sector.dta
keep if _merge != 2
rename marketsector marketsector_isin
gen marketsector = marketsector_isin
replace marketsector = marketsector_cusip if marketsector_isin == "" & marketsector_cusip != ""
keep dcmdealdealid trancheid marketsector
duplicates drop
save $cmns1/temp/dcs_dealogic_no_mns_class.dta, replace

restore
mmerge dcmdealdealid trancheid using $cmns1/temp/dcs_dealogic_no_mns_class.dta
gen is_all_bonds=0
replace is_all_bonds = 1 if mns_class == "B"  //prioritize cmns classification
replace is_all_bonds = 1 if mns_class == "" & inlist(marketsector,"Corp","Govt","Muni","Mtge")  // then get figi class
replace is_all_bonds = 1 if mns_class == "" & marketsector == "" & typeid != .  // then dealogic

gen is_corp = 0
replace is_corp = 1 if mns_class == "B" & ~inlist(mns_subclass,"S", "A", "LS", "SF", "SV") //prioritize cmns classification
replace is_corp = 1 if mns_class == "" & marketsector == "Corp" & ~inlist(typeid,13) // then get figi class
replace is_corp = 1 if mns_class == "" & marketsector == ""  & ~inlist(typeid,1,2,6,7,13) // then dealogic
replace is_corp = 0 if mns_class == "" & marketsector == "" & typeid == .

sort dcmdealdealid trancheid
drop datewanted datewanted_quarter	os_amount_usd	cgs_domicile	country_bg _merge merge_cusip start_date	end_date	
rename datewanted_year year

* ---------------------------------------------------------------------------------------------------
* Getting parent information
* ---------------------------------------------------------------------------------------------------

replace cusip6 = substr(cusip,1,6) if cusip6 == ""
qui mmerge cusip6 using $cmns1/country_master/cmns_aggregation, umatch(issuer_number) ukeep(cusip6_up_bg issuer_name_up)
drop if _merge == 2
drop _merge
sort dcmdealdealid trancheid year

replace residency = "ARE" if residency == "UAE"
replace nationality = "ARE" if nationality == "UAE"
replace residency = "ROU" if residency == "ROM"
replace nationality = "ROU" if nationality == "ROM"

drop if nationality == "YUG" | residency == "YUG"
drop if residency == "ANX" | residency == "SU"
drop mns_class mns_subclass mns_category marketsector
duplicates drop

drop if is_all_bonds == 0 & is_corp == 0

* ---------------------------------------------------------------------------------------------------
* Cleaning duplicates
* ---------------------------------------------------------------------------------------------------

duplicates drop

sort year dcmdealdealid trancheid cusip cusip6 isin residency nationality typeid pricingdate announcementdate settlementdate maturitydate fx_rate value_cur_adj cusip6_up_bg issuer_name_up is_all_bonds is_corp 

by year dcmdealdealid trancheid cusip cusip6 isin residency nationality typeid pricingdate announcementdate settlementdate maturitydate fx_rate value_cur_adj cusip6_up_bg issuer_name_up: gen Nval = _N

by year dcmdealdealid trancheid cusip cusip6 isin residency nationality typeid pricingdate announcementdate settlementdate maturitydate fx_rate value_cur_adj cusip6_up_bg issuer_name_up: gen nval = _n

keep if Nval == nval
drop Nval nval

* ---------------------------------------------------------------------------------------------------
* Drop exchanged bonds (inactive ones)
* ---------------------------------------------------------------------------------------------------

drop active
mmerge cusip using $cmns1/temp/factset_inactive_cusip.dta, uname(__)
drop if _merge == 2

gen active = 1
replace active = 0 if _merge == 3 & year >= __year
drop __*

mmerge isin using $cmns1/temp/factset_inactive_isin.dta, uname(__)
drop if _merge == 2
replace active = 0 if _merge == 3 & year >= __year

drop __*
drop _merge

gsort -value_cur_adj
drop if active == 0
save $cmns1/issuance_master/dealogic_factset_issuance_timeseries.dta, replace

log close
