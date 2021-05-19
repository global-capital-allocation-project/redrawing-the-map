* ---------------------------------------------------------------------------------------------------
* Build_Factset_Master: This job produces a bond issuance masterfile from the Factset data, which
* is an input to the Build_Bond_Issuance job
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Build_Factset_Master, replace

* ---------------------------------------------------------------------------------------------------
* Auxiliary table: inactive bonds
* ---------------------------------------------------------------------------------------------------

cap restore
use $raw/Factset/fds_stata/dcs_details.dta, clear
keep if active == 0
gen _report_date = date(report_date,"YMD")
format %td _report_date
drop report_date 
rename _report_date report_date
gen year = year(report_date)

keep instrument_id year maturity_year
sort instrument_id year 
collapse (firstnm) year maturity_year, by(instrument_id)
gen is_equal = 0
replace is_equal = 1 if maturity_year <= year
keep if is_equal == 0
drop is_equal

mmerge instrument_id using $raw/Factset/fds_stata/sym_isin.dta, umatch(fsym_id)
drop if _merge == 2
mmerge instrument_id using $raw/Factset/fds_stata/sym_isin_hist.dta, umatch(fsym_id) uname(__)
drop if _merge == 2

replace isin = __isin if isin == "" & __isin != ""
drop _merge	__isin	__start_date	__end_date	__most_recent
mmerge instrument_id using $raw/Factset/fds_stata/sym_cusip.dta, umatch(fsym_id)
drop if _merge == 2
mmerge instrument_id using $raw/Factset/fds_stata/dcs_cusip_hist.dta, umatch(fsym_id) uname(__)
drop if _merge == 2

replace cusip = __cusip if cusip == "" & __cusip != ""
drop _merge	__cusip	__start_date __end_date	__most_recent
drop if isin == "" & cusip == ""
drop if maturity_year == .
duplicates drop
drop maturity_year 
duplicates drop
bys cusip : gen Nv = _N

preserve 
drop if cusip == ""
duplicates drop
save $cmns1/temp/factset_inactive_cusip.dta, replace
restore
preserve
drop if isin == ""
duplicates drop
save $cmns1/temp/factset_inactive_isin.dta, replace
restore

* ---------------------------------------------------------------------------------------------------
* Debt Capital Structure (DCS) details
* ---------------------------------------------------------------------------------------------------

cap restore
use $raw/Factset/fds_stata/dcs_details.dta, clear
drop if issue_amount == .
mmerge factset_entity_id using $raw/Factset/fds_stata/ent_entity_coverage.dta, umatch(factset_entity_id) ukeep(entity_name	entity_proper_name	iso_country	iso_country_incorp)
keep if _merge == 3
drop factset_entity_id entity_name	entity_proper_name
duplicates drop
gen debt_code_2 = substr(debt_code,1,2)
gsort instrument_id report_date
gen _report_date = date(report_date,"YMD")
format %td _report_date
drop report_date 
rename _report_date report_date

gen date_m = mofd(report_date)
format %tm date_m

mmerge currency date_m using $cmns1/exchange_rates/IFS_ERdata.dta, umatch(iso_currency_code date_m)
drop if _merge == 2
rename lcu_per_usd exch_rate_per_usd
gen exch_rate_usd = 1 / exch_rate_per_usd
replace exch_rate_per_usd = 1 if _merge == 1
replace exch_rate_usd = 1 if _merge == 1

gen issue_amount_usd = issue_amount*exch_rate_usd
gen os_amount_usd = os_amount*exch_rate_usd
gen avail_amount_usd = avail_amount*exch_rate_usd

mmerge issuance_currency date_m using $cmns1/exchange_rates/IFS_ERdata.dta, umatch(iso_currency_code date_m) uname(LC_)
drop if _merge == 2
rename LC_lcu_per_usd LC_exch_rate_per_usd
gen LC_exch_rate_usd = 1 / LC_exch_rate_per_usd
replace LC_exch_rate_per_usd = 1 if _merge == 1
replace LC_exch_rate_usd = 1 if _merge == 1

gen issue_amount_LC = issue_amount_usd*LC_exch_rate_per_usd
gen os_amount_LC = os_amount_usd*LC_exch_rate_per_usd
gen avail_amount_LC = avail_amount_usd*LC_exch_rate_per_usd

sort instrument_id report_date
gen delta_issue_amount = issue_amount_LC
bys instrument_id: replace delta_issue_amount = issue_amount_LC[_n] - issue_amount_LC[_n-1] if _n != 1

gen delta_os_amount = os_amount_LC
bys instrument_id: replace delta_os_amount = os_amount_LC[_n] - os_amount_LC[_n-1] if _n != 1
gen delta_avail_amount = avail_amount_LC
bys instrument_id: replace delta_avail_amount = avail_amount_LC[_n] - avail_amount_LC[_n-1] if _n != 1
gen _delta_issue_amount = trunc(delta_issue_amount)
gen issue_date_m = mofd(date(issue_date,"YMD")) if _delta_issue_amount != 0

format %tm issue_date_m
replace _delta = 0 if _delta <0

gen final_issuance = _delta*LC_exch_rate_usd
save $cmns1/temp/dcs_details_issuance.dta, replace

* ---------------------------------------------------------------------------------------------------
* Time series: all bonds
* ---------------------------------------------------------------------------------------------------

cap restore
use $cmns1/temp/dcs_details_issuance.dta, clear
gen year = year(report_date)
keep year os_amount_usd instrument_id report_date active
sort instrument_id year report_date
by instrument_id year: gen nvalues = _n
by instrument_id year: gen Nvalues = _N
keep if nvalues == Nvalues
drop report_date nvalues Nvalues
duplicates drop
drop if os_amount_usd <= 0 

mmerge instrument_id using $raw/Factset/fds_stata/sym_isin.dta, umatch(fsym_id)
drop if _merge == 2
mmerge instrument_id using $raw/Factset/fds_stata/sym_isin_hist.dta, umatch(fsym_id) uname(__)
drop if _merge == 2

replace isin = __isin if isin == "" & __isin != ""
drop _merge	__isin	__start_date	__end_date	__most_recent

mmerge instrument_id using $raw/Factset/fds_stata/sym_cusip.dta, umatch(fsym_id)
drop if _merge == 2
mmerge instrument_id using $raw/Factset/fds_stata/dcs_cusip_hist.dta, umatch(fsym_id) uname(__)
drop if _merge == 2

replace cusip = __cusip if cusip == "" & __cusip != ""
drop _merge	__cusip	__start_date __end_date	__most_recent
gen has_isin = 0
replace has_isin = 1 if isin != ""
gen has_cusip = 0
replace has_cusip = 1 if cusip != ""
save $cmns1/temp/factset_issuance_temp.dta, replace

use $cmns1/temp/factset_issuance_temp.dta, clear
mmerge isin using $cmns1/security_master/gcap_security_master_isin.dta, umatch(isin) uname(master_)
drop if _merge == 2
replace cusip = master_cusip if master_cusip != "" & cusip == ""
drop _merge	master*
drop has_isin has_cusip
save $cmns1/temp/factset_issuance_temp_2.dta, replace

cap restore
use $cmns1/temp/factset_issuance_temp_2.dta, clear
gen cusip6 = substr(cusip,1,6)
keep if ~missing(cusip)
mmerge cusip6 using $cmns1/country_master/cmns_aggregation, umatch(issuer_number) ukeep(cgs_domicile country_bg)
keep if _merge != 2
mmerge cusip using $raw/figi/figi_master_compact, umatch(cusip) ukeep(marketsector)
keep if _merge == 1 | _merge == 3
drop _merge
gsort -os_amount_usd
gen teste = os_amount_usd/1000000
sum teste, detail
gsort -teste
drop if teste > 1
save $cmns1/temp/facset_issuance_all_bonds.dta, replace

log close
