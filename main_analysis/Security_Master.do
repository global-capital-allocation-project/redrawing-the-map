* ---------------------------------------------------------------------------------------------------
* Security_Master: This job produces an internal security characteristics masterfile
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Security_Master, replace

* ---------------------------------------------------------------------------------------------------
* Dictionary import
* ---------------------------------------------------------------------------------------------------

* Crosswalk from FIGI asset classes to internal asset classes
import excel using $raw/Internal/Security-Masterfile-Dictionary.xlsx, clear ///
    sheet("FIGI Asset Class Conversion") firstrow
save $cmns1/temp/security_master/figi_asset_class_conversion, replace

* Crosswalk from CGS asset classes to internal asset classes
import excel using $raw/Internal/Security-Masterfile-Dictionary.xlsx, clear ///
    sheet("CGS Security Type Conversion") firstrow
save $cmns1/temp/security_master/cgs_security_type_conversion, replace

* Internal asset class specification
import excel using $raw/Internal/Security-Masterfile-Dictionary.xlsx, clear ///
    sheet("Asset Class Dictionary") firstrow
drop if missing(asset_class1)
save $cmns1/temp/security_master/asset_class_dictionary, replace

* ---------------------------------------------------------------------------------------------------
* Mapping from Factset entity ID to CUSIP6
* ---------------------------------------------------------------------------------------------------

* Compact version of sym_coverage
use $raw/Factset/fds_stata/sym_coverage.dta, clear
keep fsym_id currency fsym_primary_equity_id fsym_primary_listing_id active_flag fref_security_type universe_type
save $raw/Factset/fds_stata/sym_coverage_compact, replace

use $raw/Factset/fds_stata/sym_coverage.dta, clear
keep fsym_id currency universe_type proper_name fref_security_type fref_listing_exchange
save $scratch/factset_sym_coverage, replace

* Construct entity to primary CUSIP6 mapping

* Map each entity to a primary security: primary equities
use $raw/Factset/fds_stata/sym_sec_entity.dta, clear
mmerge fsym_id using $raw/Factset/fds_stata/sym_cusip, unmatched(m)
drop if missing(cusip)
mmerge fsym_id using $raw/Factset/fds_stata/sym_coverage_compact, unmatched(b)
drop _merge
gen is_primary_equity = 0
replace is_primary_equity = 1 if fsym_id == fsym_primary_equity_id
bysort factset_entity_id: egen has_primary_equity = max(is_primary_equity)
drop if has_primary_equity == 1 & is_primary_equity == 0

* Collapse to entity-CUSIP6 links
mmerge fsym_id using $cmns1/temp/factset/sym_cusip_improved, unmatched(m)
drop _merge
drop fsym_primary_equity_id fsym_primary_listing_id
drop if missing(cusip)
gen issuer_number = substr(cusip, 1, 6)
gen sec_rank = 1
replace sec_rank = 0 if universe_type == "EQ"
sort issuer_number sec_rank
bys issuer_number: egen min_sec_rank = min(sec_rank)
drop if sec_rank > min_sec_rank
collapse (firstnm) universe_type, by(factset_entity_id issuer_number)

* Map each entity to a primary issuer number: prioritize according to CGS AI file
mmerge issuer_number using $cmns1/aggregation_sources/cgs_ai_aggregation.dta, unmatched(m) ukeep(ai_cusip6) umatch(cusip6)
rename ai_cusip6 ai_parent_issuer_num
gen is_ai_parent = 0
replace is_ai_parent = 1 if issuer_number == ai_parent_issuer_num
bys factset_entity_id: egen has_ai_parent = max(is_ai_parent)
drop if has_ai_parent == 1 & is_ai_parent == 0

* Map each entity to a primary issuer number: give priority to equity CUSIP6
cap drop *ai*
cap drop _merge
gen issuer_rank = 1
replace issuer_rank = 0 if universe_type == "EQ"
bys factset_entity_id: egen min_issuer_rank = min(issuer_rank)
drop if issuer_rank != min_issuer_rank

* Map each entity to a primary issuer number: for anything remaining, we simply pick at random
set seed 1
bys factset_entity_id: gen _rand = runiform()
bys factset_entity_id: egen max_rand = max(_rand)
drop if float(max_rand) != float(_rand)
drop *rank *rand
drop if missing(factset_entity_id)
save $cmns1/temp/factset_entity_id_to_primary_cusip6, replace

* ---------------------------------------------------------------------------------------------------
* Append all CGS security-level info
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/cgs/ALLMASTER_ISIN.dta, clear
gen cusip = issuer_num + issue_num + issue_check
order cusip
keep cusip issue_description dated_date maturity_date currency_code security_type_description fisn isin issue_entry_date
save $cmns1/temp/cgs/allmaster_isin_compact.dta, replace

use $cmns1/temp/cgs/allmaster_isin_compact.dta, clear
drop if missing(isin)
gsort isin
by isin: keep if _n == 1
save $cmns1/temp/allmaster_isin_compact_isin_unique, replace

use $cmns1/temp/cgs/allmaster_isin_compact.dta, clear
qui mmerge cusip using $cmns1/temp/cgs/all_cusips_universe_all_issuers, unmatched(b) uname(u_)
gen cusip_len = strlen(cusip)
drop if cusip_len != 9
drop cusip_len
drop if _merge == 1
gen allmaster_isin = 0
replace allmaster_isin = 1 if _merge == 3
drop _merge
cap drop u_issuer_number
cap drop u_domicile
cap drop u_issuer_name
save $cmns1/temp/cgs/cgs_security_master, replace

* ---------------------------------------------------------------------------------------------------
* Process TRACE/WRDS headers
* ---------------------------------------------------------------------------------------------------

use $raw/wrds/wrds_bond_returns.dta, clear
gsort CUSIP -DATE
by CUSIP: keep if _n == 1
keep CUSIP bsym ISIN BOND_TYPE SECURITY_LEVEL CONV OFFERING_DATE OFFERING_AMT OFFERING_PRICE PRINCIPAL_AMT MATURITY COUPON DAY_COUNT_BASIS DATED_DATE FIRST_INTEREST_DATE LAST_INTEREST_DATE NCOUPS R_SP R_MR R_FR N_SP N_MR N_FR RATING_NUM RATING_CAT RATING_CLASS
drop OFFERING_PRICE
drop PRINCIPAL_AMT
drop DAY_COUNT_BASIS NCOUPS
rename CUSIP cusip
rename ISIN isin
save $cmns1/temp/wrds_br_header_cusip, replace

use $raw/wrds/wrds_bond_returns.dta, clear
gsort ISIN -DATE
by ISIN: keep if _n == 1
keep CUSIP bsym ISIN BOND_TYPE SECURITY_LEVEL CONV OFFERING_DATE OFFERING_AMT OFFERING_PRICE PRINCIPAL_AMT MATURITY COUPON DAY_COUNT_BASIS DATED_DATE FIRST_INTEREST_DATE LAST_INTEREST_DATE NCOUPS R_SP R_MR R_FR N_SP N_MR N_FR RATING_NUM RATING_CAT RATING_CLASS
drop OFFERING_PRICE
drop PRINCIPAL_AMT
drop DAY_COUNT_BASIS NCOUPS
rename CUSIP cusip
rename ISIN isin
save $cmns1/temp/wrds_br_header_isin, replace

* Process TRACE header
clear
gen file = ""
append using $raw/wrds/trace_master/trace_master_corp_agency.dta
replace file = "corp_agency" if missing(file)
append using $raw/wrds/trace_master/trace_master_abs.dta
replace file = "abs" if missing(file)
append using $raw/wrds/trace_master/trace_master_cmo.dta
replace file = "cmo" if missing(file)
append using $raw/wrds/trace_master/trace_master_mbs.dta
replace file = "mbs" if missing(file)
append using $raw/wrds/trace_master/trace_master_tba.dta
replace file = "tba" if missing(file)
save $cmns1/temp/trace_master_appended, replace

* ---------------------------------------------------------------------------------------------------
* Security ID master: match identifiers from FIGI, CGS, Factset
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/factset/sym_cusip_improved.dta, clear
mmerge cusip using $cmns1/temp/cgs/cgs_security_master, unmatched(b) uname(cgs_)
mmerge cusip using $raw/figi/figi_master_compact_cusip_unique.dta, unmatched(b) uname(figi_)
mmerge fsym_id using $raw/Factset/fds_stata/sym_isin.dta, unmatched(b) uname(fds_)

gen n_isin = 0
replace n_isin = n_isin + 1 if ~missing(figi_isin)
replace n_isin = n_isin + 1 if ~missing(cgs_isin)
replace n_isin = n_isin + 1 if ~missing(fds_isin)

cap drop *_agreement
gen fds_cgs_isin_agreement = .
gen figi_cgs_isin_agreement = .
gen fds_figi_isin_agreement = .
replace fds_cgs_isin_agreement = 0 if n_isin == 3 & fds_isin != cgs_isin
replace fds_cgs_isin_agreement = 1 if n_isin == 3 & fds_isin == cgs_isin
replace figi_cgs_isin_agreement = 0 if n_isin == 3 & figi_isin != cgs_isin
replace figi_cgs_isin_agreement = 1 if n_isin == 3 & figi_isin == cgs_isin
replace fds_figi_isin_agreement = 0 if n_isin == 3 & fds_isin != figi_isin
replace fds_figi_isin_agreement = 1 if n_isin == 3 & fds_isin == figi_isin

gen isin = ""
replace isin = fds_isin if n_isin == 3 & (fds_cgs_isin_agreement == 1 | fds_figi_isin_agreement == 1)
replace isin = cgs_isin if missing(isin) & n_isin == 3 & (fds_cgs_isin_agreement == 1 | figi_cgs_isin_agreement == 1)
replace isin = figi_isin if missing(isin) & n_isin == 3 & (fds_figi_isin_agreement == 1 | figi_cgs_isin_agreement == 1)
replace isin = cgs_isin if missing(isin)
replace isin = fds_isin if missing(isin)
replace isin = figi_isin if missing(isin)

mmerge isin using $raw/figi/figi_master_compact_isin_unique.dta, unmatched(b) uname(figi_isin_)

cap drop _merge
cap drop cgs_isin fds_isin figi_isin
cap drop figi_cusip6 figi_isin_cusip6 *agreement* n_isin

gen figi = figi_figi
replace figi = figi_isin_figi if missing(figi)
drop figi_figi figi_isin_figi

replace cusip = figi_isin_cusip if missing(cusip)
drop figi_isin_cusip
drop cgs_allmaster_isin

order cusip isin figi fsym_id figi_name cgs_fisn cgs_issue_description figi_securitydescription ///
    figi_isin_name figi_isin_securitydescription cgs_security_type_description

gen security_name = ""
replace security_name = figi_name
replace security_name = figi_isin_name if missing(security_name)
replace security_name = cgs_fisn if missing(security_name)

gen security_description = ""
replace security_description = figi_securitydescription
replace security_description = figi_isin_securitydescription if missing(security_description)
replace security_description = cgs_issue_description if missing(security_description)

drop figi_name figi_isin_name cgs_fisn figi_securitydescription figi_isin_securitydescription

rename cgs_currency_code currency

order cusip isin figi fsym_id security_name security_description currency

fcollapse (firstnm) fsym_id security_name security_description currency cgs_security_type_description ///
    cgs_dated_date cgs_maturity_date cgs_issue_entry_date figi_securitytype figi_securitytype2 ///
    figi_marketsector figi_isin_securitytype figi_isin_securitytype2 figi_isin_marketsector, ///
        by(cusip isin figi)

mmerge fsym_id using $raw/Factset/fds_stata/sym_bbg.dta, unmatched(b) uname(fds_bbg_)
cap drop _merge fds_bbg_bbg_ticker
qui replace figi = fds_bbg_bbg_id if missing(figi)
drop fds_bbg_bbg_id

preserve
keep if missing(cusip)
keep fsym_id security_name security_description currency cgs_security_type_description ///
    cgs_dated_date cgs_maturity_date cgs_issue_entry_date figi_securitytype figi_securitytype2 ///
    figi_marketsector figi_isin_securitytype figi_isin_securitytype2 figi_isin_marketsector isin figi cusip
tempfile missing_cusip
save `missing_cusip', replace
restore

drop if missing(cusip)
fcollapse (firstnm) fsym_id security_name security_description currency cgs_security_type_description ///
    cgs_dated_date cgs_maturity_date cgs_issue_entry_date figi_securitytype figi_securitytype2 ///
    figi_marketsector figi_isin_securitytype figi_isin_securitytype2 figi_isin_marketsector isin figi, ///
        by(cusip)
append using `missing_cusip'

gsort cusip
by cusip: gen n_cusip = _N
qui replace n_cusip = . if missing(cusip)
tab n_cusip

gsort isin
by isin: gen priority_isin = _n
qui replace priority_isin = . if missing(isin)
gen secondary_isin = ""
gen tertiary_isin = ""
replace secondary_isin = isin if priority_isin == 2
replace tertiary_isin = isin if priority_isin == 3
replace isin = "" if priority_isin > 1 & ~missing(isin)

cap drop n_isin
gsort isin
by isin: gen n_isin = _N
qui replace n_isin = . if missing(isin)
tab n_isin
cap drop n_cusip priority_isin n_isin

gsort figi
by figi: gen priority_figi = _n
qui replace priority_figi = . if missing(figi)
gen secondary_figi = ""
gen tertiary_figi = ""
replace secondary_figi = figi if priority_figi == 2
replace tertiary_figi = figi if priority_figi == 3
replace figi = "" if priority_figi > 1 & ~missing(figi)

cap drop n_figi
gsort figi
by figi: gen n_figi = _N
qui replace n_figi = . if missing(figi)
tab n_figi

cap drop priority_figi n_figi
order cusip isin figi fsym_id
save $cmns1/temp/gcap_security_masterfile_in_progress, replace

* ---------------------------------------------------------------------------------------------------
* Process Dealogic
* ---------------------------------------------------------------------------------------------------

use $raw/dealogic/stata/DCMSecurityType.dta, clear
keep id name
rename name dealogic_bond_class
rename id typeid
save $cmns1/temp/dlg_bonds_typeid, replace

use $cmns1/temp/tranches_complete.dta, clear

keep cusip isin dcmdealdealid trancheid class commoncode couponpercent issuetypeid markettypeid ///
    currencyisocode currency_issued expectedpricingdate filingdate nationalityisocode typeid name ///
    nationalityofbusinessisocode nationalityofincorporationisocod organisationtypeid NAICS SIC ///
    _pricingdate _announcementdate _settlementdate _maturitydate pricingdate announcementdate ///
    settlementdate maturitydate typeid
drop currencyisocode
drop if missing(cusip) & missing(isin)

cap drop commoncode
order cusip isin typeid
count if ~missing(class)
qui mmerge typeid using $cmns1/temp/dlg_bonds_typeid, unmatched(m)

cap drop _merge
cap drop class typeid issuetypeid markettypeid
drop pricingdate announcementdate settlementdate maturitydate
drop organisationtypeid
rename NAICS dealogic_naics
drop SIC

gen dealogic_issue_date = _pricingdate
replace dealogic_issue_date = _announcementdate if missing(dealogic_issue_date)
replace dealogic_issue_date = _settlementdate if missing(dealogic_issue_date)
drop _pricingdate _announcementdate _settlementdate filingdate expectedpricingdate

format %td dealogic_issue_date
rename _maturitydate dealogic_maturity_date
save $cmns1/temp/secmaster_dlg, replace

use $cmns1/temp/secmaster_dlg, clear
drop if missing(cusip)
keep cusip currency_issued nationalityisocode nationalityofbusinessisocode nationalityofincorporationisocod ///
    dealogic_naics dealogic_bond_class dealogic_issue_date name couponpercent
fcollapse (firstnm) currency_issued nationalityisocode nationalityofbusinessisocode nationalityofincorporationisocod ///
    dealogic_naics dealogic_bond_class dealogic_issue_date name couponpercent, by(cusip)
save $cmns1/temp/dlg_cusip_merge, replace

use $cmns1/temp/secmaster_dlg, clear
drop if missing(isin)
keep isin currency_issued nationalityisocode nationalityofbusinessisocode nationalityofincorporationisocod ///
    dealogic_naics dealogic_bond_class dealogic_issue_date name couponpercent
fcollapse (firstnm) currency_issued nationalityisocode nationalityofbusinessisocode nationalityofincorporationisocod ///
    dealogic_naics dealogic_bond_class dealogic_issue_date name couponpercent, by(isin)
save $cmns1/temp/dlg_isin_merge, replace

* ---------------------------------------------------------------------------------------------------
* Process CGS CFI codes and market values
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/cgs/ALLMASTER_ISIN.dta, clear
gen cusip = issuer_num + issue_num + issue_check
keep cusip *cfi*
drop if missing(cusip)
drop if missing(us_cfi_code) & missing(iso_cfi_code)
save $cmns1/temp/allmaster_isin_cfi, replace

use $cmns1/holdings_master/mns_security_summary.dta, clear
collapse (sum) marketvalue_usd, by(cusip year)
collapse (max) marketvalue_usd, by(cusip)
gen max_mns_value = marketvalue_usd / 1e9
keep cusip max_mns_value
drop if missing(cusip)
save $cmns1/temp/max_mns_values, replace

* ---------------------------------------------------------------------------------------------------
* Merge in dates, asset class, TRACE info
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/gcap_security_masterfile_in_progress, clear

* Process dates
gen maturity_date = date(cgs_maturity_date, "YMD")
format %td maturity_date
drop cgs_maturity_date

gen issue_entry_date = date(cgs_issue_entry_date, "YMD")
format %td issue_entry_date

gen dated_date = date(cgs_dated_date, "YMD")
format %td dated_date

rename issue_entry_date issuance_date
replace issuance_date = dated_date if missing(issuance_date)
drop cgs_issue_entry_date cgs_dated_date dated_date

order cusip isin security_name security_description figi_securitytype figi_securitytype2 figi_marketsector ///
    figi_isin_securitytype figi_isin_securitytype2 figi_isin_marketsector cgs_security_type_description

* Add the historical data from Factset
mmerge cusip using $raw/Factset/fds_stata/sym_cusip_hist.dta, unmatched(b) ukeep(fsym_id) update
mmerge isin using $raw/Factset/fds_stata/sym_isin_hist.dta, unmatched(b) ukeep(fsym_id) update

drop if missing(cusip) & missing(isin)

* Merge in Morningstar, Factset, Dealogic, CGS
mmerge cusip using $cmns1/holdings_master/Internal_Class_NonUS_US.dta, unmatched(m)
mmerge fsym_id using $scratch/factset_sym_coverage, unmatched(m) uname(factset_)
mmerge cusip using $cmns1/temp/allmaster_isin_cfi, unmatched(m)
mmerge cusip using $cmns1/temp/max_mns_values, unmatched(m)
mmerge cusip using $cmns1/temp/dlg_cusip_merge, unmatched(m) uname(dc_)
mmerge isin using $cmns1/temp/dlg_isin_merge, unmatched(m) uname(di_)
mmerge cusip using $cmns1/temp/wrds_br_header_cusip, unmatched(m) uname(tc_)
mmerge isin using $cmns1/temp/wrds_br_header_isin, unmatched(m) uname(ti_)

gsort cusip
by cusip: gen N = _N
assert N == 1 if ~missing(cusip)
drop N

gsort isin
by isin: gen N = _N
assert N == 1 if ~missing(isin)
drop N

gsort cusip isin

cap drop _merge
cap drop tc_isin ti_cusip
foreach var in bsym BOND_TYPE SECURITY_LEVEL CONV OFFERING_DATE OFFERING_AMT MATURITY COUPON DATED_DATE ///
    FIRST_INTEREST_DATE LAST_INTEREST_DATE R_SP R_MR R_FR N_SP N_MR N_FR RATING_NUM RATING_CAT RATING_CLASS {
        di "`var'"
        replace tc_`var' = ti_`var' if missing(tc_`var')
}
cap drop ti_*
drop if missing(cusip) & missing(isin)
save $scratch/gcap_security_masterfile_in_progress2, replace

* ---------------------------------------------------------------------------------------------------
* Asset class harmonization
* ---------------------------------------------------------------------------------------------------

use $scratch/gcap_security_masterfile_in_progress2, clear
replace figi_marketsector = figi_isin_marketsector if missing(figi_marketsector)
replace figi_securitytype = figi_isin_securitytype if missing(figi_securitytype)
replace figi_securitytype2 = figi_isin_securitytype2 if missing(figi_securitytype2)
drop figi_isin*

mmerge figi_marketsector figi_securitytype figi_securitytype2 ///
    using $cmns1/temp/security_master/figi_asset_class_conversion, ///
        umatch(marketsector securitytype securitytype2) unmatched(m)
drop _merge

mmerge asset_class1 asset_class2 asset_class3 using $cmns1/temp/security_master/asset_class_dictionary
drop _merge
save $scratch/gcap_security_masterfile_in_progress3.dta, replace

use $scratch/gcap_security_masterfile_in_progress3.dta, clear

cap drop tc_R_* tc_N_*
cap drop tc_FIRST_INTEREST_DATE tc_LAST_INTEREST_DATE
cap drop tc_RATING_* tc_OFFERING_AMT tc_bsym tc_BOND_TYPE tc_SECURITY_LEVEL
cap drop tc_CONV
cap drop tc_DATED_DATE

replace issuance_date = dc_dealogic_issue_date if missing(issuance_date)
replace issuance_date = di_dealogic_issue_date if missing(issuance_date)
replace issuance_date = tc_OFFERING_DATE if missing(issuance_date)
drop dc_dealogic_issue_date di_dealogic_issue_date tc_OFFERING_DATE

order cusip isin figi currency dc_currency_issued di_currency_issued factset_currency

replace currency = factset_currency if missing(currency)
replace currency = dc_currency_issued if missing(currency)
replace currency = di_currency_issued if missing(currency)

drop dc_currency_issued di_currency_issued factset_currency
order cusip isin figi security_name security_description dc_name di_name factset_proper_name

replace security_name = factset_proper_name if missing(security_name)
replace security_name = di_name if missing(security_name)
replace security_name = dc_name if missing(security_name)
replace security_name = security_description if missing(security_name)
replace security_name = upper(security_name)

drop security_description dc_name di_name factset_proper_name
cap drop count
gsort -max_mns_value
save $scratch/gcap_security_masterfile_in_progress4.dta, replace

use $scratch/gcap_security_masterfile_in_progress4.dta, clear

replace asset_class1 = "Equity" if missing(asset_class1) & mns_class == "E"
replace asset_class2 = "Common Equity" if missing(asset_class2) & mns_class == "E"
replace asset_class3 = "Common Equity" if missing(asset_class3) & mns_class == "E"

replace asset_class1 = "Fund Share" if missing(asset_class1) & mns_class == "MF"
replace asset_class2 = "Fund Share" if missing(asset_class2) & mns_class == "MF"
replace asset_class3 = "Fund Share" if missing(asset_class3) & mns_class == "MF"

replace asset_class1 = "Cash" if missing(asset_class1) & mns_class == "C"
replace asset_class2 = "Cash" if missing(asset_class2) & mns_class == "C"
replace asset_class3 = "Cash" if missing(asset_class3) & mns_class == "C"

replace asset_class1 = "Derivative" if missing(asset_class1) & mns_class == "D"
replace asset_class2 = "Other Derivative" if missing(asset_class2) & mns_class == "D"
replace asset_class3 = "Other Derivative" if missing(asset_class3) & mns_class == "D"

replace asset_class1 = "Bond" if missing(asset_class1) & mns_class == "B" & inlist(mns_subclass, "S", "A")
replace asset_class2 = "Sovereign Bond" if missing(asset_class2) & mns_class == "B" & inlist(mns_subclass, "S", "A")
replace asset_class3 = "Sovereign Bond" if missing(asset_class3) & mns_class == "B" & inlist(mns_subclass, "S", "A")

replace asset_class1 = "Equity" if missing(asset_class1) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "SHARE")
replace asset_class2 = "Common Equity" if missing(asset_class2) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "SHARE")
replace asset_class3 = "Common Equity" if missing(asset_class3) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "SHARE")

replace asset_class1 = "Equity" if missing(asset_class1) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "TEMP")
replace asset_class2 = "Common Equity" if missing(asset_class2) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "TEMP")
replace asset_class3 = "Common Equity" if missing(asset_class3) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "TEMP")

replace asset_class1 = "Derivative" if missing(asset_class1) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "RIGHT")
replace asset_class2 = "Equity Right" if missing(asset_class2) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "RIGHT")
replace asset_class3 = "Equity Right" if missing(asset_class3) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "RIGHT")

replace asset_class1 = "Equity" if missing(asset_class1) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "ADR", "GDR", "DR", "NVDR")
replace asset_class2 = "Common Equity" if missing(asset_class2) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "ADR", "GDR", "DR", "NVDR")
replace asset_class3 = "Depositary Receipt" if missing(asset_class3) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "ADR", "GDR", "DR", "NVDR")

replace asset_class1 = "Fund Share" if missing(asset_class1) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "ETF_ETF", "UNIT", "ETF_NAV", "ETF_UVI")
replace asset_class2 = "Fund Share" if missing(asset_class2) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "ETF_ETF", "UNIT", "ETF_NAV", "ETF_UVI")
replace asset_class3 = "Fund Share" if missing(asset_class3) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "ETF_ETF", "UNIT", "ETF_NAV", "ETF_UVI")

replace asset_class1 = "Fund Share" if missing(asset_class1) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "MF_O", "MF_C")
replace asset_class2 = "Fund Share" if missing(asset_class2) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "MF_O", "MF_C")
replace asset_class3 = "Fund Share" if missing(asset_class3) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "MF_O", "MF_C")

replace asset_class1 = "Equity" if missing(asset_class1) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "PREFEQ", "PREF")
replace asset_class2 = "Preferred Equity" if missing(asset_class2) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "PREFEQ", "PREF")
replace asset_class3 = "Preferred Equity" if missing(asset_class3) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "PREFEQ", "PREF")

replace asset_class1 = "Bond" if missing(asset_class1) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "CORP")
replace asset_class2 = "Corporate Bond" if missing(asset_class2) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "CORP")
replace asset_class3 = "Other Corporate Bond" if missing(asset_class3) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "CORP")

replace asset_class1 = "Bond" if missing(asset_class1) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "MUNI")
replace asset_class2 = "Local Government Bond" if missing(asset_class2) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "MUNI")
replace asset_class3 = "Local Government Bond" if missing(asset_class3) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "MUNI")

replace asset_class1 = "Bond" if missing(asset_class1) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "SOV")
replace asset_class2 = "Sovereign Bond" if missing(asset_class2) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "SOV")
replace asset_class3 = "Sovereign Bond" if missing(asset_class3) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "SOV")

replace asset_class1 = "Bond" if missing(asset_class1) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "ABS")
replace asset_class2 = "Asset-Backed Security" if missing(asset_class2) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "ABS")
replace asset_class3 = "ABS" if missing(asset_class3) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "ABS")

replace asset_class1 = "Derivative" if missing(asset_class1) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "WARRANT")
replace asset_class2 = "Equity Warant" if missing(asset_class2) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "WARRANT")
replace asset_class3 = "Equity Warant" if missing(asset_class3) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "WARRANT")

replace dc_dealogic_bond_class = di_dealogic_bond_class if missing(dc_dealogic_bond_class)
replace asset_class1 = "Bond" if missing(asset_class1) & (inlist(dc_dealogic_bond_class, "Credit Card-Backed Ctfs", "Asset-Backed Notes", "Amortizing Notes", "Auto Lease-Backed Ctfs", "Amortizing Debentures") | inlist(dc_dealogic_bond_class, "Amortizing Ctfs", "Asset Backed Promissory Notes", "Auction Preferred Stock", "Asset-Backed Ctfs") | inlist(dc_dealogic_bond_class, "Asset-Backed Bonds", "Auction Mkt Pref Shares", "Auto Receivables Backed-Ctfs", "Auto Lease Asset Backed-Notes"))
replace asset_class2 = "Asset-Backed Security" if missing(asset_class2) & (inlist(dc_dealogic_bond_class, "Credit Card-Backed Ctfs", "Asset-Backed Notes", "Amortizing Notes", "Auto Lease-Backed Ctfs", "Amortizing Debentures") | inlist(dc_dealogic_bond_class, "Amortizing Ctfs", "Asset Backed Promissory Notes", "Auction Preferred Stock", "Asset-Backed Ctfs") | inlist(dc_dealogic_bond_class, "Asset-Backed Bonds", "Auction Mkt Pref Shares", "Auto Receivables Backed-Ctfs", "Auto Lease Asset Backed-Notes"))
replace asset_class3 = "ABS" if missing(asset_class3) & (inlist(dc_dealogic_bond_class, "Credit Card-Backed Ctfs", "Asset-Backed Notes", "Amortizing Notes", "Auto Lease-Backed Ctfs", "Amortizing Debentures") | inlist(dc_dealogic_bond_class, "Amortizing Ctfs", "Asset Backed Promissory Notes", "Auction Preferred Stock", "Asset-Backed Ctfs") | inlist(dc_dealogic_bond_class, "Asset-Backed Bonds", "Auction Mkt Pref Shares", "Auto Receivables Backed-Ctfs", "Auto Lease Asset Backed-Notes"))
drop dc_dealogic_bond_class di_dealogic_bond_class

save $scratch/gcap_security_masterfile_in_progress5.dta, replace

use $scratch/gcap_security_masterfile_in_progress5.dta, clear

replace asset_class1 = "Equity" if missing(asset_class1) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "STRUCT")
replace asset_class2 = "Other Equity" if missing(asset_class2) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "STRUCT")
replace asset_class3 = "Other Equity" if missing(asset_class3) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "STRUCT")

replace asset_class1 = "Equity" if missing(asset_class1) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "ALIEN", "RIGHT")
replace asset_class2 = "Other Equity" if missing(asset_class2) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "ALIEN", "RIGHT")
replace asset_class3 = "Other Equity" if missing(asset_class3) & factset_universe_type == "EQ" & inlist(factset_fref_security_type, "ALIEN", "RIGHT")

replace asset_class1 = "Bond" if missing(asset_class1) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "MBS")
replace asset_class2 = "Asset-Backed Security" if missing(asset_class2) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "MBS")
replace asset_class3 = "MBS" if missing(asset_class3) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "MBS")

replace asset_class1 = "Loan" if missing(asset_class1) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "BANK_LOAN")
replace asset_class2 = "Loan" if missing(asset_class2) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "BANK_LOAN")
replace asset_class3 = "Loan" if missing(asset_class3) & factset_universe_type == "FI" & inlist(factset_fref_security_type, "BANK_LOAN")

mmerge cgs_security_type_description using $cmns1/temp/security_master/cgs_security_type_conversion, ///
    unmatched(m) update
drop _merge

replace asset_class1 = "Other" if missing(asset_class1) & ~missing(cgs_security_type_description)
replace asset_class2 = "Other" if missing(asset_class2) & ~missing(cgs_security_type_description)
replace asset_class3 = "Other" if missing(asset_class3) & ~missing(cgs_security_type_description)
save $scratch/gcap_security_masterfile_in_progress6.dta, replace

* ---------------------------------------------------------------------------------------------------
* Add asset class codes
* ---------------------------------------------------------------------------------------------------

use $scratch/gcap_security_masterfile_in_progress6.dta, clear
replace asset_class1 = "Unknown" if missing(asset_class1)
replace asset_class2 = "Unknown" if missing(asset_class2)
replace asset_class3 = "Unknown" if missing(asset_class3)
mmerge asset_class1 asset_class2 asset_class3 using ///
    $cmns1/temp/security_master/asset_class_dictionary, unmatched(m) update
drop _merge

forval i=1/3 {
    assert ~missing(asset_class`i')
}

replace class_code1 = "B" if asset_class1 == "Bond" & asset_class2 == "Other Bond" & asset_class3 == "Other Bond"
replace class_code2 = "BX" if asset_class1 == "Bond" & asset_class2 == "Other Bond" & asset_class3 == "Other Bond"
replace class_code3 = "BX" if asset_class1 == "Bond" & asset_class2 == "Other Bond" & asset_class3 == "Other Bond"

replace class_code1 = "D" if asset_class1 == "Derivative" & asset_class2 == "Equity Right" & asset_class3 == "Equity Right"
replace class_code2 = "DER" if asset_class1 == "Derivative" & asset_class2 == "Equity Right" & asset_class3 == "Equity Right"
replace class_code3 = "DER" if asset_class1 == "Derivative" & asset_class2 == "Equity Right" & asset_class3 == "Equity Right"

replace asset_class2 = "Equity Warrant" if asset_class2 == "Equity Warant"
replace asset_class3 = "Equity Warrant" if asset_class3 == "Equity Warant"

replace class_code1 = "D" if asset_class1 == "Derivative" & asset_class2 == "Equity Warrant" & asset_class3 == "Equity Warrant"
replace class_code2 = "DEW" if asset_class1 == "Derivative" & asset_class2 == "Equity Warrant" & asset_class3 == "Equity Warrant"
replace class_code3 = "DEW" if asset_class1 == "Derivative" & asset_class2 == "Equity Warrant" & asset_class3 == "Equity Warrant"

replace class_code1 = "E" if asset_class1 == "Equity" & asset_class2 == "Other Equity" & asset_class3 == "Other Equity"
replace class_code2 = "EX" if asset_class1 == "Equity" & asset_class2 == "Other Equity" & asset_class3 == "Other Equity"
replace class_code3 = "EX" if asset_class1 == "Equity" & asset_class2 == "Other Equity" & asset_class3 == "Other Equity"

forval i=1/3 {
    assert ~missing(class_code`i')
}

order cusip isin figi security_name asset_class* class_code*

* Also merge in the data from DCS
mmerge fsym_id using $cmns1/temp/dcs_details_static, unmatched(m) umatch(instrument_id) uname(dcs_)
drop _merge
gsort -max_mns_value
save $scratch/gcap_security_masterfile_in_progress7.dta, replace

* ---------------------------------------------------------------------------------------------------
* Harmonize other bond characteristics
* ---------------------------------------------------------------------------------------------------

* Compactify
use $scratch/gcap_security_masterfile_in_progress7.dta, clear
keep cusip isin figi security_name asset_class1 asset_class2 asset_class3 class_code1 class_code2 class_code3 currency fsym_id maturity_date issuance_date max_mns_value dc_nationalityisocode dc_nationalityofbusinessisocode dc_nationalityofincorporationiso dc_dealogic_naics dc_couponpercent di_nationalityisocode di_nationalityofbusinessisocode di_nationalityofincorporationiso di_dealogic_naics di_couponpercent dcs_* tc_*
replace dc_nationalityisocode = di_nationalityisocode if missing(dc_nationalityisocode)
replace dc_nationalityofbusiness = di_nationalityofbusiness if missing(dc_nationalityofbusiness)
replace dc_nationalityofinc = di_nationalityofinc if missing(dc_nationalityofinc)
replace dc_dealogic_naics = di_dealogic_naics if missing(dc_dealogic_naics)
replace dc_coupon = di_coupon if missing(dc_coupon)
drop di_*
rename dc_* dl_*
rename dl_dealogic_* dl_*
save $scratch/gcap_security_masterfile_in_progress7_compact, replace

* Merge in the DCS characteristics data
use $scratch/gcap_security_masterfile_in_progress7_compact, clear
drop dcs_debt_code dcs_factset_entity_id
replace currency = dcs_issuance_currency if missing(currency)
drop dcs_issuance_currency
drop dcs_coupon_index2
drop dcs_instrument_description
rename dcs_asset_desc asset_desc
rename dcs_collateral_type collateral_type
drop dl_nat*
gen _dcs_issue_date = date(dcs_issue_date, "YMD")
format %td _dcs_issue_date
order *iss*, last
replace issuance_date = _dcs_issue_date if missing(issuance_date)
drop dcs_issue_date _dcs_issue_date
drop dcs_coupon_margin_*
order *coupon*, last

replace dl_couponpercent = dcs_coupon_rate_min if missing(dl_couponpercent)
rename dl_couponpercent coupon_percent
rename dcs_coupon_code coupon_type
rename dcs_coupon_index coupon_index
drop dcs_coupon_rate*

drop dcs_maturity_date_range_flag
rename dl_naics naics
drop dcs_maturity_year_start*
tostring dcs_maturity_year, replace
tostring dcs_maturity_year_month, replace
tostring dcs_maturity_year_day, replace

gen dcs_maturity = dcs_maturity_year + "-" + dcs_maturity_year_month + "-" + dcs_maturity_year_day
gen _dcs_maturity = date(dcs_maturity, "YMD")
format %td _dcs_maturity
replace maturity_date = _dcs_maturity if missing(maturity_date)
drop dcs_maturity* _dcs_maturity

* Also harmonize the remaining TRACE variables
replace maturity_date = tc_MATURITY if missing(maturity_date)
replace coupon_percent = tc_COUPON if missing(coupon_percent)
drop tc_*
save $scratch/gcap_security_masterfile_in_progress8_compact.dta, replace

* ---------------------------------------------------------------------------------------------------
* Add in more info
* ---------------------------------------------------------------------------------------------------

* Generate static version of Dealogic data
use $cmns1/issuance_master/dealogic_dcm_issuance_complete.dta, clear
count
keep coupondescription coupondetails couponfrequencyid couponpercent couponsetfrequencyid couponstepfrequencyid cusip domesticisin expectedmaturitydate iscallable isfloatingrate isgovernmentguaranteed ishighyield isinternational isputtable issubordinated isusdomestic isusmarketed maxcouponpercent mincouponpercent seniorityid currencyisocode currency_issued isin expectedpricingdate filingdate isinvestmentgrade name _pricingdate _announcementdate _settlementdate _maturitydate pricingdate announcementdate settlementdate maturitydate
order cusip isin
drop if missing(cusip) & missing(isin)
gsort cusip isin
by cusip isin: keep if _n == 1
unique cusip isin
order cusip isin name

replace couponpercent = mincouponpercent if missing(couponpercent)
drop mincouponpercent maxcouponpercent

cap drop coupondescription coupondetails couponfrequencyid
cap drop couponsetfrequencyid couponstepfrequencyid
cap drop currencyisocode
order is*, last
order cusip isin

cap drop domesticisin
cap drop seniorityid

rename isin _isin
foreach var of varlist is* {
    replace `var' = "1" if `var' == "True"
    replace `var' = "0" if `var' == "False"
    destring `var', replace
}

cap drop expectedmaturitydate expectedpricingdate filingdate
cap drop pricingdate announcementdate settlementdate maturitydate

gen issuance_date = _settlementdate
replace issuance_date = _pricingdate if missing(issuance_date)
replace issuance_date = _announcementdate if missing(issuance_date)
rename _maturitydate maturity_date
drop _*date

format %td issuance_date
order is*, last
rename _isin isin
order cusip isin name issuance_date maturity_date

save $scratch/dealogic_static_compact, replace

* Further versions for merges
use $scratch/dealogic_static_compact, clear
drop if missing(cusip)
bys cusip: keep if _n == 1
save $scratch/dealogic_static_compact_cusip, replace

use $scratch/dealogic_static_compact, clear
drop if missing(isin)
bys isin: keep if _n == 1
save $scratch/dealogic_static_compact_isin, replace

* Merge in further Dealogic info
use $scratch/gcap_security_masterfile_in_progress8_compact, clear
mmerge cusip using $scratch/dealogic_static_compact_cusip, uname(d_) unmatched(m)
drop _merge
replace security_name = d_name if missing(security_name)
replace issuance_date = d_issuance_date if missing(issuance_date)
replace maturity_date = d_maturity_date if missing(maturity_date)
replace coupon_percent = d_couponpercent if missing(coupon_percent)
replace currency = d_currency_issued if missing(currency)

cap drop d_*
mmerge isin using $scratch/dealogic_static_compact_isin, uname(d_) unmatched(m)
drop _merge
replace security_name = d_name if missing(security_name)
replace issuance_date = d_issuance_date if missing(issuance_date)
replace maturity_date = d_maturity_date if missing(maturity_date)
replace coupon_percent = d_couponpercent if missing(coupon_percent)
replace currency = d_currency_issued if missing(currency)
cap drop d_*
replace security_name = upper(security_name)

save $scratch/gcap_security_masterfile_in_progress9_compact, replace

* ---------------------------------------------------------------------------------------------------
* Finalize the files
* ---------------------------------------------------------------------------------------------------

use $scratch/gcap_security_masterfile_in_progress9_compact, clear
compress
save $scratch/gcap_security_masterfile_in_progress10_compact, replace

use $scratch/gcap_security_masterfile_in_progress10_compact, clear
keep cusip isin figi security_name asset_class1 asset_class2 asset_class3 class_code1 class_code2 class_code3 currency fsym_id maturity_date issuance_date coupon_percent
save $cmns1/security_master/gcap_security_master, replace

use $cmns1/security_master/gcap_security_master, clear
drop if missing(cusip)
save $cmns1/security_master/gcap_security_master_cusip, replace

use $cmns1/security_master/gcap_security_master, clear
drop if missing(isin)
save $cmns1/security_master/gcap_security_master_isin, replace

log close
