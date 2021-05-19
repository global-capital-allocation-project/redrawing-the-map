* ---------------------------------------------------------------------------------------------------
* Process_Insurance_Data: Generates rellocation matrices using US insurer holdings
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Process_Insurance_Data, replace

* ---------------------------------------------------------------------------------------------------
* Process asset classes in insurance data
* ---------------------------------------------------------------------------------------------------

* Load in insurance data
use $cmns1/insurance/insurance_master/all_insurance_master.dta, clear
qui keep if asset_class == "Bonds"
qui drop if missing(cusip)
gen issuer_number = substr(cusip, 1, 6)
qui mmerge issuer_number using $cmns1/country_master/cmns_aggregation.dta, unmatched(m)
qui drop *source* _merge
drop if missing(cgs_domicile)
gen residency = "US" if cgs_domicile == "USA"
replace residency = "Non-US" if cgs_domicile != "USA"

* Merge in asset class classifications
qui mmerge cusip using $cmns1/holdings_master/Internal_Class_NonUS_US.dta, unmatched(m)
qui mmerge cusip using $raw/figi/figi_master_compact_cusip_unique.dta, unmatched(m)

* Harmonize the classifications
qui cap drop bond_category
qui gen bond_category = ""
qui replace bond_category = "Bonds - Corporate" if mns_class == "B" & ~inlist(mns_subclass, "S", "A", "LS", "SF", "SV")
qui replace bond_category = "Bonds - Government" if mns_class == "B" & inlist(mns_subclass, "S", "A", "LS")
qui replace bond_category = "Bonds - Structured Finance" if mns_class == "B" & inlist(mns_subclass, "SF")
qui replace bond_category = "Bonds - Sovranational" if mns_class == "B" & inlist(mns_subclass, "SV")
qui replace bond_category = "Bonds - Corporate" if missing(bond_category) & marketsector == "Corp"
qui replace bond_category = "Bonds - Government" if missing(bond_category) & marketsector == "Govt"
qui replace bond_category = "Bonds - Government" if missing(bond_category) & marketsector == "Muni"
qui replace bond_category = "Bonds - Structured Finance" if missing(bond_category) & marketsector == "Mtge"
qui drop if inlist(marketsector, "Equity", "Pfd") & missing(bond_category)
qui drop if inlist(Asset_Issuer_Type, "Bank Loans", "Unaffiliated Bank Loans") & missing(bond_category)
qui replace bond_category = "Bonds - Government" if missing(bond_category) & ///
    inlist(Asset_Issuer_Type, "Foreign Gov't", "Gov't Agency", "US Federal Gov't", "Subdiv US States", "US States, Poss")
qui replace bond_category = "Bonds - Corporate" if missing(bond_category) & ///
    inlist(Asset_Issuer_Type, "Industrial")
qui drop if inlist(Asset_Issuer_Type, "Hybrid Securities", "SVO Identified Funds") & missing(bond_category)
qui drop if missing(bond_category)

* Save the dta
keep cusip bond_category
duplicates drop
save $cmns1/temp/insurance_bond_classifications, replace

* ---------------------------------------------------------------------------------------------------
* Insurance reallocation matrices: corporate bonds
* ---------------------------------------------------------------------------------------------------

* Prepare reallocation shares
use $cmns1/insurance/insurance_master/all_insurance_master.dta, clear
keep if asset_class == "Bonds"
drop if missing(cusip)
gen issuer_number = substr(cusip, 1, 6)
qui mmerge issuer_number using $cmns1/country_master/cmns_aggregation.dta, unmatched(m)
drop *source* _merge
format %tq date_q
drop if missing(cgs_domicile) | missing(country_bg)
qui mmerge cusip using $cmns1/temp/insurance_bond_classifications, unmatched(m)
qui mmerge cusip using $raw/figi/figi_master_compact_cusip_unique.dta, unmatched(m)
cap drop _merge
qui keep if bond_category == "Bonds - Corporate"

* Correct classifications for some misclassified CYM securities
drop if ~missing(Asset_Type) & Asset_Type != "Long-term Bond, Issuer Obligations"
drop if ~missing(As_Reported_Asset_Type) & As_Reported_Asset_Type != "Long-term Bond, Issuer Obligations"
drop if strpos(Issuer_Name, "Lima Metro Line") & cgs_domicile == "CYM" & country_bg == "CYM"
drop if strpos(Issuer_Name, "KKR") & cgs_domicile == "CYM" & country_bg == "CYM"
drop if strpos(Issuer_Name, "Union 15 SPC") & cgs_domicile == "CYM" & country_bg == "CYM"
drop if strpos(Issuer_Name, "Air Duke") & cgs_domicile == "CYM" & country_bg == "CYM"
drop if strpos(Issuer_Name, "ALS 3") & cgs_domicile == "CYM" & country_bg == "CYM"
replace country_bg = "OMN" if strpos(Issuer_Name, "Lamar Funding") & cgs_domicile == "CYM" & country_bg == "CYM"
replace country_bg = "CHN" if strpos(Issuer_Name, "China Overseas Land") & cgs_domicile == "CYM" & country_bg == "CYM"
replace country_bg = "CHN" if strpos(Issuer_Name, "CDBL Funding") & cgs_domicile == "CYM" & country_bg == "CYM"
drop if strpos(Issuer_Name, "LIMEROCK III") & cgs_domicile == "CYM" & country_bg == "CYM"
drop if strpos(Issuer_Name, "NORTH END CAP") & cgs_domicile == "CYM" & country_bg == "CYM"
drop if strpos(Issuer_Name, "Gate Capital Cayman") & cgs_domicile == "CYM" & country_bg == "CYM"
drop if strpos(Issuer_Name, "INTERNATIONAL DIVERSIFIED PMT") & cgs_domicile == "CYM" & country_bg == "CYM"
drop if inlist(marketsector, "Mtge", "Muni", "Pfd", "Govt")
save $cmns1/temp/insurance_corporate_bond_positions_2017.dta, replace

* Collapse and save reallocation sahres
gcollapse (sum) marketvalue_usd, by(cgs_domicile country_bg)
bys cgs_domicile: egen residencyTotal = total(marketvalue_usd)
gen reallocation_share = marketvalue_usd / residencyTotal
keep cgs_domicile country_bg reallocation_share marketvalue_usd residencyTotal
save $cmns1/temp/reallocation_shares_insurers_tmp_corp, replace

* Full version
use $cmns1/temp/reallocation_shares_insurers_tmp_corp, clear
keep country_bg
duplicates drop
gen in_country_bg = 1
rename country_bg cgs_domicile
tempfile in_country_bg
save `in_country_bg', replace

use $cmns1/temp/reallocation_shares_insurers_tmp_corp, clear
keep cgs_domicile
duplicates drop
gen in_cgs_domicile = 1
rename cgs_domicile country_bg
tempfile in_cgs_domicile
save `in_cgs_domicile', replace

use $cmns1/temp/reallocation_shares_insurers_tmp_corp, clear
sort cgs_domicile country_bg

keep cgs_domicile country_bg reallocation_share
qui reshape wide reallocation_share, i(cgs_domicile) j(country_bg) string
foreach var of varlist * {
    if "`var'" != "cgs_domicile" {
        qui replace `var' = `var' * 100
    }
}
rename reallocation_share* *
sort cgs_domicile
order cgs_domicile

export excel using $cmns1/insurance/reallocation_matrices/insurance_reallocation_matrix_corp_2017.xls, replace firstrow(var)

* ---------------------------------------------------------------------------------------------------
* Insurance reallocation matrices: government bonds
* ---------------------------------------------------------------------------------------------------

* Prepare reallocation shares
use $cmns1/insurance/insurance_master/all_insurance_master.dta, clear
keep if asset_class == "Bonds"
drop if missing(cusip)
gen issuer_number = substr(cusip, 1, 6)
qui mmerge issuer_number using $cmns1/country_master/cmns_aggregation.dta, unmatched(m)
drop *source* _merge
format %tq date_q
drop if missing(cgs_domicile)
drop if missing(country_bg)
qui mmerge cusip using $raw/figi/figi_master_compact_cusip_unique.dta, unmatched(m)
drop _merge

keep if marketsector == "Govt" | marketsector == "Muni"
collapse (sum) marketvalue_usd, by(cgs_domicile country_bg)
bys cgs_domicile: egen residencyTotal = total(marketvalue_usd)
gen reallocation_share = marketvalue_usd / residencyTotal
keep cgs_domicile country_bg reallocation_share marketvalue_usd residencyTotal
save $cmns1/temp/reallocation_shares_insurers_tmp_govt, replace

* Full version
use $cmns1/temp/reallocation_shares_insurers_tmp_govt, clear
keep country_bg
duplicates drop
gen in_country_bg = 1
rename country_bg cgs_domicile
tempfile in_country_bg
save `in_country_bg', replace

use $cmns1/temp/reallocation_shares_insurers_tmp_govt, clear
keep cgs_domicile
duplicates drop
gen in_cgs_domicile = 1
rename cgs_domicile country_bg
tempfile in_cgs_domicile
save `in_cgs_domicile', replace

use $cmns1/temp/reallocation_shares_insurers_tmp_govt, clear
sort cgs_domicile country_bg

keep cgs_domicile country_bg reallocation_share
qui reshape wide reallocation_share, i(cgs_domicile) j(country_bg) string
foreach var of varlist * {
    if "`var'" != "cgs_domicile" {
        qui replace `var' = `var' * 100
    }
}
rename reallocation_share* *
sort cgs_domicile
order cgs_domicile

export excel using $cmns1/insurance/reallocation_matrices/insurance_reallocation_matrix_govt_2017.xls, replace firstrow(var)

* ---------------------------------------------------------------------------------------------------
* Insurance reallocation matrices: equities
* ---------------------------------------------------------------------------------------------------

* Prepare reallocation shares
use $cmns1/insurance/insurance_master/all_insurance_master.dta, clear
keep if asset_class == "Common Equities"
drop if missing(cusip)
gen issuer_number = substr(cusip, 1, 6)
qui mmerge issuer_number using $cmns1/country_master/cmns_aggregation.dta, unmatched(m)
drop *source* _merge
format %tq date_q
drop if missing(cgs_domicile)
drop if missing(country_bg)
qui mmerge cusip using $raw/figi/figi_master_compact_cusip_unique.dta, unmatched(m)
drop _merge

* Giant position, likely misreported
drop if cusip == "D39632191" & Entity_Name == "Berkshire Hathaway Inc. (SNL P&C Group)"

drop if inlist(marketsector, "Govt", "Mtge", "Corp")
collapse (sum) marketvalue_usd, by(cgs_domicile country_bg)
bys cgs_domicile: egen residencyTotal = total(marketvalue_usd)
gen reallocation_share = marketvalue_usd / residencyTotal
keep cgs_domicile country_bg reallocation_share marketvalue_usd residencyTotal
save $cmns1/temp/reallocation_shares_insurers_tmp_equity, replace

* Full version
use $cmns1/temp/reallocation_shares_insurers_tmp_equity, clear
keep country_bg
duplicates drop
gen in_country_bg = 1
rename country_bg cgs_domicile
tempfile in_country_bg
save `in_country_bg', replace

use $cmns1/temp/reallocation_shares_insurers_tmp_equity, clear
keep cgs_domicile
duplicates drop
gen in_cgs_domicile = 1
rename cgs_domicile country_bg
tempfile in_cgs_domicile
save `in_cgs_domicile', replace

use $cmns1/temp/reallocation_shares_insurers_tmp_equity, clear
sort cgs_domicile country_bg

keep cgs_domicile country_bg reallocation_share
qui reshape wide reallocation_share, i(cgs_domicile) j(country_bg) string
foreach var of varlist * {
    if "`var'" != "cgs_domicile" {
        qui replace `var' = `var' * 100
    }
}
rename reallocation_share* *
sort cgs_domicile
order cgs_domicile

export excel using $cmns1/insurance/reallocation_matrices/insurance_reallocation_matrix_equity_2017.xls, replace firstrow(var)

log close
