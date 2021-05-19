* ---------------------------------------------------------------------------------------------------
* Build_Norway_SWF_Data_3: This job continues importing and processing data on the holdings of the 
* Norwegian SWF, using the fixed income crosswalk established in Build_Norway_SWF_Data_2
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Build_Norway_SWF_Data_3, replace

* ---------------------------------------------------------------------------------------------------
* Import CUSIP6 croswalk for fixed income positions
* ---------------------------------------------------------------------------------------------------

import delimited using $cmns1/temp/merged_norway_bonds.csv, clear
keep issuernumber name residency sector
rename name Name 
rename residency Residency
rename issuernumber issuer_number
duplicates drop issuer_number Name Residency, force
collapse (firstnm) issuer_number sector, by(Name Residency)
save $cmns1/temp/norway_swf_bonds_cusip6_crosswalk, replace

* ---------------------------------------------------------------------------------------------------
* Link positions to CUSIP codes for baseline year
* ---------------------------------------------------------------------------------------------------

* Link bonds positions to CUSIP codes
use $cmns1/temp/norway_swf/bonds_2017, clear
qui mmerge IncorporationCountry using $cmns1/temp/norway_swf/country_name_to_iso.dta, umatch(Country_Name) unmatched(m)
drop _merge
rename ISO Residency
qui mmerge Name Residency using $cmns1/temp/norway_swf_bonds_cusip6_crosswalk.dta, unmatched(m)
drop _merge
qui mmerge issuer_number using $cmns1/country_master/cmns_aggregation.dta, unmatched(m)
drop _merge *source*
qui mmerge Country using $cmns1/temp/norway_swf/country_name_to_iso.dta, umatch(Country_Name) unmatched(m)
drop _merge
rename ISO Nationality
gsort -MarketValueUSD
order Name issuer_name_up Residency Nationality country_bg
qui replace Nationality = country_bg if ~missing(country_bg)
drop cgs_domicile country_bg
save $cmns1/temp/norway_swf/bonds_2017_adjusted, replace

* Crosswalk to CUSIP codes from Factset equity data
use $cmns1/temp/norway_swf/equity_2017_factset.dta, clear
gen mv_int = floor(MarketValueUSD / 1000)
bys mv_int cgs_domicile: gen N = _N
drop if N > 1
drop N
unique mv_int cgs_domicile
tempfile factset_eq
save `factset_eq', replace

* Link equity positions to CUSIP codes
use $cmns1/temp/norway_swf/equity_2017, clear
gen mv_int = floor(MarketValueUSD / 1000)
qui mmerge IncorporationCountry using $cmns1/temp/norway_swf/country_name_to_iso.dta, umatch(Country_Name) unmatched(m)
drop _merge
rename ISO cgs_domicile
unique mv_int cgs_domicile
qui mmerge mv_int cgs_domicile using `factset_eq', unmatched(m)
gsort -MarketValueUSD

cap drop securityname_cln
qui clean_secname Name
rename securityname_cln orig_name
qui clean_secname issuer_name
rename securityname_cln matched_name
jarowinkler orig_name matched_name, gen(namedist)
order Name issuer_name orig_name matched_name issuer_number
cap drop _merge
qui replace namedist = 0 if inlist(issuer_number, "P37186", "P3762G", "X0192N")
foreach var in "cusip" "cusip6_up_bg" "issuer_number" "country_bg" "issuer_name_up" {
    qui replace `var' = "" if namedist < .7
}

drop orig_name matched_name mv_int cusip
rename cgs_domicile Residency
qui mmerge Country using $cmns1/temp/norway_swf/country_name_to_iso.dta, umatch(Country_Name) unmatched(m)
drop _merge
rename ISO Nationality
gsort -MarketValueUSD
order Name issuer_name country_bg Nationality namedist
qui replace Nationality = country_bg if ~missing(country_bg)
drop country_bg namedist
save $cmns1/temp/norway_swf/equity_2017_adjusted, replace

* Append bond and equity data for baseline year
use $cmns1/temp/norway_swf/equity_2017_adjusted, clear
append using $cmns1/temp/norway_swf/bonds_2017_adjusted.dta
count
keep Region Nationality Name Industry MarketValueNOK MarketValueUSD Voting Ownership Residency Year Asset_Class issuer_number
save $cmns1/temp/norway_swf/all_2017_adjusted, replace

* ---------------------------------------------------------------------------------------------------
* Link positions to CUSIP codes for prior years
* ---------------------------------------------------------------------------------------------------

* Bonds, equities and merge
forval year = 2007/2016 {
    
    di "Processing `year'"
    
    * Link bonds positions to CUSIP codes
    use $cmns1/temp/norway_swf/bonds_`year', clear
    if `year' < 2014 {
        drop IncorporationCountry
        gen IncorporationCountry = Country
    }
    qui mmerge IncorporationCountry using $cmns1/temp/norway_swf/country_name_to_iso.dta, umatch(Country_Name) unmatched(m)
    drop _merge
    rename ISO Residency
    qui mmerge Name Residency using $cmns1/temp/norway_swf_bonds_cusip6_crosswalk.dta, unmatched(m)
    drop _merge
    qui mmerge issuer_number using $cmns1/country_master/cmns_aggregation.dta, unmatched(m)
    drop _merge *source*
    qui mmerge Country using $cmns1/temp/norway_swf/country_name_to_iso.dta, umatch(Country_Name) unmatched(m)
    drop _merge
    rename ISO Nationality
    gsort -MarketValueUSD
    order Name issuer_name_up Residency Nationality country_bg
    qui replace Nationality = country_bg if ~missing(country_bg)
    drop cgs_domicile country_bg
    save $cmns1/temp/norway_swf/bonds_`year'_adjusted, replace

    * Crosswalk to CUSIP codes from Factset equity data
    use $cmns1/temp/norway_swf/equity_`year'_factset.dta, clear
    gen mv_int = floor(MarketValueUSD / 1000)
    bys mv_int cgs_domicile: gen N = _N
    drop if N > 1
    drop N
    tempfile factset_eq
    save `factset_eq', replace

    * Link equity positions to CUSIP codes
    use $cmns1/temp/norway_swf/equity_`year', clear
    gen mv_int = floor(MarketValueUSD / 1000)
    qui mmerge IncorporationCountry using $cmns1/temp/norway_swf/country_name_to_iso.dta, umatch(Country_Name) unmatched(m)
    drop _merge
    rename ISO cgs_domicile
    qui mmerge mv_int cgs_domicile using `factset_eq', unmatched(m)
    gsort -MarketValueUSD

    cap drop securityname_cln
    qui clean_secname Name
    rename securityname_cln orig_name
    qui clean_secname issuer_name
    rename securityname_cln matched_name
    jarowinkler orig_name matched_name, gen(namedist)
    order Name issuer_name orig_name matched_name issuer_number
    cap drop _merge
    qui replace namedist = 0 if inlist(issuer_number, "P37186", "P3762G", "X0192N")
    foreach var in "cusip" "cusip6_up_bg" "issuer_number" "country_bg" "issuer_name_up" {
        qui replace `var' = "" if namedist < .7
    }

    drop orig_name matched_name mv_int cusip
    rename cgs_domicile Residency
    qui mmerge Country using $cmns1/temp/norway_swf/country_name_to_iso.dta, umatch(Country_Name) unmatched(m)
    drop _merge
    rename ISO Nationality
    gsort -MarketValueUSD
    order Name issuer_name country_bg Nationality namedist
    qui replace Nationality = country_bg if ~missing(country_bg)
    drop country_bg namedist
    save $cmns1/temp/norway_swf/equity_`year'_adjusted, replace

    * Append bond and equity data for baseline year
    use $cmns1/temp/norway_swf/equity_`year'_adjusted, clear
    append using $cmns1/temp/norway_swf/bonds_`year'_adjusted.dta
    count
    keep Region Nationality Name Industry MarketValueNOK MarketValueUSD Voting Ownership Residency Year Asset_Class issuer_number
    save $cmns1/temp/norway_swf/all_`year'_adjusted, replace
    
}

* ---------------------------------------------------------------------------------------------------
* Build SWF-based reallocation matrices
* ---------------------------------------------------------------------------------------------------

* Generate reallocation matrix data from SWF holdings for baseline year
use $cmns1/temp/norway_swf/all_2017_adjusted, clear
gen Subclass = Industry
replace Subclass = "Equity" if Asset_Class == "Equity"
replace Subclass = "Government Bonds" if inlist(Subclass, "Government", "Government Related Bonds")
replace Subclass = "Corporate Bonds" if inlist(Subclass, "Convertible Bonds")
replace Subclass = "Securitized Bonds" if inlist(Subclass, "Corporate Bonds/Securitized Bonds")
drop Asset_Class
rename Subclass Asset_Class
collapse (sum) MarketValueUSD, by(Asset_Class Residency Nationality)
bys Asset_Class Residency: egen Residency_Total = total(MarketValueUSD)
gen Reallocation_Share = MarketValueUSD / Residency_Total
save $cmns1/temp/norway_swf/reallocation_shares_2017_adjusted, replace

* Format equity reallocation shares data
use $cmns1/temp/norway_swf/reallocation_shares_2017_adjusted.dta, clear
keep if Asset_Class == "Equity"
keep Nationality Residency Reallocation_Share
rename Reallocation_Share Share_
sort Residency Nationality
qui reshape wide Share_, i(Residency) j(Nationality) string
rename Share_* *
save $cmns1/temp/norway_swf/swf_matrices/equity_2017_adjusted, replace
export excel using $cmns1/temp/norway_swf/swf_matrices/equity_2017_adjusted.xls, replace firstrow(var)

* Format corporate bonds reallocation shares data
use $cmns1/temp/norway_swf/reallocation_shares_2017_adjusted, clear
keep if Asset_Class == "Corporate Bonds"
keep Nationality Residency Reallocation_Share
rename Reallocation_Share Share_
sort Residency Nationality
replace Residency = subinstr(Residency, " ", "_", .)
replace Nationality = subinstr(Nationality, " ", "_", .)
qui reshape wide Share_, i(Residency) j(Nationality) string
rename Share_* *
save $cmns1/temp/norway_swf/swf_matrices/corporate_bonds_2017_adjusted, replace
export excel using $cmns1/temp/norway_swf/swf_matrices/corporate_bonds_2017_adjusted.xls, replace firstrow(var)

log close
