* ---------------------------------------------------------------------------------------------------
* Build_Norway_SWF_Data_1: This job imports and processes data on the holdings of the Norwegian SWF
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Build_Norway_SWF_Data_1, replace

* ---------------------------------------------------------------------------------------------------
* Name cleaning procedures
* ---------------------------------------------------------------------------------------------------

cap program drop clean_secname
program clean_secname

    * special character stripping
    gen securityname_cln = `1'
    replace securityname_cln = subinstr(securityname_cln, "amp;", " ", .)
    replace securityname_cln = subinstr(securityname_cln, "([Wts/Rts])", " ", .)
    replace securityname_cln = subinstr(securityname_cln, ":", " ", .)
    replace securityname_cln = subinstr(securityname_cln, ";", " ", .)
    replace securityname_cln = subinstr(securityname_cln, ",", " ", .)
    replace securityname_cln = subinstr(securityname_cln, ".", " ", .)
    replace securityname_cln = subinstr(securityname_cln, "#", " ", .)
    replace securityname_cln = subinstr(securityname_cln, "@", " ", .)
    replace securityname_cln = subinstr(securityname_cln, "(", " ", .)
    replace securityname_cln = subinstr(securityname_cln, ")", " ", .)
    replace securityname_cln = subinstr(securityname_cln, "[", " ", .)
    replace securityname_cln = subinstr(securityname_cln, "]", " ", .)
    replace securityname_cln = subinstr(securityname_cln, "&", " & ", .)
    replace securityname_cln = upper(securityname_cln)
    replace securityname_cln = itrim(securityname_cln)

    * categorical stripping from names (reduce spurious matches due to presence of "Bond", "Treasury" and "Corp" etc.)
    * separated into separate lines because Stata has a limit on brackets in Regex commands, and to ensure systematic ordering of strips
    gen name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )INC($| )")
    replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )CORP($| )") & name_firm_categ==""
    replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )LTD($| )") & name_firm_categ==""
    replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )AUTH($| )") & name_firm_categ==""
    replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )CO($| )") & name_firm_categ==""
    replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )GROUP($| )") & name_firm_categ==""
    replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )GRP($| )") & name_firm_categ==""
    replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )HOLDINGS($| )") & name_firm_categ==""
    replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )HLDGS($| )") & name_firm_categ==""
    replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )PLC($| )") & name_firm_categ==""
    replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )AG($| )") & name_firm_categ==""
    replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )SPA($| )") & name_firm_categ==""
    replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )SA($| )") & name_firm_categ==""
    replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )AB($| )") & name_firm_categ==""
    replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )NV($| )") & name_firm_categ==""
    gen name_bond_type = trim(regexs(0)) if regexm(securityname_cln, "(^| )BOND($| )|(^| )NOTE($| )|(^| )BILL($| )|(^| )FRN($| )")
    replace name_bond_type = trim(regexs(0)) if regexm(securityname_cln, "(^| )CMO($| )") & name_bond_type==""
    replace name_bond_type = trim(regexs(0)) if regexm(securityname_cln, "(^| )ZCB($| )") & name_bond_type==""
    replace name_bond_type = trim(regexs(0)) if regexm(securityname_cln, "(^| )ADR($| )") & name_bond_type==""
    replace name_bond_type = trim(regexs(0)) if regexm(securityname_cln, "(^| )GDR($| )") & name_bond_type==""
    replace name_bond_type = trim(regexs(0)) if regexm(securityname_cln, "(^| )NTS($| )|(^| )MTG($| )|(^| )(TERM LOAN)($| )") & name_bond_type==""
    gen name_bond_legal = trim(regexs(0)) if regexm(securityname_cln, "(^| )144A($| )")

    * strip the extracted words from clean name
    replace securityname_cln = subinstr(securityname_cln, name_firm_categ, "", .)
    replace securityname_cln = subinstr(securityname_cln, name_bond_type, "", .)
    replace securityname_cln = subinstr(securityname_cln, name_bond_legal, "", .)

    * collapse categories of firms
    replace name_firm_categ = "GROUP" if regexm(name_firm_categ, "GROUP|GRP")
    replace name_firm_categ = "HOLDINGS" if regexm(name_firm_categ, "HOLDINGS|HLDGS")

    * final standardisation
    replace securityname_cln = itrim(securityname_cln)
    replace securityname_cln = trim(securityname_cln)
    drop name_bond_type name_bond_legal name_firm_categ

end

* ---------------------------------------------------------------------------------------------------
* Import raw holdings data for Norwegian sovereign wealth fund
* ---------------------------------------------------------------------------------------------------

forval year = 1998/2019 {

    * Equities
    qui import excel using $raw/Norway_SWF/EQ_`year'_Country.xlsx, clear firstrow
    gen Year = `year'
    gen Asset_Class = "Equity"
    qui save $cmns1/temp/norway_swf/equity_`year', replace

    * Fixed income
    qui import excel using $raw/Norway_SWF/FI_`year'_Country.xlsx, clear firstrow
    gen Year = `year'
    gen Asset_Class = "Bonds"
    qui save $cmns1/temp/norway_swf/bonds_`year', replace

    * Real estate
    if `year' >= 2011 {
        qui import excel using $raw/Norway_SWF/RE_`year'_Country.xlsx, clear firstrow
        gen Year = `year'
        gen Asset_Class = "RE"
        qui save $cmns1/temp/norway_swf/re_`year', replace        
    }

    * Append
    use $cmns1/temp/norway_swf/equity_`year'.dta, clear
    qui append using $cmns1/temp/norway_swf/bonds_`year'.dta, force
    save $cmns1/temp/norway_swf/all_`year', replace
    
}

* ---------------------------------------------------------------------------------------------------
* Import Factset holdings
* ---------------------------------------------------------------------------------------------------

* Import Factset's version of Norwegian SWF equity holdings: we use these files in order to link
* positions in the raw data to CUSIP codes
forval year = 2007/2017 {
    import excel using $raw/Factset/workstation/Norway_SWF_Equity_Holdings_`year'_Factset.xlsx, clear sheet("Sheet1") firstrow
    rename CUSIP cusip
    gen issuer_number = substr(cusip, 1, 6)
    qui mmerge issuer_number using $cmns1/country_master/cmns_aggregation.dta, unmatched(m)
    cap drop *source*
    drop _merge
    qui drop if inlist(cusip, "CASH", "REALPPTY")
    save $cmns1/temp/norway_swf/equity_`year'_factset, replace
}

* ---------------------------------------------------------------------------------------------------
* Process country names
* ---------------------------------------------------------------------------------------------------

* Merge in country ISO3 codes
use $cmns1/temp/norway_swf/all_2017, clear
keep Country
duplicates drop
rename Country Country_Name
qui mmerge Country_Name using $raw/Macro/Concordances/country_names.dta, unmatched(m)
drop _merge
rename ISO_Country_Code ISO
qui replace ISO = "XSN" if Country_Name == "International Organisations"
qui replace ISO = "RUS" if Country_Name == "Russia"
qui replace ISO = "KOR" if Country_Name == "South Korea"
qui replace ISO = "TWN" if Country_Name == "Taiwan"
qui replace ISO = "TZA" if Country_Name == "Tanzania"
qui replace ISO = "VNM" if Country_Name == "Vietnam"
assert ~missing(ISO)
tempfile country1
save `country1', replace

* Crosswalk from country names to ISO3 codes
use $cmns1/temp/norway_swf/all_2017, clear
keep IncorporationCountry
duplicates drop
rename IncorporationCountry Country_Name
qui mmerge Country_Name using $raw/Macro/Concordances/country_names.dta, unmatched(m)
drop _merge
rename ISO_Country_Code ISO
qui replace ISO = "XSN" if Country_Name == "International Organisations"
qui replace ISO = "RUS" if Country_Name == "Russia"
qui replace ISO = "KOR" if Country_Name == "South Korea"
qui replace ISO = "TWN" if Country_Name == "Taiwan"
qui replace ISO = "TZA" if Country_Name == "Tanzania"
qui replace ISO = "VNM" if Country_Name == "Vietnam"
qui replace ISO = "VGB" if Country_Name == "British Virgin Islands"
qui replace ISO = "CUW" if Country_Name == "Curacao"
assert ~missing(ISO)
append using `country1'
duplicates drop
bys Country_Name: gen N = _N
assert N == 1
assert ~missing(ISO)
drop N
save $cmns1/temp/norway_swf/country_name_to_iso, replace

* ---------------------------------------------------------------------------------------------------
* Prepare data for fixed income fuzzy merge (this assigns CUSIP6 codes to fixed income positions)
* ---------------------------------------------------------------------------------------------------

* Bonds to be matched (note we only have sufficient data for fuzzy merge from 2014 onwards)
clear
gen year = .
forval year = 2019(-1)2014 {
    append using $cmns1/temp/norway_swf/bonds_`year', force
    qui replace year = `year' if missing(year)
}
qui replace Industry = "Corporate Bonds" if Industry == "Corporate"
qui replace Industry = "Securitized Bonds" if Industry == "Securitized"
qui replace Industry = "Corporate Bonds/Securitized Bonds" if Industry == "Corporate/Securitized"
qui replace Industry = "Government Related Bonds" if Industry == "Government Related"
qui replace Industry = subinstr(Industry, " Bonds", "", .)
qui replace Industry = "Securitized" if Industry == "Corporate/Securitized"
qui replace Industry = "Corporate" if Industry == "Convertible"
qui replace Industry = "Government" if Industry == "Government Related"
collapse (max) MarketValueUSD (min) year, by(Name Industry IncorporationCountry)
tempfile bonds
save `bonds', replace
gsort MarketValueUSD

use `bonds', clear
collapse (firstnm) IncorporationCountry, by(Name Industry)
tempfile incorp
save `incorp', replace

use `bonds', clear
qui mmerge Name Industry using `incorp', uname(u_)
drop _merge
replace IncorporationCountry = u_IncorporationCountry if missing(IncorporationCountry)
collapse (max) MarketValueUSD (min) year, by(Name Industry IncorporationCountry)
gsort -MarketValueUSD
rename Industry Sector
rename IncorporationCountry Residency
rename year FirstYear
gen PriorityRank = _n
qui mmerge Residency using $cmns1/temp/norway_swf/country_name_to_iso.dta, umatch(Country_Name) unmatched(m)
qui replace ISO = "ARG" if Residency == "Argentina"
qui replace ISO = "BHS" if Residency == "Bahamas"
qui replace ISO = "BGR" if Residency == "Bulgaria"
qui replace ISO = "TTO" if Residency == "Trinidad and Tobago"
qui replace ISO = "URY" if Residency == "Uruguay"
drop _merge
assert ~missing(ISO) if ~missing(Residency)
replace Residency = ISO if ~missing(ISO)
drop ISO
gsort PriorityRank
save $cmns1/temp/swf_bonds_to_match, replace

* Issuer numbers in SWF data 
use $cmns1/holdings_master/mns_issuer_summary.dta, clear
keep if Domicile == "NOR"
keep issuer_number
rename issuer_number IssuerNumber
duplicates drop
save $cmns1/temp/issuer_numbers_in_norway_mutual_fund_data.dta, replace

* Data for match: in holdings
use $cmns1/holdings_master/mns_issuer_summary.dta, clear
drop if asset_class == "Equity"
gcollapse (sum) marketvalue_usd, by(asset_class issuer_name issuer_number cgs_domicile year)
gcollapse (max) marketvalue_usd, by(asset_class issuer_name issuer_number cgs_domicile)
qui replace asset_class = "Corporate" if asset_class == "Bonds - Corporate"
qui replace asset_class = "Securitized" if asset_class == "Bonds - Structured Finance"
qui replace asset_class = "Government" if asset_class == "Bonds - Government"
qui replace asset_class = "Government" if asset_class == "Bonds - Sovranational"
gsort -marketvalue_usd
collapse (sum) marketvalue_usd (firstnm) issuer_number, by(issuer_name asset_class cgs_domicile)
gsort -marketvalue_usd
rename asset_class Sector
rename issuer_name Name
rename marketvalue_usd Value
rename issuer_number IssuerNumber
rename cgs_domicile Residency
tempfile morningstar_cusip6
save `morningstar_cusip6', replace

use $raw/figi/figi_master_compact_cusip_unique.dta, clear
fcollapse (firstnm) name, by(cusip6 marketsector)
drop if inlist(marketsector, "Equity", "Pfd", "Comdty", "Curncy", "Index")
rename marketsector Sector
qui replace Sector = "Corporate" if Sector == "Corp"
qui replace Sector = "Government" if Sector == "Muni"
qui replace Sector = "Government" if Sector == "Govt"
qui replace Sector = "Securitized" if Sector == "Mtge"
rename name Name
rename cusip6 IssuerNumber
tempfile figi_cusip6
save `figi_cusip6', replace

use $raw/figi/figi_master_compact_cusip_unique.dta, clear
fcollapse (firstnm) name, by(cusip6 marketsector)
rename marketsector Sector
rename name Name
rename cusip6 IssuerNumber
tempfile figi_cusip6_all
save `figi_cusip6_all', replace

use $cmns1/temp/cgs/AIMASTER.dta, clear
keep issuer_num issuer_desc
drop if missing(issuer_num)
drop if missing(issuer_desc)
fcollapse (firstnm) issuer_desc, by(issuer_num)
rename issuer_num IssuerNumber
rename issuer_desc Name
qui mmerge IssuerNumber using `figi_cusip6_all', uname(u_) unmatched(m)
drop if inlist(u_Sector, "Equity", "Pfd", "Comdty", "Curncy", "Index")
drop u_* _merge
tempfile cgs_cusip6
save `cgs_cusip6', replace

use $cmns1/country_master/cmns_aggregation.dta, clear
keep issuer_number issuer_name
rename issuer_num IssuerNumber
rename issuer_name Name
qui mmerge IssuerNumber using `figi_cusip6_all', uname(u_) unmatched(m)
drop if inlist(u_Sector, "Equity", "Pfd", "Comdty", "Curncy", "Index")
drop u_* _merge
drop if missing(Name)
drop if missing(IssuerNumber)
tempfile cmns_cusip6
save `cmns_cusip6', replace

use `morningstar_cusip6', clear
qui mmerge IssuerNumber using `figi_cusip6', uname(FIGI_)
qui mmerge IssuerNumber using `cgs_cusip6', uname(CGS_)
qui mmerge IssuerNumber using `cmns_cusip6', uname(CMNS_)
gsort -Value
cap drop _merge
replace Name = CMNS_Name if missing(Name)
replace Name = FIGI_Name if missing(Name)
replace Sector = FIGI_Sector if missing(Sector)
keep Sector Name Residency Value IssuerNumber
qui mmerge IssuerNumber using $cmns1/country_master/cmns_aggregation.dta, umatch(issuer_number) ukeep(cgs_domicile)
replace Residency = cgs_domicile if missing(Residency)
drop _merge cgs_domicile
gcollapse (max) Value, by(Sector Name IssuerNumber Residency)
gsort -Value
drop if missing(Name)
tempfile matchfile_tmp
save `matchfile_tmp', replace

use `matchfile_tmp', clear
gsort IssuerNumber
by IssuerNumber: gen N = _N
count if N == 1
keep if N > 1
gsort -Value
drop Value N
bys IssuerNumber: gen n = _n
qui reshape wide Sector Name Residency, i(IssuerNumber) j(n)
assert Residency1 == Residency2 if ~missing(Residency3)
assert Residency1 == Residency3 if ~missing(Residency3)
assert Name1 == Name2 if ~missing(Name3)
assert Name1 == Name3 if ~missing(Name3)
drop Residency3
drop Name3
assert Residency1 == Residency2
drop Residency2
rename Residency1 Residency
assert Name1 == Name2
drop Name2
rename Name1 Name

use `matchfile_tmp', clear
keep if ~missing(Value)
drop Value
save $cmns1/temp/bonds_matchfile_preferred, replace

* Data for match: not in holdings
use `matchfile_tmp', clear
keep if missing(Value)
drop Value
save $cmns1/temp/bonds_matchfile_other, replace

log close
