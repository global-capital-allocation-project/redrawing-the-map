* ---------------------------------------------------------------------------------------------------
* Merge_TIC_CPIS: Merges and cleans TIC and CPIS data
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Merge_TIC_CPIS, replace

* ---------------------------------------------------------------------------------------------------
* Utilities
* ---------------------------------------------------------------------------------------------------

cap program drop fill_in_country_codes
program fill_in_country_codes
    qui replace issuer = "ASM" if issuer_name == "American Samoa"
    qui replace issuer = "AND" if issuer_name == "Andorra"
    qui replace issuer = "AIA" if issuer_name == "Anguilla"
    qui replace issuer = "BES" if issuer_name == "Bonaire, Sint Eustatius and Saba"
    qui replace issuer = "IOT" if issuer_name == "British Indian Ocean Territory"
    qui replace issuer = "MAC" if issuer_name == "China, P.R.: Macao"
    qui replace issuer = "CXR" if issuer_name == "Christmas Island"
    qui replace issuer = "CCK" if issuer_name == "Cocos (Keeling) Islands"
    qui replace issuer = "COK" if issuer_name == "Cook Islands"
    qui replace issuer = "CUB" if issuer_name == "Cuba"
    qui replace issuer = "FLK" if issuer_name == "Falkland Islands"
    qui replace issuer = "FRO" if issuer_name == "Faroe Islands"
    qui replace issuer = "ATF" if issuer_name == "French Southern Territories"
    qui replace issuer = "PYF" if issuer_name == "French Territories: French Polynesia"
    qui replace issuer = "NCL" if issuer_name == "French Territories: New Caledonia"
    qui replace issuer = "GRL" if issuer_name == "Greenland"
    qui replace issuer = "GLP" if issuer_name == "Guadeloupe"
    qui replace issuer = "GUM" if issuer_name == "Guam"
    qui replace issuer = "GGY" if issuer_name == "Guernsey"
    qui replace issuer = "GUF" if issuer_name == "Guiana, French"
    qui replace issuer = "XSN" if issuer_name == "International Organizations"
    qui replace issuer = "IMN" if issuer_name == "Isle of Man"
    qui replace issuer = "JEY" if issuer_name == "Jersey"
    qui replace issuer = "PRK" if issuer_name == "Korea, Democratic People's Rep. of"
    qui replace issuer = "LIE" if issuer_name == "Liechtenstein"
    qui replace issuer = "MTQ" if issuer_name == "Martinique"
    qui replace issuer = "MYT" if issuer_name == "Mayotte"
    qui replace issuer = "MCO" if issuer_name == "Monaco"
    qui replace issuer = "MSR" if issuer_name == "Montserrat"
    qui replace issuer = "NRU" if issuer_name == "Nauru"
    qui replace issuer = "NIU" if issuer_name == "Niue"
    qui replace issuer = "NFK" if issuer_name == "Norfolk Island"
    qui replace issuer = "OTH" if issuer_name == "Not Specified (including Confidential)"
    qui replace issuer = "PCN" if issuer_name == "Pitcairn Islands"
    qui replace issuer = "PRI" if issuer_name == "Puerto Rico"
    qui replace issuer = "REU" if issuer_name == "Reunion"
    qui replace issuer = "SHN" if issuer_name == "Saint Helena"
    qui replace issuer = "SPM" if issuer_name == "Saint Pierre and Miquelon"
    qui replace issuer = "SOM" if issuer_name == "Somalia"
    qui replace issuer = "TKL" if issuer_name == "Tokelau Islands"
    qui replace issuer = "TCA" if issuer_name == "Turks and Caicos Islands"
    qui replace issuer = "VIR" if issuer_name == "US Virgin Islands"
    qui replace issuer = "VAT" if issuer_name == "Vatican"
    qui replace issuer = "VGB" if issuer_name == "Virgin Islands, British"
    qui replace issuer = "WLF" if issuer_name == "Wallis and Futuna"
    qui replace issuer = "ESH" if issuer_name == "Western Sahara"
    qui drop if issuer_name == "West Bank and Gaza"
    qui drop if issuer_name == "World"
    qui drop if issuer_name == "US Pacific Islands"
    qui replace issuer = "AFG" if issuer_name == "Afghanistan, Islamic Rep. of"
    qui replace issuer = "AND" if issuer_name == "Andorra, Principality of"
    qui replace issuer = "ABW" if issuer_name == "Aruba, Kingdom of the Netherlands"
    qui replace issuer = "BLR" if issuer_name == "Belarus, Rep. of"
    qui replace issuer = "BES" if issuer_name == "Bonaire, St. Eustatius and Saba"
    qui replace issuer = "CAF" if issuer_name == "Central African Rep."
    qui replace issuer = "COM" if issuer_name == "Comoros, Union of the"
    qui replace issuer = "COD" if issuer_name == "Congo, Dem. Rep. of the"
    qui replace issuer = "HRV" if issuer_name == "Croatia, Rep. of"
    qui replace issuer = "CUW" if issuer_name == "Curaçao, Kingdom of the Netherlands"
    qui replace issuer = "CZE" if issuer_name == "Czech Rep."
    qui replace issuer = "EGY" if issuer_name == "Egypt, Arab Rep. of"
    qui replace issuer = "GNQ" if issuer_name == "Equatorial Guinea, Rep. of"
    qui replace issuer = "ERI" if issuer_name == "Eritrea, The State of"
    qui replace issuer = "EST" if issuer_name == "Estonia, Rep. of"
    qui replace issuer = "FLK" if issuer_name == "Falkland Islands (Malvinas)"
    qui replace issuer = "FJI" if issuer_name == "Fiji, Rep. of"
    qui replace issuer = "PYF" if issuer_name == "French Polynesia"
    qui replace issuer = "VAT" if issuer_name == "Holy See"
    qui replace issuer = "IRN" if issuer_name == "Iran, Islamic Rep. of"
    qui replace issuer = "PRK" if issuer_name == "Korea, Dem. People's Rep. of"
    qui replace issuer = "KOR" if issuer_name == "Korea, Rep. of"
    qui replace issuer = "KOS" if issuer_name == "Kosovo, Rep. of"
    qui replace issuer = "KGZ" if issuer_name == "Kyrgyz Rep."
    qui replace issuer = "LAO" if issuer_name == "Lao People's Dem. Rep."
    qui replace issuer = "LSO" if issuer_name == "Lesotho, Kingdom of"
    qui replace issuer = "MDG" if issuer_name == "Madagascar, Rep. of"
    qui replace issuer = "MHL" if issuer_name == "Marshall Islands, Rep. of the"
    qui replace issuer = "MRT" if issuer_name == "Mauritania, Islamic Rep. of"
    qui replace issuer = "MDA" if issuer_name == "Moldova, Rep. of"
    qui replace issuer = "NRU" if issuer_name == "Nauru, Rep. of"
    qui replace issuer = "NLD" if issuer_name == "Netherlands, The"
    qui replace issuer = "NCL" if issuer_name == "New Caledonia"
    qui replace issuer = "PLW" if issuer_name == "Palau, Rep. of"
    qui replace issuer = "POL" if issuer_name == "Poland, Rep. of"
    qui replace issuer = "SMR" if issuer_name == "San Marino, Rep. of"
    qui replace issuer = "SRB" if issuer_name == "Serbia, Rep. of"
    qui replace issuer = "SXM" if issuer_name == "Sint Maarten, Kingdom of the Netherlands"
    qui replace issuer = "SVK" if issuer_name == "Slovak Rep."
    qui replace issuer = "SVN" if issuer_name == "Slovenia, Rep. of"
    qui replace issuer = "SSD" if issuer_name == "South Sudan, Rep. of"
    qui replace issuer = "SYR" if issuer_name == "Syrian Arab Rep."
    qui replace issuer = "STP" if issuer_name == "São Tomé and Príncipe, Dem. Rep. of"
    qui replace issuer = "TJK" if issuer_name == "Tajikistan, Rep. of"
    qui replace issuer = "TZA" if issuer_name == "Tanzania, United Rep. of"
    qui replace issuer = "TKL" if issuer_name == "Tokelau"
    qui replace issuer = "VIR" if issuer_name == "United States Virgin Islands"
    qui replace issuer = "UZB" if issuer_name == "Uzbekistan, Rep. of"
    qui replace issuer = "WLF" if issuer_name == "Wallis and Futuna Islands"
    qui replace issuer = "YEM" if issuer_name == "Yemen, Rep. of"
    qui replace issuer = "ARM" if issuer_name == "Armenia, Rep. of"
    qui replace issuer = "AZE" if issuer_name == "Azerbaijan, Rep. of"
    qui replace issuer = "COG" if issuer_name == "Congo, Rep. of"
    qui replace issuer = "CIV" if issuer_name == "Côte d'Ivoire"
    qui replace issuer = "DOM" if issuer_name == "Dominican Rep."
    qui replace issuer = "ETH" if issuer_name == "Ethiopia, The Federal Dem. Rep. of"
    qui replace issuer = "MOZ" if issuer_name == "Mozambique, Rep. of"
    qui replace issuer = "MKD" if strpos(issuer_name, "North Macedonia")
    qui drop if issuer_name == "Total Value of Investment"
    qui replace issuer = "VEN" if issuer_name == "Venezuela, Rep. Bolivariana de"
end

* ---------------------------------------------------------------------------------------------------
* Construct cross-border investment shares for domestic imputation
* ---------------------------------------------------------------------------------------------------

* Sumstats for cross-border shares
use $cmns1/holdings_master/mns_issuer_summary_disaggregated_emu, clear
collapse (sum) marketvalue_usd, by(DomicileCountryId asset_class cgs_domicile country_bg year)
save $cmns1/temp/sumstats_for_reallocation_shares_extra_disagg, replace

* Class B: all bonds
use $cmns1/temp/sumstats_for_reallocation_shares_extra_disagg, clear
keep if inlist(DomicileCountryId, "USA", "GBR", "CAN", "CHE", "AUS") | inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL") | inlist(DomicileCountryId, $eu1) | inlist(DomicileCountryId, $eu2) | inlist(DomicileCountryId, $eu3)
keep if strpos(asset_class, "Bond")
drop if cgs_domicile == ""
collapse (sum) marketvalue_usd, by(DomicileCountryId cgs_domicile country_bg year)
gen cross_border_residency = 0
replace cross_border_residency = 1 if DomicileCountryId != cgs_domicile
collapse (sum) marketvalue_usd, by(DomicileCountryId cross_border_residency year)
bysort DomicileCountryId year: egen tot_portfolio = total(marketvalue_usd)
keep if cross_border_residency == 1
gen share_cross_border_residency = marketvalue_usd / tot_portfolio
keep DomicileCountryId year share_cross_border_residency
save $cmns1/temp/share_cross_border_residency_disagg_B, replace

* Class E: equities
use $cmns1/temp/sumstats_for_reallocation_shares_extra_disagg, clear
keep if inlist(DomicileCountryId, "USA", "GBR", "CAN", "CHE", "AUS") | inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL") | inlist(DomicileCountryId, $eu1) | inlist(DomicileCountryId, $eu2) | inlist(DomicileCountryId, $eu3)
keep if asset_class == "Equity"
drop if cgs_domicile == ""
collapse (sum) marketvalue_usd, by(DomicileCountryId cgs_domicile country_bg year)
gen cross_border_residency = 0
replace cross_border_residency = 1 if DomicileCountryId != cgs_domicile
collapse (sum) marketvalue_usd, by(DomicileCountryId cross_border_residency year)
bysort DomicileCountryId year: egen tot_portfolio = total(marketvalue_usd)
keep if cross_border_residency == 1
gen share_cross_border_residency = marketvalue_usd / tot_portfolio
keep DomicileCountryId year share_cross_border_residency
save $cmns1/temp/share_cross_border_residency_disagg_E, replace

* Class BC: corporate bonds
use $cmns1/temp/sumstats_for_reallocation_shares_extra_disagg, clear
keep if inlist(DomicileCountryId, "USA", "GBR", "CAN", "CHE", "AUS") | inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL") | inlist(DomicileCountryId, $eu1) | inlist(DomicileCountryId, $eu2) | inlist(DomicileCountryId, $eu3)
keep if asset_class == "Bonds - Corporate"
drop if cgs_domicile == ""
collapse (sum) marketvalue_usd, by(DomicileCountryId cgs_domicile country_bg year)
gen cross_border_residency = 0
replace cross_border_residency = 1 if DomicileCountryId != cgs_domicile
collapse (sum) marketvalue_usd, by(DomicileCountryId cross_border_residency year)
bysort DomicileCountryId year: egen tot_portfolio = total(marketvalue_usd)
keep if cross_border_residency == 1
gen share_cross_border_residency = marketvalue_usd / tot_portfolio
keep DomicileCountryId year share_cross_border_residency
save $cmns1/temp/share_cross_border_residency_disagg_BC, replace

* Class BSALS: government bonds
use $cmns1/temp/sumstats_for_reallocation_shares_extra_disagg, clear
keep if inlist(DomicileCountryId, "USA", "GBR", "CAN", "CHE", "AUS") | inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL") | inlist(DomicileCountryId, $eu1) | inlist(DomicileCountryId, $eu2) | inlist(DomicileCountryId, $eu3)
keep if asset_class == "Bonds - Government"
drop if cgs_domicile == ""
collapse (sum) marketvalue_usd, by(DomicileCountryId cgs_domicile country_bg year)
gen cross_border_residency = 0
replace cross_border_residency = 1 if DomicileCountryId != cgs_domicile
collapse (sum) marketvalue_usd, by(DomicileCountryId cross_border_residency year)
bysort DomicileCountryId year: egen tot_portfolio = total(marketvalue_usd)
keep if cross_border_residency == 1
gen share_cross_border_residency = marketvalue_usd / tot_portfolio
keep DomicileCountryId year share_cross_border_residency
save $cmns1/temp/share_cross_border_residency_disagg_BSALS, replace

* Class BSF: structured finance
use $cmns1/temp/sumstats_for_reallocation_shares_extra_disagg, clear
keep if inlist(DomicileCountryId, "USA", "GBR", "CAN", "CHE", "AUS") | inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL") | inlist(DomicileCountryId, $eu1) | inlist(DomicileCountryId, $eu2) | inlist(DomicileCountryId, $eu3)
keep if asset_class == "Bonds - Structured Finance"
drop if cgs_domicile == ""
collapse (sum) marketvalue_usd, by(DomicileCountryId cgs_domicile country_bg year)
gen cross_border_residency = 0
replace cross_border_residency = 1 if DomicileCountryId != cgs_domicile
collapse (sum) marketvalue_usd, by(DomicileCountryId cross_border_residency year)
bysort DomicileCountryId year: egen tot_portfolio = total(marketvalue_usd)
keep if cross_border_residency == 1
gen share_cross_border_residency = marketvalue_usd / tot_portfolio
keep DomicileCountryId year share_cross_border_residency
save $cmns1/temp/share_cross_border_residency_disagg_BSF, replace

* Append all the cross-border residency shares
clear
gen mns_class = ""
foreach _class in "B" "E" "BC" "BSALS" "BSF" {
	append using $cmns1/temp/share_cross_border_residency_disagg_`_class'
	replace mns_class = "`_class'" if missing(mns_class)
}
save $cmns1/temp/share_cross_border_residency_disagg, replace

* ---------------------------------------------------------------------------------------------------
* Construct merged TIC-CPIS version with domestic imputations
* ---------------------------------------------------------------------------------------------------

* TIC preparation
use $cmns1/holdings_master/TIC-Disaggregated-Clean-Main.dta, clear
gen DomicileCountryId = "USA"
gen total_debt = government_debt_lt + government_debt_st + private_debt
gen government_debt = government_debt_lt + government_debt_st
keep DomicileCountryId year common_equity total_debt corporate_debt government_debt abs iso
rename common_equity valueE
rename total_debt valueB
rename corporate_debt valueBC
rename government_debt valueBSALS
rename abs valueBSF
reshape long value, i(DomicileCountryId iso year) j(mns_class) string
save $cmns1/temp/TIC_merged, replace

* CPIS preparation
use $cmns1/holdings_master/CPIS-Clean-Main.dta, clear
gen mns_class = ""
replace mns_class = "B" if asset_class == "Debt (All)"
replace mns_class = "E" if asset_class == "Equity (All)"
rename position value
rename issuer iso
rename investor DomicileCountryId
keep iso DomicileCountryId mns_class year value
drop if DomicileCountryId == "USA"
save $cmns1/temp/CPIS_merged_disagg, replace

* CPIS preparation, with disaggregated EMU
use $cmns1/holdings_master/CPIS-Clean-Main-Disagg-EMU.dta, clear
gen mns_class = ""
replace mns_class = "B" if asset_class == "Debt (All)"
replace mns_class = "E" if asset_class == "Equity (All)"
rename position value
rename issuer iso
rename investor DomicileCountryId
keep iso DomicileCountryId mns_class year value
drop if DomicileCountryId == "USA"
save $cmns1/temp/CPIS_merged_disagg_break_emu, replace

* Merge TIC and CPIS
use $cmns1/temp/TIC_merged, clear
append using $cmns1/temp/CPIS_merged_disagg
drop if DomicileCountryId == iso
drop if iso == "OTH"
save $cmns1/temp/TIC_CPIS_merged_disagg, replace

* Merge TIC and CPIS: with disaggregated EMU
use $cmns1/temp/TIC_merged, clear
append using $cmns1/temp/CPIS_merged_disagg_break_emu
drop if DomicileCountryId == iso
drop if iso == "OTH"
save $cmns1/temp/TIC_CPIS_merged_disagg_break_emu, replace

* Perfom the domestic investment imputation
use $cmns1/temp/TIC_CPIS_merged_disagg_break_emu, clear
drop if DomicileCountryId == iso
collapse (sum) value, by(DomicileCountryId mns_class year)
rename value cross_border_portfolio
qui mmerge DomicileCountryId mns_class year using $cmns1/temp/share_cross_border_residency_disagg, unmatched(m)
gen domestic_portfolio = ((1 - share) / share) * cross_border_portfolio
replace domestic_portfolio = -1 if domestic_portfolio < 0
keep DomicileCountryId mns_class year domestic_portfolio
gen iso = DomicileCountryId
rename domestic_portfolio value
save $cmns1/temp/TIC_CPIS_domestic_imputation_disagg, replace

* Append the imputation to the original TIC/CPIS data
use $cmns1/temp/TIC_CPIS_merged_disagg, clear
append using $cmns1/temp/TIC_CPIS_domestic_imputation_disagg

* Now collapse the EMU
replace DomicileCountryId = "EMU" if inlist(DomicileCountryId,$eu1)|inlist(DomicileCountryId,$eu2)|inlist(DomicileCountryId,$eu3)
collapse (sum) value, by(year DomicileCountryId iso mns_class)
drop if iso == "OTH"
bysort Domicile mns_class iso year: gen n = _n
drop if n > 1
drop n
drop if year < 2007
save $cmns1/temp/_TIC_CPIS_merged_augmented_disagg, replace

* ISO code crosswalk
use $cmns1/temp/CPIS-Main.dta, clear
keep issuer_name issuer
duplicates drop
save $cmns1/temp/cpis_names_to_iso3, replace

* Supplementary data for Australia: equities, June values
import excel using $raw/CPIS/supplementary/AUS_Equity.xlsx, clear cellrange(B5)
keep B W
rename B issuer_name
rename W june_value
drop if _n == 1
drop if missing(issuer_name)
destring june_value, force replace
drop if missing(june_value)
qui mmerge issuer_name using $cmns1/temp/cpis_names_to_iso3, unmatched(m)
fill_in_country_codes
assert ~missing(issuer)
gen mns_class = "E"
drop _merge
rename issuer iso
drop issuer_name
gen DomicileCountryId = "AUS"
gen year = 2017
save $cmns1/temp/aus_cpis_supplementary_equity, replace

* Supplementary data for Australia: bonds, June values
import excel using $raw/CPIS/supplementary/AUS_Debt.xlsx, clear cellrange(B5)
keep B W
rename B issuer_name
rename W june_value
drop if _n == 1
drop if missing(issuer_name)
destring june_value, force replace
drop if missing(june_value)
qui mmerge issuer_name using $cmns1/temp/cpis_names_to_iso3, unmatched(m)
fill_in_country_codes
assert ~missing(issuer)
gen mns_class = "B"
drop _merge
rename issuer iso
drop issuer_name
gen DomicileCountryId = "AUS"
gen year = 2017
save $cmns1/temp/aus_cpis_supplementary_debt, replace

* Append supplementary data for Australia
use $cmns1/temp/aus_cpis_supplementary_debt, clear
append using $cmns1/temp/aus_cpis_supplementary_equity
save $cmns1/temp/aus_cpis_supplementary, replace

* Flags for CPIS censoring
use $cmns1/holdings_master/CPIS-Clean-Main, clear
collapse (max) censored, by(investor issuer year asset_class)
replace censored = 0 if censored < 0
drop if missing(issuer) | missing(investor)
replace asset_class = "B" if asset_class == "Debt (All)"
replace asset_class = "E" if asset_class == "Equity (All)"
rename year Year
rename investor Investor
rename issuer Issuer
save $cmns1/temp/cpis_censoring, replace

* Make adjustments and interpolations
use $cmns1/temp/_TIC_CPIS_merged_augmented_disagg, clear
qui gen interpolated = 0
qui mmerge year mns_class DomicileCountryId iso using $cmns1/temp/cpis_censoring.dta, ///
     umatch(Year asset_class Investor Issuer) unmatched(m) ukeep(censored)
qui drop _merge
qui mmerge DomicileCountryId iso year mns_class using $cmns1/temp/aus_cpis_supplementary, unmatched(m)
drop _merge
qui replace value = june_value if ~missing(june_value) & censored == 1 & DomicileCountryId == "AUS"
qui replace interpolated = 1 if ~missing(june_value) & censored == 1 & DomicileCountryId == "AUS"
qui replace censored = 0 if ~missing(june_value) & censored == 1 & DomicileCountryId == "AUS"
qui reshape wide value interpolated censored, i(DomicileCountryId iso mns_class) j(year)
order DomicileCountryId iso mns_class value* interpolated* censored*
forval year = 2007/2017 {
    qui replace censored`year' = 1 if missing(value`year') & DomicileCountryId == "USA"
    qui replace censored`year' = 0 if ~missing(value`year') & DomicileCountryId == "USA"
}
qui gen sum_censored = censored2007 + censored2008 + censored2009 + censored2010 + censored2011 + ///
    censored2012 + censored2013 + censored2014 + censored2015 + censored2016 + censored2017

* There are missing values for Investor==”GBR” & Asset_Class==”All Bonds” & 2010 for Issuers CHL, CYP, and CHE. 
* For those values (under residency), fill them in using the average of their values from 2009 and 2011
replace value2010 = (value2011 + value2009) / 2 if DomicileCountryId == "GBR" & mns_class == "B" & ///
    inlist(iso, "CHL", "CYP", "CHE")
replace interpolated2010 = 1 if DomicileCountryId == "GBR" & mns_class == "B" & ///
    inlist(iso, "CHL", "CYP", "CHE")

* For Investor==”CHE” & Asset_Class==”All Bonds” and Issuers = {CIV, ECU, LVA, SVK, TCA, TUN, VNM, and ZWE} 
* there are a bunch of missing values during the period 2007-2010, set all those values to zero
forval year = 2007/2017 {
    qui replace interpolated`year' = 1 if DomicileCountryId == "CHE" & mns_class == "B" & value`year' == 0 & ///
        (inlist(iso, "CIV", "ECU", "LVA", "SVK", "TCA") | inlist(iso, "TUN", "VNM", "ZWE"))    
    qui replace interpolated`year' = 1 if mns_class == "BSF" & DomicileCountryId == "USA" & missing(value`year')
    qui replace value`year' = 0 if mns_class == "BSF" & DomicileCountryId == "USA" & missing(value`year')
}

replace value2010 = (value2011 + value2009) / 2 if DomicileCountryId == "GBR" & iso == "LUX"
replace interpolated2010 = 1 if DomicileCountryId == "GBR" & iso == "LUX"
replace censored2010 = 1 if DomicileCountryId == "GBR" & iso == "LUX"

forval year = 2007/2017 {
    qui gen nonmissing_post`year' = 0
    qui gen nonmissing_pre`year' = 0
    forval offset = 2007/2017 {
        qui replace nonmissing_post`year' = nonmissing_post`year' + 1 if censored`offset' == 0 & `offset' > `year'
        qui replace nonmissing_pre`year' = nonmissing_pre`year' + 1 if censored`offset' == 0 & `offset' < `year'
    }
}
order nonmissing_pre* nonmissing_post*, last

* Forward-fill and back-fill
forval year = 2007/2017 {
    
    * Forward-fill at the end
    forval offset = 2007/2017 {
        
        * USA
        qui replace value`year' = value`offset' if `offset' < `year' & censored`year' == 1 & ///
            nonmissing_post`year' == 0 & nonmissing_pre`year' > 0 & censored`offset' == 0 & ///
            ~missing(censored`offset') & DomicileCountryId == "USA" & inlist(mns_class, "BC", "BSALS", "E")
        
        qui replace interpolated`year' = 1 if `offset' < `year' & censored`year' == 1 & ///
            nonmissing_post`year' == 0 & nonmissing_pre`year' > 0 & censored`offset' == 0 & ///
            ~missing(censored`offset') & DomicileCountryId == "USA" & inlist(mns_class, "BC", "BSALS", "E")

        * Non-USA
        qui replace value`year' = value`offset' if `offset' < `year' & censored`year' == 1 & ///
            nonmissing_post`year' == 0 & nonmissing_pre`year' > 0 & censored`offset' == 0 & ///
            ~missing(censored`offset') & DomicileCountryId != "USA" & inlist(mns_class, "E")
        
        qui replace interpolated`year' = 1 if `offset' < `year' & censored`year' == 1 & ///
            nonmissing_post`year' == 0 & nonmissing_pre`year' > 0 & censored`offset' == 0 & ///
            ~missing(censored`offset') & DomicileCountryId != "USA" & inlist(mns_class, "E")
        
    }
    
    * Back-fill at the beginning
    forval offset = 2017(-1)2007 {
        
        * USA
        qui replace value`year' = value`offset' if `offset' > `year' & censored`year' == 1 & ///
            nonmissing_post`year' > 0 & nonmissing_pre`year' == 0 & censored`offset' == 0 & ///
            ~missing(censored`offset') & DomicileCountryId == "USA" & inlist(mns_class, "BC", "BSALS", "E")

        qui replace interpolated`year' = 1 if `offset' > `year' & censored`year' == 1 & ///
            nonmissing_post`year' > 0 & nonmissing_pre`year' == 0 & censored`offset' == 0 & ///
            ~missing(censored`offset') & DomicileCountryId == "USA" & inlist(mns_class, "BC", "BSALS", "E")

        * Non-USA
        qui replace value`year' = value`offset' if `offset' > `year' & censored`year' == 1 & ///
            nonmissing_post`year' > 0 & nonmissing_pre`year' == 0 & censored`offset' == 0 & ///
            ~missing(censored`offset') & DomicileCountryId != "USA" & inlist(mns_class, "E")

        qui replace interpolated`year' = 1 if `offset' > `year' & censored`year' == 1 & ///
            nonmissing_post`year' > 0 & nonmissing_pre`year' == 0 & censored`offset' == 0 & ///
            ~missing(censored`offset') & DomicileCountryId != "USA" & inlist(mns_class, "E")
        
    }
    
}

* Interior interpolation
forval year = 2008/2016 {
    
    *** Set counters ***
    local pre=`year'-1
    local post=`year'+1
    local pre2=`year'-2
    local post2=`year'+2
    local pre3=`year'-3
    local post3=`year'+3

    *** Single gap ***
    
    * USA
    qui replace value`year' = (value`pre' + value`post') / 2 if censored`year' == 1 & censored`pre' == 0 & ///
        censored`post' == 0 & DomicileCountryId == "USA" & inlist(mns_class, "BC", "BSALS", "E")
    
    qui replace interpolated`year' = 1 if censored`year' == 1 & censored`pre' == 0 & ///
        censored`post' == 0 & DomicileCountryId == "USA" & inlist(mns_class, "BC", "BSALS", "E")
 
    * Non-USA
    qui replace value`year' = (value`pre' + value`post') / 2 if censored`year' == 1 & censored`pre' == 0 & ///
        censored`post' == 0 & DomicileCountryId != "USA" & inlist(mns_class, "E")
    
    qui replace interpolated`year' = 1 if censored`year' == 1 & censored`pre' == 0 & ///
        censored`post' == 0 & DomicileCountryId != "USA" & inlist(mns_class, "E")

    *** Double gap ***
    
    if `year' <= 2015 {

        * USA
        qui replace value`year' = (value`pre' + value`pre' + value`post2') / 3 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre' == 0 & censored`post' == 1 & censored`post2' == 0 & ///
            DomicileCountryId == "USA"  & inlist(mns_class, "BC", "BSALS", "E")

        qui replace interpolated`year' = 1 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre' == 0 & censored`post' == 1 & censored`post2' == 0 & ///
            DomicileCountryId == "USA"  & inlist(mns_class, "BC", "BSALS", "E")
        
        * Non-USA
        qui replace value`year' = (value`pre' + value`pre' + value`post2') / 3 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre' == 0 & censored`post' == 1 & censored`post2' == 0 & ///
            DomicileCountryId != "USA"  & inlist(mns_class, "E")

        qui replace interpolated`year' = 1 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre' == 0 & censored`post' == 1 & censored`post2' == 0 & ///
            DomicileCountryId != "USA"  & inlist(mns_class, "E")
                
    }

    if `year' >= 2009 {

        * USA
        qui replace value`year' = (value`pre2' + value`post' + value`post') / 3 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre2' == 0 & censored`pre' == 1 & censored`post' == 0 & ///
            DomicileCountryId == "USA"  & inlist(mns_class, "BC", "BSALS", "E")

        qui replace interpolated`year' = 1 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre2' == 0 & censored`pre' == 1 & censored`post' == 0 & ///
            DomicileCountryId == "USA"  & inlist(mns_class, "BC", "BSALS", "E")
        
        * Non-USA
        qui replace value`year' = (value`pre2' + value`post' + value`post') / 3 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre2' == 0 & censored`pre' == 1 & censored`post' == 0 & ///
            DomicileCountryId != "USA"  & inlist(mns_class, "E")

        qui replace interpolated`year' = 1 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre2' == 0 & censored`pre' == 1 & censored`post' == 0 & ///
            DomicileCountryId != "USA"  & inlist(mns_class, "E")
        
    }

    *** Triple gap ***
    
    if `year' <= 2014 {

        * USA
        qui replace value`year' = (value`pre' + value`pre' + value`pre' + value`post3') / 4 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre' == 0 & censored`post' == 1 & censored`post2' == 1 & ///
            censored`post3' == 0 & DomicileCountryId == "USA"  & inlist(mns_class, "BC", "BSALS", "E")

        qui replace interpolated`year' = 1 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre' == 0 & censored`post' == 1 & censored`post2' == 1 & ///
            censored`post3' == 0 & DomicileCountryId == "USA"  & inlist(mns_class, "BC", "BSALS", "E")
     
        * Non-USA
        qui replace value`year' = (value`pre' + value`pre' + value`pre' + value`post3') / 4 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre' == 0 & censored`post' == 1 & censored`post2' == 1 & ///
            censored`post3' == 0 & DomicileCountryId != "USA"  & inlist(mns_class, "E")

        qui replace interpolated`year' = 1 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre' == 0 & censored`post' == 1 & censored`post2' == 1 & ///
            censored`post3' == 0 & DomicileCountryId != "USA"  & inlist(mns_class, "E")
        
    }

    
    if `year' >= 2010 {
        
        * USA
        qui replace value`year' = (value`pre3' + value`post' + value`post' + value`post') / 4 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre3' == 0 & censored`pre' == 1 & censored`pre2' == 1 & ///
            censored`post' == 0 & DomicileCountryId == "USA"  & inlist(mns_class, "BC", "BSALS", "E")

        qui replace interpolated`year' = 1 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre3' == 0 & censored`pre' == 1 & censored`pre2' == 1 & ///
            censored`post' == 0 & DomicileCountryId == "USA"  & inlist(mns_class, "BC", "BSALS", "E")

        * Non-USA
        qui replace value`year' = (value`pre3' + value`post' + value`post' + value`post') / 4 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre3' == 0 & censored`pre' == 1 & censored`pre2' == 1 & ///
            censored`post' == 0 & DomicileCountryId != "USA"  & inlist(mns_class, "E")

        qui replace interpolated`year' = 1 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre3' == 0 & censored`pre' == 1 & censored`pre2' == 1 & ///
            censored`post' == 0 & DomicileCountryId != "USA"  & inlist(mns_class, "E")

        
    }

    
    if `year' >= 2009 & `year' <= 2015 {

        * USA
        qui replace value`year' = (value`pre2' + value`post2') / 2 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre2' == 0 & censored`pre' == 1 & censored`post' == 1 & ///
            censored`post2' == 0 & DomicileCountryId == "USA"  & inlist(mns_class, "BC", "BSALS", "E")

        qui replace interpolated`year' = 1 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre2' == 0 & censored`pre' == 1 & censored`post' == 1 & ///
            censored`post2' == 0 & DomicileCountryId == "USA"  & inlist(mns_class, "BC", "BSALS", "E")

        * Non-USA
        qui replace value`year' = (value`pre2' + value`post2') / 2 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre2' == 0 & censored`pre' == 1 & censored`post' == 1 & ///
            censored`post2' == 0 & DomicileCountryId != "USA"  & inlist(mns_class, "E")

        qui replace interpolated`year' = 1 if censored`year' == 1 & ///
            missing(interpolated`year') & censored`pre2' == 0 & censored`pre' == 1 & censored`post' == 1 & ///
            censored`post2' == 0 & DomicileCountryId != "USA"  & inlist(mns_class, "E")
        
    }
    
}

* NIC case
forval year=2011/2015 {
    qui replace value`year' = 0 if DomicileCountryId == "USA" & mns_class == "BC" & iso == "NIC"
    qui replace value`year' = 0 if DomicileCountryId == "USA" & mns_class == "E" & iso == "NIC"
    qui replace interpolated`year' = 1 if DomicileCountryId == "USA" & mns_class == "BC" & iso == "NIC"
    qui replace interpolated`year' = 1 if DomicileCountryId == "USA" & mns_class == "BSALS" & iso == "NIC"
    qui replace interpolated`year' = 1 if DomicileCountryId == "USA" & mns_class == "E" & iso == "NIC"
}

qui replace value2011 = 20.16666667 if DomicileCountryId == "USA" & mns_class == "BSALS" & iso == "NIC"
qui replace value2012 = 16.33333333 if DomicileCountryId == "USA" & mns_class == "BSALS" & iso == "NIC"
qui replace value2013 = 12.5 if DomicileCountryId == "USA" & mns_class == "BSALS" & iso == "NIC"
qui replace value2014 = 8.666666667 if DomicileCountryId == "USA" & mns_class == "BSALS" & iso == "NIC"
qui replace value2015 = 4.833333333 if DomicileCountryId == "USA" & mns_class == "BSALS" & iso == "NIC"

qui replace interpolated2011 = 1 if DomicileCountryId == "CHE" & mns_class == "E" & iso == "SVK"
qui replace interpolated2012 = 1 if DomicileCountryId == "CHE" & mns_class == "E" & iso == "SVK"
qui replace interpolated2009 = 1 if DomicileCountryId == "CHE" & mns_class == "E" & iso == "VNM"
qui replace interpolated2010 = 1 if DomicileCountryId == "CHE" & mns_class == "E" & iso == "VNM"
qui replace interpolated2011 = 1 if DomicileCountryId == "CHE" & mns_class == "E" & iso == "VNM"

cap drop nonmissing*
cap drop censored* sum_censored
qui reshape long value interpolated, i(DomicileCountryId iso mns_class) j(year)

* Save the data
save $cmns1/temp/TIC_CPIS_merged_augmented_disagg, replace
drop interpolated
save $cmns1/temp/TIC_CPIS_merged_augmented_disagg_lean, replace

* ---------------------------------------------------------------------------------------------------
* Finishing up: non-augmented version
* ---------------------------------------------------------------------------------------------------

* TIC component
use $cmns1/holdings_master/TIC-Disaggregated-Clean-Main.dta, clear
gen Investor = "USA"
gen total_debt = government_debt_lt + government_debt_st + private_debt
gen government_debt = government_debt_lt + government_debt_st
keep Investor year common_equity total_debt corporate_debt government_debt abs iso
rename common_equity valueE
rename total_debt valueB
rename corporate_debt valueBC
rename government_debt valueBSALS
rename abs valueBSF
reshape long value, i(Investor iso year) j(asset_class) string
rename iso Issuer
rename year Year
replace asset_class = "Common Equity" if asset_class == "E"
replace asset_class = "Debt Securities" if asset_class == "B"
replace asset_class = "Corporate Debt Securities" if asset_class == "BC"
replace asset_class = "Government Debt Securities" if asset_class == "BSALS"
replace asset_class = "Asset-Backed Debt Securities" if asset_class == "BSF"
rename asset_class Asset_Class
rename value Position
save $cmns1/temp/tic_master_pre, replace

* CPIS component
use $cmns1/holdings_master/CPIS-Clean-Main.dta, clear
keep year investor issuer asset_class position
drop if investor == "USA"
replace asset_class = "Debt Securities" if asset_class == "Debt (All)"
replace asset_class = "Equity Securities" if asset_class == "Equity (All)"
rename investor Investor
rename issuer Issuer
rename year Year
rename asset_class Asset_Class
rename position Position
save $cmns1/temp/cpis_master_pre, replace

* Reference version (human-readable, non-augmented)
use $cmns1/temp/tic_master_pre.dta, clear
append using $cmns1/temp/cpis_master_pre.dta
save $cmns1/holdings_master/TIC-CPIS-Master-Main, replace

* ---------------------------------------------------------------------------------------------------
* Finishing up: augmented version, for restatements
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/TIC_CPIS_merged_augmented_disagg_lean, clear
rename DomicileCountryId Investor
rename year Year
rename iso Issuer
rename value Position
rename mns_class Asset_Class_Code
qui replace Asset_Class_Code = "BG" if Asset_Class_Code == "BSALS"
qui replace Asset_Class_Code = "EF" if Asset_Class_Code == "E" & Investor != "USA"
qui drop if Asset_Class_Code == "B" & Investor == "USA"
qui drop if Investor == "EMU" & Issuer == "LUX" & Asset_Class_Code == "EF" // Luxembourg drop for EMU equities (to avoid double-counting)
drop june_value
rename Position Position_Residency
gen Asset_Class = "All Bonds" if Asset_Class_Code == "B"
replace Asset_Class = "Asset-Backed Securities" if Asset_Class_Code == "BSF"
replace Asset_Class = "Common Equity" if Asset_Class_Code == "E"
replace Asset_Class = "Common Equity and Fund Shares" if Asset_Class_Code == "EF"
replace Asset_Class = "Corporate Bonds" if Asset_Class_Code == "BC"
replace Asset_Class = "Sovereign, Agency, and Muni Bonds" if Asset_Class_Code == "BG"
save $cmns1/holdings_master/TIC-CPIS-Augmented-Main, replace

log close
