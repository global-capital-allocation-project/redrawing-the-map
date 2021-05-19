* ---------------------------------------------------------------------------------------------------
* Representativeness_Analysis: Produces representativeness analysis using holdings of Norwegian SWF
* and U.S. insurers (Figure 9 in the paper)
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Representativeness_Analysis, replace

* ---------------------------------------------------------------------------------------------------
* Name cleaning procedures
* ---------------------------------------------------------------------------------------------------

cap program drop fill_in_country_codes
program fill_in_country_codes
    qui replace iso_code = "ASM" if country_name == "American Samoa"
    qui replace iso_code = "AND" if country_name == "Andorra"
    qui replace iso_code = "AIA" if country_name == "Anguilla"
    qui replace iso_code = "BES" if country_name == "Bonaire, Sint Eustatius and Saba"
    qui replace iso_code = "IOT" if country_name == "British Indian Ocean Territory"
    qui replace iso_code = "MAC" if country_name == "China, P.R.: Macao"
    qui replace iso_code = "CXR" if country_name == "Christmas Island"
    qui replace iso_code = "CCK" if country_name == "Cocos (Keeling) Islands"
    qui replace iso_code = "COK" if country_name == "Cook Islands"
    qui replace iso_code = "CUB" if country_name == "Cuba"
    qui replace iso_code = "FLK" if country_name == "Falkland Islands"
    qui replace iso_code = "FRO" if country_name == "Faroe Islands"
    qui replace iso_code = "ATF" if country_name == "French Southern Territories"
    qui replace iso_code = "PYF" if country_name == "French Territories: French Polynesia"
    qui replace iso_code = "NCL" if country_name == "French Territories: New Caledonia"
    qui replace iso_code = "GRL" if country_name == "Greenland"
    qui replace iso_code = "GLP" if country_name == "Guadeloupe"
    qui replace iso_code = "GUM" if country_name == "Guam"
    qui replace iso_code = "GGY" if country_name == "Guernsey"
    qui replace iso_code = "GUF" if country_name == "Guiana, French"
    qui replace iso_code = "XSN" if country_name == "International Organizations"
    qui replace iso_code = "IMN" if country_name == "Isle of Man"
    qui replace iso_code = "JEY" if country_name == "Jersey"
    qui replace iso_code = "PRK" if country_name == "Korea, Democratic People's Rep. of"
    qui replace iso_code = "LIE" if country_name == "Liechtenstein"
    qui replace iso_code = "MTQ" if country_name == "Martinique"
    qui replace iso_code = "MYT" if country_name == "Mayotte"
    qui replace iso_code = "MCO" if country_name == "Monaco"
    qui replace iso_code = "MSR" if country_name == "Montserrat"
    qui replace iso_code = "NRU" if country_name == "Nauru"
    qui replace iso_code = "NIU" if country_name == "Niue"
    qui replace iso_code = "NFK" if country_name == "Norfolk Island"
    qui replace iso_code = "OTH" if country_name == "Not Specified (including Confidential)"
    qui replace iso_code = "PCN" if country_name == "Pitcairn Islands"
    qui replace iso_code = "PRI" if country_name == "Puerto Rico"
    qui replace iso_code = "REU" if country_name == "Reunion"
    qui replace iso_code = "SHN" if country_name == "Saint Helena"
    qui replace iso_code = "SPM" if country_name == "Saint Pierre and Miquelon"
    qui replace iso_code = "SOM" if country_name == "Somalia"
    qui replace iso_code = "TKL" if country_name == "Tokelau Islands"
    qui replace iso_code = "TCA" if country_name == "Turks and Caicos Islands"
    qui replace iso_code = "VIR" if country_name == "US Virgin Islands"
    qui replace iso_code = "VAT" if country_name == "Vatican"
    qui replace iso_code = "VGB" if country_name == "Virgin Islands, British"
    qui replace iso_code = "WLF" if country_name == "Wallis and Futuna"
    qui replace iso_code = "ESH" if country_name == "Western Sahara"
    qui drop if country_name == "West Bank and Gaza"
    qui drop if country_name == "World"
    qui drop if country_name == "US Pacific Islands"
    qui replace iso_code = "AFG" if country_name == "Afghanistan, Islamic Rep. of"
    qui replace iso_code = "AND" if country_name == "Andorra, Principality of"
    qui replace iso_code = "ABW" if country_name == "Aruba, Kingdom of the Netherlands"
    qui replace iso_code = "BLR" if country_name == "Belarus, Rep. of"
    qui replace iso_code = "BES" if country_name == "Bonaire, St. Eustatius and Saba"
    qui replace iso_code = "CAF" if country_name == "Central African Rep."
    qui replace iso_code = "COM" if country_name == "Comoros, Union of the"
    qui replace iso_code = "COD" if country_name == "Congo, Dem. Rep. of the"
    qui replace iso_code = "HRV" if country_name == "Croatia, Rep. of"
    qui replace iso_code = "CUW" if country_name == "Curaçao, Kingdom of the Netherlands"
    qui replace iso_code = "CZE" if country_name == "Czech Rep."
    qui replace iso_code = "EGY" if country_name == "Egypt, Arab Rep. of"
    qui replace iso_code = "GNQ" if country_name == "Equatorial Guinea, Rep. of"
    qui replace iso_code = "ERI" if country_name == "Eritrea, The State of"
    qui replace iso_code = "EST" if country_name == "Estonia, Rep. of"
    qui replace iso_code = "FLK" if country_name == "Falkland Islands (Malvinas)"
    qui replace iso_code = "FJI" if country_name == "Fiji, Rep. of"
    qui replace iso_code = "PYF" if country_name == "French Polynesia"
    qui replace iso_code = "VAT" if country_name == "Holy See"
    qui replace iso_code = "IRN" if country_name == "Iran, Islamic Rep. of"
    qui replace iso_code = "PRK" if country_name == "Korea, Dem. People's Rep. of"
    qui replace iso_code = "KOR" if country_name == "Korea, Rep. of"
    qui replace iso_code = "KOS" if country_name == "Kosovo, Rep. of"
    qui replace iso_code = "KGZ" if country_name == "Kyrgyz Rep."
    qui replace iso_code = "LAO" if country_name == "Lao People's Dem. Rep."
    qui replace iso_code = "LSO" if country_name == "Lesotho, Kingdom of"
    qui replace iso_code = "MDG" if country_name == "Madagascar, Rep. of"
    qui replace iso_code = "MHL" if country_name == "Marshall Islands, Rep. of the"
    qui replace iso_code = "MRT" if country_name == "Mauritania, Islamic Rep. of"
    qui replace iso_code = "MDA" if country_name == "Moldova, Rep. of"
    qui replace iso_code = "NRU" if country_name == "Nauru, Rep. of"
    qui replace iso_code = "NLD" if country_name == "Netherlands, The"
    qui replace iso_code = "NCL" if country_name == "New Caledonia"
    qui replace iso_code = "PLW" if country_name == "Palau, Rep. of"
    qui replace iso_code = "POL" if country_name == "Poland, Rep. of"
    qui replace iso_code = "SMR" if country_name == "San Marino, Rep. of"
    qui replace iso_code = "SRB" if country_name == "Serbia, Rep. of"
    qui replace iso_code = "SXM" if country_name == "Sint Maarten, Kingdom of the Netherlands"
    qui replace iso_code = "SVK" if country_name == "Slovak Rep."
    qui replace iso_code = "SVN" if country_name == "Slovenia, Rep. of"
    qui replace iso_code = "SSD" if country_name == "South Sudan, Rep. of"
    qui replace iso_code = "SYR" if country_name == "Syrian Arab Rep."
    qui replace iso_code = "STP" if country_name == "São Tomé and Príncipe, Dem. Rep. of"
    qui replace iso_code = "TJK" if country_name == "Tajikistan, Rep. of"
    qui replace iso_code = "TZA" if country_name == "Tanzania, United Rep. of"
    qui replace iso_code = "TKL" if country_name == "Tokelau"
    qui replace iso_code = "VIR" if country_name == "United States Virgin Islands"
    qui replace iso_code = "UZB" if country_name == "Uzbekistan, Rep. of"
    qui replace iso_code = "WLF" if country_name == "Wallis and Futuna Islands"
    qui replace iso_code = "YEM" if country_name == "Yemen, Rep. of"
    qui replace iso_code = "ARM" if country_name == "Armenia, Rep. of"
    qui replace iso_code = "AZE" if country_name == "Azerbaijan, Rep. of"
    qui replace iso_code = "COG" if country_name == "Congo, Rep. of"
    qui replace iso_code = "CIV" if country_name == "Côte d'Ivoire"
    qui replace iso_code = "DOM" if country_name == "Dominican Rep."
    qui replace iso_code = "ETH" if country_name == "Ethiopia, The Federal Dem. Rep. of"
    qui replace iso_code = "MOZ" if country_name == "Mozambique, Rep. of"
    qui replace iso_code = "MKD" if strpos(country_name, "North Macedonia")
    qui drop if country_name == "Total Value of Investment"
    qui replace iso_code = "VEN" if country_name == "Venezuela, Rep. Bolivariana de"
end

* ---------------------------------------------------------------------------------------------------
* Portfolio restatements for comparison with US insurers
* ---------------------------------------------------------------------------------------------------

* Get insurance matrix: corporate bonds
import excel using $cmns1/insurance/reallocation_matrices/insurance_reallocation_matrix_corp_2017.xls, clear firstrow
rename * _*
rename _cgs_domicile Residency
qui reshape long _, i(Residency) j(Nationality) string
rename _ Reallocation_Share
drop if Reallocation_Share == .
gen Asset_Class = "Corporate Bonds"
save $cmns1/temp/insurance/corporate_bonds_2017_matrix, replace

* Get insurance matrix: equities
import excel using $cmns1/insurance/reallocation_matrices/insurance_reallocation_matrix_equity_2017.xls, clear firstrow
rename * _*
rename _cgs_domicile Residency
qui reshape long _, i(Residency) j(Nationality) string
rename _ Reallocation_Share
drop if Reallocation_Share == .
gen Asset_Class = "Equity"
save $cmns1/temp/insurance/equity_2017_matrix, replace

* Append insurance matrices
use $cmns1/temp/insurance/equity_2017_matrix, clear
append using $cmns1/temp/insurance/corporate_bonds_2017_matrix.dta
save $cmns1/temp/insurance/reallocation_shares_2017, replace

* Get Morningstar matrix: corporate bonds
import excel using $scratch/Reallocation_Matrix_USA_Corporate_Bonds_2017.xls, clear firstrow
rename Immediate_Destination Residency
rename * _*
rename _Residency Residency
qui reshape long _, i(Residency) j(Nationality) string
rename _ Reallocation_Share
drop if Reallocation_Share == .
gen Asset_Class = "Corporate Bonds"
save $cmns1/temp/insurance/corporate_bonds_2017_matrix_mns, replace

* Get Morningstar matrix: equities
import excel using $scratch/Reallocation_Matrix_USA_Equities_2017.xls, clear firstrow
rename Immediate_Destination Residency
rename * _*
rename _Residency Residency
qui reshape long _, i(Residency) j(Nationality) string
rename _ Reallocation_Share
drop if Reallocation_Share == .
gen Asset_Class = "Equity"
save $cmns1/temp/insurance/equity_2017_matrix_mns, replace

* Append Morningstar matrices
use $cmns1/temp/insurance/equity_2017_matrix_mns, clear
append using $cmns1/temp/insurance/corporate_bonds_2017_matrix_mns
save $cmns1/temp/insurance/reallocation_shares_2017_mns, replace

* Merge the SWF and Morningstar reallocation shares
use $cmns1/temp/insurance/reallocation_shares_2017, clear
rename Reallocation_Share Realloc_Share_INS
qui mmerge Residency Nationality Asset_Class using $cmns1/temp/insurance/reallocation_shares_2017_mns.dta
rename Reallocation_Share Realloc_Share_MNS
qui replace Realloc_Share_INS = Realloc_Share_INS / 100
qui replace Realloc_Share_MNS = Realloc_Share_MNS / 100
drop _merge
bys Residency Asset_Class: egen Tot_INS = total(Realloc_Share_INS)
bys Residency Asset_Class: egen Tot_MNS = total(Realloc_Share_MNS)
qui replace Realloc_Share_MNS = 1 if missing(Realloc_Share_MNS) & Residency == Nationality & Tot_MNS < .5
qui replace Realloc_Share_MNS = 0 if missing(Realloc_Share_MNS) & Residency != Nationality & Tot_MNS < .5
qui replace Realloc_Share_INS = 1 if missing(Realloc_Share_INS) & Residency == Nationality & Tot_INS < .5
qui replace Realloc_Share_INS = 0 if missing(Realloc_Share_INS) & Residency != Nationality & Tot_INS < .5
drop Tot_*
order Residency Nationality Asset_Class
save $cmns1/temp/insurance/reallocation_shares_2017_merged, replace

* Get TIC and merge with shares
use $cmns1/holdings_master/TIC-CPIS-Master-Main.dta, clear
keep if Investor == "USA" & Year == 2017
keep if inlist(Asset_Class, "Corporate Debt Securities", "Common Equity")
replace Asset_Class = "Corporate Bonds" if Asset_Class == "Corporate Debt Securities"
replace Asset_Class = "Equity" if Asset_Class == "Common Equity"
qui replace Position = 0 if missing(Position)
rename Position TIC
local new = _N + 1
set obs `new'
qui replace Investor = "USA" if _n == _N
qui replace Year = 2017 if _n == _N
qui replace Issuer = "USA" if _n == _N
qui replace Asset_Class = "Equity" if _n == _N
qui replace TIC = 19530000 if _n == _N
local new = _N + 1
set obs `new'
qui replace Investor = "USA" if _n == _N
qui replace Year = 2017 if _n == _N
qui replace Issuer = "USA" if _n == _N
qui replace Asset_Class = "Corporate Bonds" if _n == _N
qui replace TIC = 5247000 if _n == _N
tempfile tic_vec
save `tic_vec', replace

use `tic_vec', clear
count
qui mmerge Asset_Class Issuer using $cmns1/temp/insurance/reallocation_shares_2017_merged, ///
    umatch(Asset_Class Residency) ///
    ukeep(Nationality Realloc_Share_MNS Realloc_Share_INS) unmatched(m)
count
replace Realloc_Share_INS = 1 if missing(Nationality) & _merge == 1
replace Realloc_Share_MNS = 1 if missing(Nationality) & _merge == 1
replace Nationality = Issuer if missing(Nationality)
drop _merge
qui replace Realloc_Share_INS = 0 if missing(Realloc_Share_INS)
qui replace Realloc_Share_MNS = 0 if missing(Realloc_Share_MNS)

* Tax haven indicators
gen TH = 0
qui replace TH = 1 if inlist(Issuer, $tax_haven_1)
qui replace TH = 1 if inlist(Issuer, $tax_haven_2)
qui replace TH = 1 if inlist(Issuer, $tax_haven_3)
qui replace TH = 1 if inlist(Issuer, $tax_haven_4)
qui replace TH = 1 if inlist(Issuer, $tax_haven_5)
qui replace TH = 1 if inlist(Issuer, $tax_haven_6)
qui replace TH = 1 if inlist(Issuer, $tax_haven_7)
qui replace TH = 1 if inlist(Issuer, $tax_haven_8)

* Add TH-only shares
foreach x in "MNS" "INS" {
    qui replace Realloc_Share_`x' = 1 if missing(Realloc_Share_`x') & Issuer == Nationality
    qui replace Realloc_Share_`x' = 0 if missing(Realloc_Share_`x') & Issuer != Nationality
    gen Realloc_Share_`x'_THO = Realloc_Share_`x'
    qui replace Realloc_Share_`x'_THO = 1 if TH == 0 & Issuer == Nationality
    qui replace Realloc_Share_`x'_THO = 0 if TH == 0 & Issuer != Nationality
}
drop TH
tempfile restatement_frame
qui save `restatement_frame', replace

* Insurance-based restatement
use `restatement_frame', clear
gen Restatement_INS = TIC * Realloc_Share_INS
gen Restatement_INS_THO = TIC * Realloc_Share_INS_THO
collapse (sum) Restatement_INS Restatement_INS_THO, by(Asset_Class Nationality)
rename Nationality Issuer
tempfile ins_restatement
save `ins_restatement', replace

* Regular restatement
use `restatement_frame', clear
gen Restatement_MNS = TIC * Realloc_Share_MNS
gen Restatement_MNS_THO = TIC * Realloc_Share_MNS_THO
collapse (sum) Restatement_MNS Restatement_MNS_THO, by(Asset_Class Nationality)
rename Nationality Issuer
tempfile mns_restatement
save `mns_restatement', replace

* Merge all the portfolio restatements
use `tic_vec', clear
keep Asset_Class Issuer TIC
qui mmerge Asset_Class Issuer using `mns_restatement'
qui mmerge Asset_Class Issuer using `ins_restatement'
drop _merge
replace TIC = 0 if missing(TIC)
replace Restatement_MNS = 0 if missing(Restatement_MNS)
replace Restatement_INS = 0 if missing(Restatement_INS)
replace Restatement_MNS_THO = 0 if missing(Restatement_MNS_THO)
replace Restatement_INS_THO = 0 if missing(Restatement_INS_THO)
save $cmns1/temp/insurance/restatement_twoways_adjusted, replace

* ---------------------------------------------------------------------------------------------------
* Portfolio restatements for comparison with Norwegian SWF
* ---------------------------------------------------------------------------------------------------

* Construct Morningstar reallocation matrix
use $cmns1/holdings_master/mns_issuer_summary.dta, clear
keep if Domicile == "NOR" & year == 2017
replace asset_class = "Bonds" if strpos(asset_class, "Bonds")

* Extremely large position, likely misreporting
qui drop if issuer_number == "L8882U" & asset_class == "Equity"

collapse (sum) marketvalue_usd, by(year asset_class DomicileCountryId cgs_domicile country_bg)
drop year
rename DomicileCountryId Investor
rename cgs_domicile Residency
rename country_bg Nationality
rename marketvalue_usd Morningstar
replace Morningstar = Morningstar / 1e6
rename asset_class Asset_Class
bys Asset_Class Residency: egen Morningstar_Total_Res = total(Morningstar)
tempfile morningstar_share
drop Investor
save `morningstar_share', replace

* Merge the SWF and Morningstar reallocation shares
use $cmns1/temp/norway_swf/reallocation_shares_2017_adjusted, clear
drop Reallocation_Share
rename Residency_Total SWF_Total_Res
rename MarketValueUSD SWF
replace Asset_Class = "Bonds" if strpos(Asset_Class, "Bonds")
count
collapse (sum) SWF SWF_Total_Res, by(Nationality Residency Asset_Class)
count
qui mmerge Nationality Residency Asset_Class using `morningstar_share'
cap drop _merge
replace SWF = 0 if missing(SWF)
replace Morningstar = 0 if missing(Morningstar)

preserve
keep Residency Asset_Class SWF_Total_Res Morningstar_Total_Res
collapse (firstnm) SWF_Total_Res Morningstar_Total_Res, by(Residency Asset_Class)
tempfile residency_totals
qui save `residency_totals', replace
restore
cap drop *Total_Res
qui mmerge Residency Asset_Class using `residency_totals'
drop _merge

gen Realloc_Share_MNS = Morningstar / Morningstar_Total_Res
gen Both = SWF + Morningstar
cap drop SWF_Total_Res
bys Asset_Class Residency: egen Both_Total_Res = total(Both)
bys Asset_Class Residency: egen SWF_Total_Res = total(SWF)
gen Realloc_Share_Both = Both / Both_Total_Res
gen Realloc_Share_SWF = SWF / SWF_Total_Res

* Luxembourg adjustment
qui replace Realloc_Share_MNS = 1 if Asset_Class == "Equity" & Residency == "LUX" & Nationality == "LUX"
qui replace Realloc_Share_MNS = 0 if Asset_Class == "Equity" & Residency == "LUX" & Nationality != "LUX"
qui replace Realloc_Share_SWF = 1 if Asset_Class == "Equity" & Residency == "LUX" & Nationality == "LUX"
qui replace Realloc_Share_SWF = 0 if Asset_Class == "Equity" & Residency == "LUX" & Nationality != "LUX"
qui replace Realloc_Share_Both = 1 if Asset_Class == "Equity" & Residency == "LUX" & Nationality == "LUX"
qui replace Realloc_Share_Both = 0 if Asset_Class == "Equity" & Residency == "LUX" & Nationality != "LUX"

* Save the merged reallocation shares
save $cmns1/temp/norway_swf/realloc_shares_twoways_adjusted, replace

* Corrected Norwegian CPIS for baseline year
use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
keep if Year == 2017 & Investor == "NOR"
keep Asset_Class Issuer Position_Residency
rename Position_Residency Corrected_CPIS
replace Asset_Class = "Bonds" if Asset_Class == "All Bonds"
replace Asset_Class = "Equity" if Asset_Class == "Common Equity and Fund Shares"
save $cmns1/temp/norway_swf/corrected_cpis, replace

* Merge corrected CPIS and reallocation shares
use $cmns1/temp/norway_swf/corrected_cpis.dta, clear
qui mmerge Asset_Class Issuer using $cmns1/temp/norway_swf/realloc_shares_twoways_adjusted.dta, ///
    umatch(Asset_Class Residency) ///
    ukeep(Nationality Realloc_Share_Both Realloc_Share_MNS Realloc_Share_SWF)
drop _merge

* Tax haven indicators
gen TH = 0
qui replace TH = 1 if inlist(Issuer, $tax_haven_1)
qui replace TH = 1 if inlist(Issuer, $tax_haven_2)
qui replace TH = 1 if inlist(Issuer, $tax_haven_3)
qui replace TH = 1 if inlist(Issuer, $tax_haven_4)
qui replace TH = 1 if inlist(Issuer, $tax_haven_5)
qui replace TH = 1 if inlist(Issuer, $tax_haven_6)
qui replace TH = 1 if inlist(Issuer, $tax_haven_7)
qui replace TH = 1 if inlist(Issuer, $tax_haven_8)

* Add TH-only shares
foreach x in "MNS" "SWF" "Both" {
    qui replace Realloc_Share_`x' = 1 if missing(Realloc_Share_`x') & Issuer == Nationality
    qui replace Realloc_Share_`x' = 0 if missing(Realloc_Share_`x') & Issuer != Nationality
    gen Realloc_Share_`x'_THO = Realloc_Share_`x'
    qui replace Realloc_Share_`x'_THO = 1 if TH == 0 & Issuer == Nationality
    qui replace Realloc_Share_`x'_THO = 0 if TH == 0 & Issuer != Nationality
}
drop TH
tempfile restatement_frame
qui save `restatement_frame', replace

* SWF-based restatement
use `restatement_frame', clear
gen Restatement_SWF = Corrected_CPIS * Realloc_Share_SWF
gen Restatement_SWF_THO = Corrected_CPIS * Realloc_Share_SWF_THO
collapse (sum) Restatement_SWF Restatement_SWF_THO, by(Asset_Class Nationality)
rename Nationality Issuer
tempfile swf_restatement
save `swf_restatement', replace

* Regular restatement
use `restatement_frame', clear
gen Restatement_MNS = Corrected_CPIS * Realloc_Share_MNS
gen Restatement_MNS_THO = Corrected_CPIS * Realloc_Share_MNS_THO
collapse (sum) Restatement_MNS Restatement_MNS_THO, by(Asset_Class Nationality)
rename Nationality Issuer
tempfile mns_restatement
save `mns_restatement', replace

* Merge all the portfolio restatements
use $cmns1/temp/norway_swf/corrected_cpis.dta, clear
keep Asset_Class Issuer Corrected_CPIS
qui mmerge Asset_Class Issuer using `mns_restatement'
qui mmerge Asset_Class Issuer using `swf_restatement'
drop _merge
replace Corrected_CPIS = 0 if missing(Corrected_CPIS)
replace Restatement_MNS = 0 if missing(Restatement_MNS)
replace Restatement_SWF = 0 if missing(Restatement_SWF)
replace Restatement_MNS_THO = 0 if missing(Restatement_MNS_THO)
replace Restatement_SWF_THO = 0 if missing(Restatement_SWF_THO)
unique Asset_Class Issuer
save $cmns1/temp/norway_swf/restatement_twoways_adjusted, replace

* ---------------------------------------------------------------------------------------------------
* Comparison figure: US insurance, corporate bonds
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/insurance/restatement_twoways_adjusted.dta, clear
qui keep if Asset_Class == "Corporate Bonds"

gen Delta_MNS = (Restatement_MNS - TIC)
gen Delta_INS = (Restatement_INS - TIC)
gen Delta_MNS_THO = (Restatement_MNS_THO - TIC)
gen Delta_INS_THO = (Restatement_INS_THO - TIC)

qui replace Delta_MNS = Delta_MNS / 1e3
qui replace Delta_INS = Delta_INS / 1e3
qui replace Delta_MNS_THO = Delta_MNS_THO / 1e3
qui replace Delta_INS_THO = Delta_INS_THO / 1e3
qui drop if Issuer == "XSN"
qui drop if Issuer == "OTH"
label var Delta_MNS_THO "Baseline {&Delta}, TH Only (USD Billion)"
label var Delta_MNS "Baseline {&Delta}, Full Nationality (USD Billion)"
label var Delta_INS_THO "Insurance {&Delta}, TH Only (USD Billion)"
label var Delta_INS "Insurance {&Delta}, Full Nationality (USD Billion)"
save $cmns1/temp/usa_issuance_compare, replace


use $cmns1/temp/usa_issuance_compare, clear
egen clock_ins = mlabvpos(Delta_INS_THO Delta_MNS_THO)
cap drop Label
gen Label = Issuer
qui replace Label = "" if abs(Delta_MNS_THO) < 12 & ~inlist(Issuer, "RUS")
qui replace Label = "" if Issuer == "XSN"
qui replace Label = "" if Issuer == "GGY"
qui replace Label = "" if Issuer == "ITA"
qui replace Label = "" if Issuer == "JPN"

qui replace clock_ins = 9 if Issuer == "USA"
qui replace clock_ins = 12 if Issuer == "LUX"
qui replace clock_ins = 6 if Issuer == "CYM"    
qui replace clock_ins = 4 if Issuer == "LUX"
qui replace clock_ins = 9 if Issuer == "CHE"
qui replace clock_ins = 3 if Issuer == "CHN"
qui replace clock_ins = 4 if Issuer == "GBR"
qui replace clock_ins = 6 if Issuer == "VGB"
qui replace clock_ins = 7 if Issuer == "DEU"

scatter Delta_INS_THO Delta_MNS_THO if Asset_Class == "Corporate Bonds", ///
    graphregion(color(white)) mlab(Label) msymbol(Oh) mcolor(blue) mlabcolor(blue) ///
    mlabvpos(clock_ins) ///
    xtitle("Baseline {&Delta} (USD Billions)") ytitle("Insurance-Based {&Delta} (USD Billions)") ///
    title("", color(black) size(medsmall)) || function y=x, range(-100 110) clpat(dash) clcolor(gray) legend(off)

graph export $cmns1/graphs/representativeness_us_corporate_bonds.pdf, as(pdf) replace

* ---------------------------------------------------------------------------------------------------
* Comparison figure: US insurance, equity
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/insurance/restatement_twoways_adjusted.dta, clear
qui keep if Asset_Class == "Equity"

gen Delta_MNS = (Restatement_MNS - TIC)
gen Delta_INS = (Restatement_INS - TIC)
gen Delta_MNS_THO = (Restatement_MNS_THO - TIC)
gen Delta_INS_THO = (Restatement_INS_THO - TIC)

qui replace Delta_MNS = Delta_MNS / 1e3
qui replace Delta_INS = Delta_INS / 1e3
qui replace Delta_MNS_THO = Delta_MNS_THO / 1e3
qui replace Delta_INS_THO = Delta_INS_THO / 1e3
qui drop if Issuer == "XSN"
qui drop if Issuer == "OTH"
label var Delta_MNS_THO "Baseline {&Delta}, TH Only (USD Billion)"
label var Delta_MNS "Baseline {&Delta}, Full Nationality (USD Billion)"
label var Delta_INS_THO "Insurance {&Delta}, TH Only (USD Billion)"
label var Delta_INS "Insurance {&Delta}, Full Nationality (USD Billion)"
save $cmns1/temp/usa_issuance_compare_equity, replace

use $cmns1/temp/usa_issuance_compare_equity, clear
egen clock_ins = mlabvpos(Delta_INS_THO Delta_MNS_THO)

cap drop Label
gen Label = Issuer
qui replace Label = "" if abs(Delta_MNS_THO) < 50
qui replace Label = "" if Issuer == "JEY"
qui replace Label = "" if Issuer == "CUW"

qui replace clock_ins = 12 if Issuer == "CYM"
qui replace clock_ins = 6 if Issuer == "CYM"
qui replace clock_ins = 6 if Issuer == "BMU"
qui replace clock_ins = 9 if Issuer == "USA"
qui replace clock_ins = 12 if Issuer == "LUX"
qui replace clock_ins = 6 if Issuer == "CYM"    

scatter Delta_INS_THO Delta_MNS_THO if Asset_Class == "Equity", ///
    graphregion(color(white)) mlab(Label) msymbol(Oh) mcolor(blue) mlabcolor(blue) ///
    mlabvpos(clock_ins) xlab(-500(250)500) ylab(-500(250)750) ///
    xtitle("Baseline {&Delta} (USD Billions)") ytitle("Insurance-Based {&Delta} (USD Billions)") ///
    title("", color(black) size(medsmall)) ///
    || function y=x, range(-600 650) clpat(dash) clcolor(gray) ///
    legend(off)

graph export $cmns1/graphs/representativeness_us_common_equity.pdf, as(pdf) replace

* ---------------------------------------------------------------------------------------------------
* Comparison figure: Norway SWF, bonds
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/norway_swf/restatement_twoways_adjusted.dta, clear
qui keep if Asset_Class == "Bonds"
gen Delta_MNS = Restatement_MNS - Corrected_CPIS
gen Delta_SWF = Restatement_SWF - Corrected_CPIS
gen Delta_MNS_THO = Restatement_MNS_THO - Corrected_CPIS
gen Delta_SWF_THO = Restatement_SWF_THO - Corrected_CPIS
qui drop if Issuer == "XSN"
qui drop if Issuer == "OTH"
qui drop if missing(Issuer)
gsort -Delta_MNS_THO
qui replace Delta_MNS = Delta_MNS / 1e3
qui replace Delta_SWF = Delta_SWF / 1e3
qui replace Delta_MNS_THO = Delta_MNS_THO / 1e3
qui replace Delta_SWF_THO = Delta_SWF_THO / 1e3
save $scratch/nor_swf_compare, replace

use $scratch/nor_swf_compare, clear
keep if Asset_Class == "Bonds"
egen clock_swf = mlabvpos(Delta_SWF_THO Delta_MNS_THO)

cap drop Label
gen Label = Issuer
qui replace Label = "" if abs(Delta_MNS_THO) < .5
qui replace Label = "" if Issuer == "BEL"
qui replace Label = "" if Issuer == "BMU"
qui replace Label = "" if Issuer == "JEY"

qui replace clock_swf = 3 if Issuer == "LUX"
qui replace clock_swf = 9 if Issuer == "IRL"
qui replace clock_swf = 9 if Issuer == "ZAF"
qui replace clock_swf = 12 if Issuer == "ZMB"
qui replace clock_swf = 12 if Issuer == "CHE"
qui replace clock_swf = 4 if Issuer == "GBR"
qui replace clock_swf = 9 if Issuer == "CYM"

scatter Delta_SWF_THO Delta_MNS_THO, ///
    graphregion(color(white)) mlab(Label) msymbol(Oh) mcolor(blue) mlabcolor(blue) ///
    mlabvpos(clock_swf) xtitle("Baseline {&Delta} (USD Billions)") ytitle("SWF-Based {&Delta} (USD Billions)") ///
    title("", color(black) size(medsmall)) ///
    || function y=x, range(-6 4) clpat(dash) clcolor(gray) xlab(-6(2)4) ylab(-6(2)4) legend(off)

graph export $cmns1/graphs/representativeness_norway_bonds.pdf, as(pdf) replace

* ---------------------------------------------------------------------------------------------------
* Comparison figure: Norway SWF, equity
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/norway_swf/restatement_twoways_adjusted.dta, clear
qui keep if Asset_Class == "Equity"
gen Delta_MNS = Restatement_MNS - Corrected_CPIS
gen Delta_SWF = Restatement_SWF - Corrected_CPIS
gen Delta_MNS_THO = Restatement_MNS_THO - Corrected_CPIS
gen Delta_SWF_THO = Restatement_SWF_THO - Corrected_CPIS
qui drop if Issuer == "XSN"
qui drop if Issuer == "OTH"
qui drop if missing(Issuer)
gsort -Delta_MNS_THO
qui replace Delta_MNS = Delta_MNS / 1e3
qui replace Delta_SWF = Delta_SWF / 1e3
qui replace Delta_MNS_THO = Delta_MNS_THO / 1e3
qui replace Delta_SWF_THO = Delta_SWF_THO / 1e3
save $scratch/nor_swf_compare_equity, replace

use $scratch/nor_swf_compare_equity, clear
egen clock_swf = mlabvpos(Delta_SWF_THO Delta_MNS_THO)

cap drop Label
gen Label = Issuer
qui replace Label = "" if abs(Delta_MNS_THO) < 1.5
qui replace Label = "" if Issuer == "JEY"

qui replace clock_swf = 9 if Issuer == "USA"
qui replace clock_swf = 5 if Issuer == "CHN"
qui replace clock_swf = 12 if Issuer == "GBR"
qui replace clock_swf = 12 if Issuer == "BMU"
qui replace clock_swf = 3 if Issuer == "NOR"

scatter Delta_SWF_THO Delta_MNS_THO, ///
    graphregion(color(white)) mlab(Label) msymbol(Oh) mcolor(blue) mlabcolor(blue) ///
    mlabvpos(clock_swf) xtitle("Baseline {&Delta} (USD Billions)") ytitle("SWF-Based {&Delta} (USD Billions)") ///
    title("", color(black) size(medsmall)) ///
    || function y=x, range(-20 20) clpat(dash) clcolor(gray) legend(off)

graph export $cmns1/graphs/representativeness_norway_equity.pdf, as(pdf) replace

log close
