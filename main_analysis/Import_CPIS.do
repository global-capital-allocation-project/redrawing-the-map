* ---------------------------------------------------------------------------------------------------
* Import_CPIS: Imports and cleans data from CPIS
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Import_CPIS, replace

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
end

* ---------------------------------------------------------------------------------------------------
* Basic CPIS import
* ---------------------------------------------------------------------------------------------------

* to be used later for merging in iso codes
import excel $raw/Macro/Concordances/imf_country_codes.xlsx, clear firstrow
cap drop imf_code
save $cmns1/temp/imf_country_codes.dta, replace

tempfile debt

* we import all as string because it will help us later when creating the variable, censored, that tells us if the data is censored for a given (country,year) pair
import delimited "$raw/CPIS/CPIS_debt_01-29-2020 19-01-34-56_timeSeries.csv", clear stringc(_all)
ren countrycode investor
ren ïcountryname investor_name
ren counterpartcountrycode issuer
ren counterpartcountryname issuer_name

* merges "C" into positions row - this is to allow us to create the, censored variable once we reshape
collapse (lastnm) v*, by( issuer issuer_name indicatorname  issuer_name investor investor_name)

* double checking that these variables uniquely identify our data in order to use them for reshaping
isid issuer_name investor_name indicatorname

* this drop the extra variable that is in the dataset
missings dropvars, force

* reshape from wide to long
reshape long v, i(issuer_name investor_name indicatorname ) j(year)

* generate censored variable
gen censored = (v=="C")

* this is to double check that censored makes sense - each entry is either censored or not
collapse (mean) censored, by(issuer_name issuer indicatorname investor_name investor year v)
tab censored

* destring the position variable, "v", issuer, and investor codes 
destring v issuer investor, replace force 

* in this file this line doesn't do anything, but if our data had multipy indicators 
* in the dataset this would sum the indicatros values 
collapse (sum) v, by(issuer_name issuer investor_name investor year censored )

* fix years
replace year = 1997 if year == 12
forval i =13/30{
    replace year = 1988 + `i' if year == `i'
}
ren v position
list in 1/10
gen asset_class = "Debt (All)"

* make positon misising if it is censored
replace position = . if censored == 1

* we save this in a tempfile because we will use it to merge with 
* our other dataset after cleaning that dataset
save `debt' 

* we import all as string because it will help us later when creating the variable, censored, that tells us if the data is censored for a given (country,year) pair
import delimited "$raw/CPIS/CPIS_equity_01-29-2020 19-27-19-25_timeSeries.csv", clear  stringc(_all) 

* we rename the following variables to more useable names
ren countrycode investor
ren ïcountryname investor_name
ren counterpartcountrycode issuer
ren counterpartcountryname issuer_name

* merges "C" into positions row - this is to allow us to create the, censored variable once we reshape
collapse (lastnm) v*, by( issuer issuer_name indicatorname  issuer_name investor investor_name)

* double checking that these variables uniquely identify our data in order to use them for reshaping
isid issuer_name investor_name indicatorname

* this drop the extra variable that is in the dataset
missings dropvars, force

* reshape from wide to long
reshape long v, i(issuer_name investor_name indicatorname ) j(year)

* generate censored variable 
gen censored = (v=="C")
    
* this is to double check that censored makes sense - each entry is either censored or not
collapse (mean) censored, by(issuer_name issuer indicatorname investor_name investor year v)

* destring the position variable, "v", issuer, and investor codes 
destring v issuer investor, replace force 

* in this file this line doesn't do anything, but if our data had multipy indicators in the dataset this would sum the indicatros values 
collapse (sum) v, by(issuer_name issuer investor_name investor year censored )

* fix years
replace year = 1997 if year == 12
forval i =13/30{
    replace year = 1988 + `i' if year == `i'
}
ren v position
gen asset_class = "Equity (All)"

* make positon missing if it is censored
replace position = . if censored == 1

append using `debt'

* we merge with iso3 codes for investors
ren investor imf_code
mmerge imf_code using $cmns1/temp/imf_country_codes.dta, ukeep(iso_code) unmatched(m)
rename iso_code investor
ren imf_code investor_imf_code
qui replace investor = "MAC" if investor_name == "China, P.R.: Macao"
qui replace investor = "CUW" if investor_name == "Curacao & St. Maarten"
qui replace investor = "GGY" if investor_name == "Guernsey"
qui replace investor = "IMN" if investor_name == "Isle of Man"
qui replace investor = "JEY" if investor_name == "Jersey"
qui drop if investor_name == "West Bank and Gaza"
assert ~missing(investor)
drop _merge

* we merge with iso3 codes for issuers
ren issuer imf_code
mmerge imf_code using $cmns1/temp/imf_country_codes.dta, ukeep(iso_code) unmatched(m)
rename iso_code issuer
ren imf_code issuer_imf_code
fill_in_country_codes

assert ~missing(issuer)
assert ~missing(investor)
assert ~missing(issuer_name)
assert ~missing(investor_name)
cap drop _merge

* get data into millions 
replace position = position/1000000

* label the data
label var issuer_name "Issuer of Security"
label var investor_name "Residency of investors"
label var issuer "ISO3 alpha codes for issuer"
label var investor "ISO3 alpha codes for investor"
label var year "Year of investment - reported in December of reported year"
label var position "in millions of USD"
label var asset_class "Asset Class"
order investor_name issuer_name investor issuer year position asset_class
sort investor_name issuer_name asset_class year
save $cmns1/temp/CPIS-Main.dta, replace 

* ---------------------------------------------------------------------------------------------------
* Internal version
* ---------------------------------------------------------------------------------------------------

* EMU censoring
use $cmns1/temp/CPIS-Main.dta, clear
keep if inlist(investor, $eu1) | inlist(investor, $eu2) | inlist(investor, $eu3)
keep if censored == 1
gen censorer = investor
drop *imf_code
drop investor_name investor position
sort year issuer asset_class, stable
by year issuer asset_class : gen all_censorers = censorer[1]
by year issuer asset_class : replace all_censorers = all_censorers[_n-1] + " " + censorer if _n > 1
by year issuer asset_class : replace all_censorers = all_censorers[_N]
drop censorer
by year issuer asset_class: keep if _n == _N
drop if year < 2007 | year > 2017
drop issuer_name
gen investor = "EMU"
save $cmns1/temp/cpis_emu_censorers, replace

use $cmns1/temp/CPIS-Main.dta, clear
keep if inlist(investor, "USA", "CAN", "GBR", "EMU") | inlist(investor, "AUS", "CHE", "NOR", "SWE", "DNK") ///
    | inlist(investor, $eu1) | inlist(investor, $eu2) | inlist(investor, $eu3)
bys investor issuer asset_class year: gen N = _N
assert N == 1
cap drop N
assert investor != "EMU"
replace investor_name = "European Monetary Union" if ///
    inlist(investor, $eu1) | inlist(investor, $eu2) | inlist(investor, $eu3)
replace investor = "EMU" if inlist(investor, $eu1) | inlist(investor, $eu2) | inlist(investor, $eu3)
drop *imf_code
gcollapse (sum) position censored, by(investor investor_name issuer issuer_name year asset_class)
assert investor == "EMU" if censored > 1
drop if investor == "USA"
drop if year < 2007 | year > 2017
drop if investor == "AUS" & year < 2017

qui mmerge investor issuer year asset_class using $cmns1/temp/cpis_emu_censorers.dta, unmatched(m)
drop _merge
assert investor == "EMU" if ~missing(all_censorers)
assert ~missing(all_censorers) if investor == "EMU" & censored > 0

* GBR -> LUX censored in 2010; carry over from 2009
replace position = 34386.4315456196 if investor == "GBR" & issuer == "LUX" & asset_class == "Equity (All)" & year == 2010
replace position = 54975.1343018518 if investor == "GBR" & issuer == "LUX" & asset_class == "Debt (All)" & year == 2010
replace censored = -1 if investor == "GBR" & issuer == "LUX" & year == 2010

* AUS 2017: fill in with June values for missing December TH positions - Equity
replace position = 25192.8384 if investor == "AUS" & issuer == "CYM" & asset_class == "Equity (All)" & year == 2017
replace position = 5562.08520 if investor == "AUS" & issuer == "IRL" & asset_class == "Equity (All)" & year == 2017
replace position = 2692.9692 if investor == "AUS" & issuer == "LUX" & asset_class == "Equity (All)" & year == 2017
replace position = 90.7656 if investor == "AUS" & issuer == "PAN" & asset_class == "Equity (All)" & year == 2017

replace censored = -1 if investor == "AUS" & issuer == "CYM" & asset_class == "Equity (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "IRL" & asset_class == "Equity (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "LUX" & asset_class == "Equity (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "PAN" & asset_class == "Equity (All)" & year == 2017

* AUS 2017: fill in with June values for missing December TH positions - Debt
replace position = 6120.5244 if investor == "AUS" & issuer == "CYM" & asset_class == "Debt (All)" & year == 2017
replace position = 614.5908 if investor == "AUS" & issuer == "IRL" & asset_class == "Debt (All)" & year == 2017
replace position = 7790.4576 if investor == "AUS" & issuer == "LUX" & asset_class == "Debt (All)" & year == 2017

replace censored = -1 if investor == "AUS" & issuer == "CYM" & asset_class == "Debt (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "IRL" & asset_class == "Debt (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "LUX" & asset_class == "Debt (All)" & year == 2017

* For EMU we just use the data we have
replace censored = 0 if investor == "EMU"
replace position = . if investor == "GBR" & censored == 1
replace position = . if investor == "CHE" & censored == 1
save $cmns1/holdings_master/CPIS-Clean-Main, replace

* With disaggregated EMU
use $cmns1/temp/CPIS-Main.dta, clear
keep if inlist(investor, "USA", "CAN", "GBR", "EMU") | inlist(investor, "AUS", "CHE", "NOR", "SWE", "DNK") ///
    | inlist(investor, $eu1) | inlist(investor, $eu2) | inlist(investor, $eu3)
bys investor issuer asset_class year: gen N = _N
assert N == 1
cap drop N
assert investor != "EMU"
drop *imf_code
gcollapse (sum) position censored, by(investor investor_name issuer issuer_name year asset_class)
drop if investor == "USA"
drop if year < 2007 | year > 2017
drop if investor == "AUS" & year < 2017

* GBR -> LUX censored in 2010; carry over from 2009
replace position = 34386.4315456196 if investor == "GBR" & issuer == "LUX" & asset_class == "Equity (All)" & year == 2010
replace position = 54975.1343018518 if investor == "GBR" & issuer == "LUX" & asset_class == "Debt (All)" & year == 2010
replace censored = -1 if investor == "GBR" & issuer == "LUX" & year == 2010

* AUS 2017: fill in with June values for missing December TH positions - Equity
replace position = 25192.8384 if investor == "AUS" & issuer == "CYM" & asset_class == "Equity (All)" & year == 2017
replace position = 5562.08520 if investor == "AUS" & issuer == "IRL" & asset_class == "Equity (All)" & year == 2017
replace position = 2692.9692 if investor == "AUS" & issuer == "LUX" & asset_class == "Equity (All)" & year == 2017
replace position = 90.7656 if investor == "AUS" & issuer == "PAN" & asset_class == "Equity (All)" & year == 2017

replace censored = -1 if investor == "AUS" & issuer == "CYM" & asset_class == "Equity (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "IRL" & asset_class == "Equity (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "LUX" & asset_class == "Equity (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "PAN" & asset_class == "Equity (All)" & year == 2017

* AUS 2017: fill in with June values for missing December TH positions - Debt
replace position = 6120.5244 if investor == "AUS" & issuer == "CYM" & asset_class == "Debt (All)" & year == 2017
replace position = 614.5908 if investor == "AUS" & issuer == "IRL" & asset_class == "Debt (All)" & year == 2017
replace position = 7790.4576 if investor == "AUS" & issuer == "LUX" & asset_class == "Debt (All)" & year == 2017

replace censored = -1 if investor == "AUS" & issuer == "CYM" & asset_class == "Debt (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "IRL" & asset_class == "Debt (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "LUX" & asset_class == "Debt (All)" & year == 2017

replace position = . if inlist(investor, $eu1) & censored == 1
replace position = . if inlist(investor, $eu2) & censored == 1
replace position = . if inlist(investor, $eu3) & censored == 1
replace position = . if investor == "GBR" & censored == 1
replace position = . if investor == "CHE" & censored == 1

save $cmns1/holdings_master/CPIS-Clean-Main-Disagg-EMU, replace

* ---------------------------------------------------------------------------------------------------
* Long version
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/CPIS-Main.dta, clear
bys investor issuer asset_class year: gen N = _N
assert N == 1
cap drop N
assert investor != "EMU"
drop *imf_code
gcollapse (sum) position censored, by(investor investor_name issuer issuer_name year asset_class)

* GBR -> LUX censored in 2010; carry over from 2009
replace position = 34386.4315456196 if investor == "GBR" & issuer == "LUX" & asset_class == "Equity (All)" & year == 2010
replace position = 54975.1343018518 if investor == "GBR" & issuer == "LUX" & asset_class == "Debt (All)" & year == 2010
replace censored = -1 if investor == "GBR" & issuer == "LUX" & year == 2010

* AUS 2017: fill in with June values for missing December TH positions - Equity
replace position = 25192.8384 if investor == "AUS" & issuer == "CYM" & asset_class == "Equity (All)" & year == 2017
replace position = 5562.08520 if investor == "AUS" & issuer == "IRL" & asset_class == "Equity (All)" & year == 2017
replace position = 2692.9692 if investor == "AUS" & issuer == "LUX" & asset_class == "Equity (All)" & year == 2017
replace position = 90.7656 if investor == "AUS" & issuer == "PAN" & asset_class == "Equity (All)" & year == 2017

replace censored = -1 if investor == "AUS" & issuer == "CYM" & asset_class == "Equity (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "IRL" & asset_class == "Equity (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "LUX" & asset_class == "Equity (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "PAN" & asset_class == "Equity (All)" & year == 2017

* AUS 2017: fill in with June values for missing December TH positions - Debt
replace position = 6120.5244 if investor == "AUS" & issuer == "CYM" & asset_class == "Debt (All)" & year == 2017
replace position = 614.5908 if investor == "AUS" & issuer == "IRL" & asset_class == "Debt (All)" & year == 2017
replace position = 7790.4576 if investor == "AUS" & issuer == "LUX" & asset_class == "Debt (All)" & year == 2017

replace censored = -1 if investor == "AUS" & issuer == "CYM" & asset_class == "Debt (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "IRL" & asset_class == "Debt (All)" & year == 2017
replace censored = -1 if investor == "AUS" & issuer == "LUX" & asset_class == "Debt (All)" & year == 2017

replace position = . if inlist(investor, $eu1) & censored == 1
replace position = . if inlist(investor, $eu2) & censored == 1
replace position = . if inlist(investor, $eu3) & censored == 1
replace position = . if investor == "GBR" & censored == 1
replace position = . if investor == "CHE" & censored == 1

qui drop if inlist(investor, "LBR", "PLW", "")

save $cmns1/holdings_master/CPIS-Raw, replace

log close
