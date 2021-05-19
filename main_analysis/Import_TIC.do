* ---------------------------------------------------------------------------------------------------
* Import_TIC: Imports and cleans data from TIC
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Import_TIC, replace
global ticraw $raw/TIC

* ---------------------------------------------------------------------------------------------------
* Utilities
* ---------------------------------------------------------------------------------------------------

cap program drop process_names
program process_names
    qui replace Countryorregion="China" if Countryorregion=="China, Peoples Republic of" 
    qui replace Countryorregion="China" if Countryorregion=="China, mainland (1)"
    qui replace Countryorregion="China" if Countryorregion=="China, mainland (2)"
    qui replace Countryorregion="China" if Countryorregion=="China, mainland1"
    qui replace Countryorregion="China" if Countryorregion=="China,mainland"
    qui replace Countryorregion="China" if Countryorregion=="China, mainland" 
    qui replace Countryorregion="China" if Countryorregion=="China, P.R." 
    qui replace Countryorregion="China" if Countryorregion=="China, P.R.C" 
    qui replace Countryorregion="China" if Countryorregion=="China, P.R.C." 
    qui replace Countryorregion="China" if Countryorregion=="China, mainland2"
    qui replace Countryorregion="Taiwan" if Countryorregion=="China, Republic of (Taiwan)" 
    qui replace Countryorregion="Taiwan" if Countryorregion==" China, Rep. of (Taiwan)"
    qui replace Countryorregion="Korea" if Countryorregion=="Korea, South"
    qui replace Countryorregion="Curacao" if Countryorregion=="Curacao (2)" 
    qui replace Countryorregion="Guadeloupe" if Countryorregion=="Guadeloupe (3)" 
    qui replace Countryorregion="Hong Kong" if Countryorregion=="Hong Kong, S.A.R." 
    qui replace Countryorregion="Hong Kong" if Countryorregion=="Hong Kong S.A.R." 
    qui replace Countryorregion="Kirabati" if Countryorregion=="Kiribati" 
    qui replace Countryorregion="Serbia" if Countryorregion=="Serbia (2)" 
    qui replace Countryorregion="Serbia" if Countryorregion=="Serbia (4)"
    qui replace Countryorregion="Serbia" if Countryorregion=="Serbia and Montenegro"
    qui replace Countryorregion="Montenegro" if Countryorregion=="Montenegro (2)"
    qui replace Countryorregion="Montenegro" if Countryorregion=="Montenegro (4)"
    qui replace Countryorregion = "Marshall Islands" if Countryorregion == "MarshallIslands"
    qui replace Countryorregion = "Saint Vincent and the Grenadines" if Countryorregion == "Saint Vincent and the Grenadi"
    qui replace Countryorregion = "Trinidad and Tobago" if Countryorregion == "Trinidadand Tobago"
    qui replace Countryorregion = "Turks and Caicos Islands" if Countryorregion == "Turks andCaicosIslands"
    qui replace Countryorregion = "Dominican Republic" if Countryorregion == "DominicanRepublic"
end

cap program drop clean_rows
program clean_rows
    cap drop if strpos(Countryorregion, "Greater than")
    cap drop if strpos(Countryorregion, "Excludes Hong Kong and Macau")
    cap drop if strpos(Countryorregion, "Separate reporting")
    cap drop if strpos(Countryorregion, "Austria, Belgium, Cyprus, Estonia, Finland")
    cap drop if strpos(Countryorregion, "Of which")
    cap drop if strpos(Countryorregion, "Total")
    cap drop if strpos(Countryorregion, "Entries for")
    cap drop if strpos(Countryorregion, "Portugal, Slovakia, Slovenia, Spain")
    cap drop if strpos(Countryorregion, "Bahrain, Iran, Iraq, Kuwait, Oman, Qat")
    cap drop if strpos(Countryorregion, "Algeria, Gabon, Libya, Nigeria")
    cap drop if strpos(Countryorregion, "Austria, Belgium, Cyprus, Estonia")
    cap drop if strpos(Countryorregion, "African oil exporters")
    cap drop if strpos(Countryorregion, "African oil-exporters")
    cap drop if strpos(Countryorregion, "Austria, Belgium, Cyprus, Estonia")
    cap drop if strpos(Countryorregion, "Middle East oil exporters")
    cap drop if strpos(Countryorregion, "oil-exporters")
    cap drop if strpos(Countryorregion, "Market value of")
    cap drop if strpos(Countryorregion, "as of December")
    cap drop if strpos(Countryorregion, "Millions of dollars")
    cap drop if strpos(Countryorregion, "Country or region of issuer")
    cap drop if strpos(Countryorregion, "previous years")
    cap drop if strpos(Countryorregion, "n.a. Not available.")
    cap drop if strpos(Countryorregion, "Country or category")
    cap drop if strpos(Countryorregion, "Stock")
    cap drop if strpos(Countryorregion, "Other")
    cap drop if strpos(Countryorregion, "Austria, Belgium, Finland, France")
    cap drop if strpos(Countryorregion, "includes central")
    cap drop if strpos(Countryorregion, "Debt issued by")
    cap drop if strpos(Countryorregion, "Netherlands, Portugal, Slovakia, Slovenia")
    cap drop if strpos(Countryorregion, "Austria, Belgium, Finland, France")
    cap drop if strpos(Countryorregion, "Country")
    cap drop if strpos(Countryorregion, "Amounts in each row of this table")
    cap drop if strpos(Countryorregion, "Netherlands, Portugal, Slovakia")
    cap drop if strpos(Countryorregion, "For example")
    cap drop if strpos(Countryorregion, "and the country")
    cap drop if strpos(Countryorregion, "first row")
    cap drop if strpos(Countryorregion, "oilexporters")
    qui duplicates drop
end

cap program drop merge_iso
program merge_iso
    qui mmerge Countryorregion using $cmns1/temp/tic_data/tic_xwalk.dta, umatch(country) unmatched(m)
    qui replace iso="CZE" if strpos(Countryorregion, "Czech Republic")
    qui replace iso="ARE" if strpos(Countryorregion, "United Arab Emirates")
    qui replace iso="XSN" if strpos(Countryorregion, "International organizations")
    qui replace iso="ATG" if strpos(Countryorregion, "Antigua and Barbuda")
    qui replace iso="CPV" if strpos(Countryorregion, "Cape Verde")
    qui replace iso="CRI" if strpos(Countryorregion, "Costa Rica")
    qui replace iso="CIV" if strpos(Countryorregion, "Cote D'Ivoire")
    qui replace iso="CUW" if strpos(Countryorregion, "Curacao")
    qui replace iso="DOM" if strpos(Countryorregion, "Dominican Republic")
    qui replace iso="FRO" if strpos(Countryorregion, "Faroe Islands")
    qui replace iso="IMN" if strpos(Countryorregion, "Isle of Man")
    qui replace iso="MAC" if strpos(Countryorregion, "Macau")
    qui replace iso="MHL" if strpos(Countryorregion, "Marshall Islands")
    qui replace iso="PNG" if strpos(Countryorregion, "Papua New Guinea")
    qui replace iso="LCA" if strpos(Countryorregion, "Saint Lucia")
    qui replace iso="SAU" if strpos(Countryorregion, "Saudi Arabia")
    qui replace iso="LKA" if strpos(Countryorregion, "Sri Lanka")
    qui replace iso="TZA" if strpos(Countryorregion, "Tanzania")
    qui replace iso="TCA" if strpos(Countryorregion, "Turks and Caicos Islands")
    qui replace iso="VEN" if strpos(Countryorregion, "Venezuela")
    qui replace iso="VNM" if strpos(Countryorregion, "Vietnam")
    qui replace iso="COK" if strpos(Countryorregion, "Cook Islands")
    qui replace iso="FLK" if strpos(Countryorregion, "Falkland Islands")
    qui replace iso="IOT" if strpos(Countryorregion, "British Indian Ocean Territory")
    qui replace iso="BFA" if strpos(Countryorregion, "Burkina Faso")
    qui replace iso="TTO" if strpos(Countryorregion, "Trinidad and Tobago")
    qui replace iso="SLV" if strpos(Countryorregion, "El Salvador")
    qui replace iso="MMR" if strpos(Countryorregion, "Burma")
    qui replace iso="KNA" if strpos(Countryorregion, "Saint Kitts and Nevis")
    qui replace iso="XSN" if strpos(Countryorregion, "International Organizations")
    qui replace iso="GNQ" if strpos(Countryorregion, "Equatorial Guinea")
    qui replace iso="GUF" if strpos(Countryorregion, "French Guiana")
    qui replace iso="ANT" if strpos(Countryorregion, "Netherlands Antilles")
    qui replace iso="COG" if strpos(Countryorregion, "Congo (Brazzaville)")
    qui replace iso="KIR" if strpos(Countryorregion, "Kirabati")
    qui replace iso="BIH" if strpos(Countryorregion, "Bosnia and Herzegovina")
    qui replace iso="REU" if strpos(Countryorregion, "Reunion")
    qui replace iso="CIV" if strpos(Countryorregion, "Ivoire")
    qui replace iso="NCL" if strpos(Countryorregion, "New Caledonia")
    qui replace iso="MKD" if strpos(Countryorregion, "Macedonia")
    qui replace iso="SUR" if strpos(Countryorregion, "Suriname")
    qui replace iso="VGB" if strpos(Countryorregion, "British Virgin Is.")
    qui replace iso="VCT" if strpos(Countryorregion, "Saint Vincent and the Grenadines")
    qui replace iso="OTH" if strpos(Countryorregion, "unknown")
    cap drop if strpos(Countryorregion, "West Bank & Gaza")
    cap drop if strpos(Countryorregion, "Sint Maarten (2)")
    assert ~missing(iso)
    cap drop _merge
end

* ---------------------------------------------------------------------------------------------------
* Generate crosswalk
* ---------------------------------------------------------------------------------------------------

* Import TIC crosswalk
tempfile xwalk
import excel $ticraw/xwalk_iso.xlsx, sheet("Sheet1") firstrow clear
save "`xwalk'", replace

* Import TIC data: Overall outward portfolios for crosswalk
import delimited $ticraw/Annual/shchistdat_2017update.csv, encoding(ISO-8859-1) clear
replace v2 = "Australia" if v1 == "60089"
drop if _n<4
sxpose, clear
drop if _n==1
replace _var1="year" if _n==1
foreach x of varlist _all {
    local temp=`x'[1]
    if "`temp'"=="" {
        drop `x'
    }
}
foreach x of varlist _all {
    local temp=`x'[1]
    forvalues yy=0/9 {
        local temp=subinstr("`temp'","`yy'","",.)
    }
    local temp=subinstr("`temp'","(","",.)
    local temp=subinstr("`temp'",")","",.)
    local temp=subinstr("`temp'"," ","",.)
    local temp=subinstr("`temp'"," ","",.)
    local temp=subinstr("`temp'","'","",.)
    * Parsing " characters
    local temp=subinstr("`temp'",".","",.)
    local temp=subinstr("`temp'",",","",.)
    local temp=subinstr("`temp'","&","",.)
    local temp=subinstr("`temp'","-","",.)
    local temp=substr("`temp'",1,25)
    rename `x' `temp'
}
foreach x of varlist _all {
    rename `x' ccc_`x'
}
rename ccc_year year
rename ccc_Country type
reshape long ccc_, i(year type) j(country) string
rename ccc_ value
replace country="Luxembourg" if country=="BelgiumandLuxembourg"
replace country="British Virgin Islands" if country=="BritishVirginIslands"
replace country="Cayman Islands" if country=="CaymanIslands"
replace country="Hong Kong, Special Administrative Region of China" if country=="HongKong"
replace country="Korea, Republic of" if country=="KoreaSouth"
replace country="New Zealand" if country=="NewZealand"
replace country="Russian Federation" if country=="RussiaUSSRuntil"
replace country="South Africa" if country=="SouthAfrica"
replace country="Taiwan, Republic of China" if country=="Taiwan"
replace country="United Kingdom" if country=="UnitedKingdom"
mmerge country using "`xwalk'"
keep if _merge==3
drop _merge 
destring year, replace force
drop if year==.
drop if type=="Total"
gen debt=1
replace debt=0 if type=="Equity"
replace value=subinstr(value,",","",.)
destring value, force replace
collapse (sum) value, by(year debt iso country)
gen mns_class="E" if debt==0
replace mns_class="B" if debt==1
drop debt
save $cmns1/temp/tic_agg_outward.dta, replace

* Importing TIC data: Internal crosswalk
use $cmns1/temp/tic_agg_outward.dta, clear
keep country iso
duplicates drop
replace country=trim(country)
replace country="Taiwan" if regexm(country,"Taiwan")==1
replace country="Hong Kong" if regexm(country,"Hong Kong")==1
replace country="Korea" if regexm(country,"Korea")==1
replace country="Russia" if regexm(country,"Russia")==1
save $cmns1/temp/tic_data/tic_xwalk.dta, replace

* ---------------------------------------------------------------------------------------------------
* Disaggregated equity
* ---------------------------------------------------------------------------------------------------

cap program drop process_tic_equity
program process_tic_equity
    rename v1 Countryorregion
    rename v2 Total
    rename v3 Common
    rename v4 Funds
    rename v5 Preferred_Other
    gen Censored = 0
    qui drop if Common=="stock"
    foreach var in "Total" "Common" "Funds" "Preferred_Other" {
        qui replace `var' = "0" if `var' == "*"
        qui replace `var' = "0" if `var' == "* "
        qui replace `var' = "0" if `var' == "*  "
    }
    qui drop if missing(Countryorregion)
    clean_rows    
    qui destring Total Common Preferred_Other Funds, replace
    foreach var in "Total" "Common" "Funds" "Preferred_Other" {
        capture confirm numeric variable `var'
        if _rc == 7 error 666
    }
    process_names
    merge_iso
    keep Countryorregion Total Common Funds Preferred_Other iso Censored
end

* 2017
import delimited $raw/TIC/Annual/shca2017_appendix/shc_app12_2017.csv, delimit(",") rowrange(5:) ///
    encoding(ISO-8859-1) clear
process_tic_equity
gen year = 2017
save $cmns1/temp/tic_disaggregated/equity/tic_equity_2017, replace

* 2016
import delimited $raw/TIC/Annual/shc2016_appendix/shc_app12_2016.csv, delimit(",") rowrange(5:) ///
    encoding(ISO-8859-1) clear
process_tic_equity
gen year = 2016
save $cmns1/temp/tic_disaggregated/equity/tic_equity_2016, replace

* 2015
import delimited $raw/TIC/Annual/shca2015_appendix_and_exhibits/shc15_app12.csv, ///
    delimit(",") rowrange(5:) ///
    encoding(ISO-8859-1) clear
process_tic_equity
gen year = 2015
save $cmns1/temp/tic_disaggregated/equity/tic_equity_2015, replace

* 2014
import delimited $raw/TIC/Annual/shc2014_appendix/shc14_app12.csv, ///
    delimit(",") rowrange(5:) ///
    encoding(ISO-8859-1) clear
process_tic_equity
gen year = 2014
save $cmns1/temp/tic_disaggregated/equity/tic_equity_2014, replace

* 2013
import delimited $raw/TIC/Annual/shc2013_appendix/appendix_tab12.csv, ///
    delimit(",") rowrange(5:) ///
    encoding(ISO-8859-1) clear
process_tic_equity
gen year = 2013
save $cmns1/temp/tic_disaggregated/equity/tic_equity_2013, replace

* 2012
import excel using $raw/TIC/Annual/TIC2012.xlsx, clear sheet("A12")
rename (A B C D E) (v1 v2 v3 v4 v5)
process_tic_equity
gen year = 2012
save $cmns1/temp/tic_disaggregated/equity/tic_equity_2012, replace

* 2011
import excel using $raw/TIC/Annual/TIC2011.xlsx, clear sheet("A12")
rename (A B C D E) (v1 v2 v3 v4 v5)
process_tic_equity
gen year = 2011
save $cmns1/temp/tic_disaggregated/equity/tic_equity_2011, replace

* 2010
import excel using $raw/TIC/Annual/TIC2010.xlsx, clear sheet("A12")
rename (A B C D E) (v1 v2 v3 v4 v5)
process_tic_equity
gen year = 2010
save $cmns1/temp/tic_disaggregated/equity/tic_equity_2010, replace

* 2009
import excel using $raw/TIC/Annual/TIC2009.xlsx, clear sheet("A12")
rename (A B C D E) (v1 v2 v3 v4 v5)
process_tic_equity
gen year = 2009
save $cmns1/temp/tic_disaggregated/equity/tic_equity_2009, replace

* 2008
import excel using $raw/TIC/Annual/TIC2008.xlsx, clear sheet("TB30")
rename (A B C D E) (v1 v2 v3 v4 v5)
process_tic_equity
gen year = 2008
save $cmns1/temp/tic_disaggregated/equity/tic_equity_2008, replace

* 2007
import excel using $raw/TIC/Annual/TIC2007.xlsx, clear sheet("Table 30")
rename B v1
rename C v2 
rename D v3
rename E v4
rename F v5
process_tic_equity
gen year = 2007
save $cmns1/temp/tic_disaggregated/equity/tic_equity_2007, replace

* 2006
import excel using $raw/TIC/Annual/Data_Entry/shc2006r.xlsx, clear sheet("27") firstrow
qui gen Preferred_Other = Preferred + Other
qui tostring Total Common Preferred_Other Funds, replace
rename Countryor v1 
rename Total v2
rename Common v3
rename Funds v4
rename Preferred_Other v5
process_tic_equity
gen year = 2006
save $cmns1/temp/tic_disaggregated/equity/tic_equity_2006, replace

* Put it together
clear
forval year = 2006/2017 {
    append using $cmns1/temp/tic_disaggregated/equity/tic_equity_`year'
}
save $cmns1/temp/tic_disaggregated/equity/tic_equity, replace

* ---------------------------------------------------------------------------------------------------
* Private debt
* ---------------------------------------------------------------------------------------------------

cap program drop process_tic_private_debt
    program process_tic_private_debt
    rename v1 Countryorregion
    rename v2 Total
    rename v3 GovernmentLT
    rename v4 PrivateLT
    rename v5 GovernmentST
    rename v6 PrivateST
    gen Censored = 0
    qui tostring Total GovernmentLT PrivateLT GovernmentST PrivateST, replace
    foreach var in "Total" "GovernmentLT" "PrivateLT" "GovernmentST" "PrivateST" {
        qui replace `var' = "0" if `var' == "*"
        qui replace `var' = "0" if `var' == "* "
        qui replace `var' = "0" if `var' == "*  "
    }
    qui drop if missing(Countryorregion)
    clean_rows
    bysort Countryorregion: gen n = _n
    qui drop if Countryorregion == "Canada" & n == 2
    qui destring Total GovernmentLT PrivateLT GovernmentST PrivateST, replace
    foreach var in "Total" "GovernmentLT" "PrivateLT" "GovernmentST" "PrivateST" {
        capture confirm numeric variable `var'
        if _rc == 7 error 666
    }
    process_names
    merge_iso
    qui gen Private = PrivateST + PrivateLT
    keep Countryorregion GovernmentLT PrivateLT PrivateST GovernmentST Private iso Censored
end

* 2017
import delimited "$raw/TIC/Annual/shca2017_appendix/shc_app10_2017.csv", delimit(",") ///
    rowrange(6:) encoding(ISO-8859-1)clear
process_tic_private_debt
gen year = 2017
save $cmns1/temp/tic_disaggregated/private_debt/tic_private_debt_2017, replace

* 2016
import delimited "$raw/TIC/Annual/shc2016_appendix/shc_app10_2016.csv", delimit(",") ///
    rowrange(6:) encoding(ISO-8859-1)clear
process_tic_private_debt
gen year = 2016
save $cmns1/temp/tic_disaggregated/private_debt/tic_private_debt_2016, replace

* 2015
import delimited "$raw/TIC/Annual/shca2015_appendix_and_exhibits/shc15_app10.csv", delimit(",") ///
    rowrange(6:) encoding(ISO-8859-1)clear
process_tic_private_debt
gen year = 2015
save $cmns1/temp/tic_disaggregated/private_debt/tic_private_debt_2015, replace

* 2014
import delimited "$raw/TIC/Annual/shc2014_appendix/shc14_app10.csv", delimit(",") ///
    encoding(ISO-8859-1) clear
cap rename ïcountryorregionofissuer v1
cap rename countryorregionofissuer v1
rename total v2
rename longtermgovernment v3
rename longtermprivate v4
rename shorttermgovernment v5
rename shorttermprivate v6
process_tic_private_debt
gen year = 2014
save $cmns1/temp/tic_disaggregated/private_debt/tic_private_debt_2014, replace

* 2013
import delimited "$raw/TIC/Annual/shc2013_appendix/appendix_tab10.csv",delimit(",") ///
    rowrange(6:) encoding(ISO-8859-1)clear
process_tic_private_debt
gen year = 2013
save $cmns1/temp/tic_disaggregated/private_debt/tic_private_debt_2013, replace

* 2009-2012
forval year = 2009/2012 {

    import excel using $raw/TIC/Annual/TIC`year'.xlsx, clear sheet("A10")
    rename (A B C D E F) (v1 v2 v3 v4 v5 v6)
    process_tic_private_debt
    gen year = `year'
    save $cmns1/temp/tic_disaggregated/private_debt/tic_private_debt_`year', replace
    
}

* 2008
import excel using $raw/TIC/Annual/TIC2008.xlsx, clear sheet("TB28")
rename (A B C D E F) (v1 v2 v3 v4 v5 v6)
process_tic_private_debt
gen year = 2008
save $cmns1/temp/tic_disaggregated/private_debt/tic_private_debt_2008, replace

* 2007
import excel using $raw/TIC/Annual/TIC2007.xlsx, clear sheet("Table 28")
drop A
rename (B C D E F G) (v1 v2 v3 v4 v5 v6)
process_tic_private_debt
gen year = 2007
save $cmns1/temp/tic_disaggregated/private_debt/tic_private_debt_2007, replace

* 2006
import excel using $raw/TIC/Annual/Data_Entry/shc2006r.xlsx, clear sheet("25")
rename (A B C D E F) (v1 v2 v3 v4 v5 v6)
process_tic_private_debt
gen year = 2006
save $cmns1/temp/tic_disaggregated/private_debt/tic_private_debt_2006, replace

* 2005
import excel using $raw/TIC/Annual/Data_Entry/shc2005r.xlsx, clear sheet("25")
rename (A B C D E F) (v1 v2 v3 v4 v5 v6)
process_tic_private_debt
gen year = 2005
save $cmns1/temp/tic_disaggregated/private_debt/tic_private_debt_2005, replace

* Put it together
clear
forval year = 2006/2017 {
    append using $cmns1/temp/tic_disaggregated/private_debt/tic_private_debt_`year'
}
save $cmns1/temp/tic_disaggregated/private_debt/tic_private_debt, replace

* ---------------------------------------------------------------------------------------------------
* ABS
* ---------------------------------------------------------------------------------------------------

cap program drop process_tic_abs
program process_tic_abs
    rename v1 Countryorregion 
    rename v2 Total 
    rename v3 Straight
    rename v4 ZeroCoupon
    rename v5 Convertible
    rename v6 ABS
    gen Censored = 0
    qui tostring Total Straight ZeroCoupon Convertible ABS, replace
    foreach var in "Total" "Straight" "ZeroCoupon" "Convertible" "ABS" {
        qui replace `var' = "0" if `var' == "*"
        qui replace `var' = "0" if `var' == "* "
        qui replace `var' = "0" if `var' == "*  "
    }
    qui drop if missing(Countryorregion)
    clean_rows
    bysort Countryorregion: gen n = _n
    qui drop if Countryorregion == "Canada" & n == 2
    qui destring Total Straight ZeroCoupon Convertible ABS, replace
    foreach var in "Total" "Straight" "ZeroCoupon" "Convertible" "ABS" {
        capture confirm numeric variable `var'
        if _rc == 7 error 666
    }
    process_names
    merge_iso
    keep Countryorregion Straight ZeroCoupon Convertible ABS iso Censored
end

* 2017
import delimited "$raw/TIC/Annual/shca2017_appendix/shc_app08_2017.csv", delimit(",") /// 
    rowrange(5:) encoding(ISO-8859-1)clear
process_tic_abs
gen year = 2017
save $cmns1/temp/tic_disaggregated/abs/tic_abs_2017, replace

* 2016
import delimited "$raw/TIC/Annual/shc2016_appendix/shc_app08_2016.csv", delimit(",") ///
    rowrange(5:) encoding(ISO-8859-1)clear
process_tic_abs
gen year = 2016
save $cmns1/temp/tic_disaggregated/abs/tic_abs_2016, replace

* 2015
import delimited "$raw/TIC/Annual/shca2015_appendix_and_exhibits/shc15_app08.csv", delimit(",") ///
    rowrange(5:) encoding(ISO-8859-1)clear
process_tic_abs
gen year = 2015
save $cmns1/temp/tic_disaggregated/abs/tic_abs_2015, replace

* 2014
import delimited "$raw/TIC/Annual/shc2014_appendix/shc14_app08.csv", delimit(",") encoding(ISO-8859-1) clear
process_tic_abs
gen year = 2014
save $cmns1/temp/tic_disaggregated/abs/tic_abs_2014, replace

* 2013
import delimited "$raw/TIC/Annual/shc2013_appendix/appendix_tab08.csv", delimit(",") ///
    rowrange(5:) encoding(ISO-8859-1)clear
process_tic_abs
gen year = 2013
save $cmns1/temp/tic_disaggregated/abs/tic_abs_2013, replace

* 2009-2012
forval year = 2009/2012 {
    import excel using $raw/TIC/Annual/TIC`year'.xlsx, clear sheet("A8")
    rename (A B C D E F) (v1 v2 v3 v4 v5 v6)
    process_tic_abs
    gen year = `year'
    save $cmns1/temp/tic_disaggregated/abs/tic_abs_`year', replace
}

* 2008
import excel using $raw/TIC/Annual/TIC2008.xlsx, clear sheet("TB26")
rename (A B C D E F) (v1 v2 v3 v4 v5 v6)
process_tic_abs
gen year = 2008
save $cmns1/temp/tic_disaggregated/abs/tic_abs_2008, replace

* 2007
import excel using $raw/TIC/Annual/TIC2007.xlsx, clear sheet("Table 26")
drop A
rename (B C D E F G) (v1 v2 v3 v4 v5 v6)
process_tic_abs
gen year = 2007
save $cmns1/temp/tic_disaggregated/abs/tic_abs_2007, replace

* 2006
import excel using $raw/TIC/Annual/Data_Entry/shc2006r.xlsx, clear sheet("23")
rename (A B C D E F) (v1 v2 v3 v4 v5 v6)
process_tic_abs
gen year = 2006
save $cmns1/temp/tic_disaggregated/abs/tic_abs_2006, replace

* 2005
import excel using $raw/TIC/Annual/Data_Entry/shc2005r.xlsx, clear sheet("23")
rename (A B C D E F) (v1 v2 v3 v4 v5 v6)
process_tic_abs
gen year = 2005
save $cmns1/temp/tic_disaggregated/abs/tic_abs_2005, replace

* Put it together
clear
forval year = 2006/2017 {
    append using $cmns1/temp/tic_disaggregated/abs/tic_abs_`year'
}
save $cmns1/temp/tic_disaggregated/abs/tic_abs, replace

* ---------------------------------------------------------------------------------------------------
* Merge the data
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/tic_disaggregated/equity/tic_equity, clear
rename Countryorregion issuer_name
rename Total total_equity
rename Common common_equity
rename Funds fund_shares
rename Preferred_Other preferred_other_equity
rename Censored censored_equity
order iso year
qui mmerge iso year using $cmns1/temp/tic_disaggregated/private_debt/tic_private_debt, unmatched(b)
replace issuer_name = Country if missing(issuer_name)
drop Country
drop _merge
rename GovernmentLT government_debt_lt
rename GovernmentST government_debt_st
rename PrivateLT private_debt_lt
rename PrivateST private_debt_st
rename Private private_debt
rename Censored censored_debt
qui mmerge iso year using $cmns1/temp/tic_disaggregated/abs/tic_abs, unmatched(b)
replace issuer_name = Country if missing(issuer_name)
drop Country
qui drop _merge
rename Straight debt_straight
rename Zero debt_zero_coupon
rename Convertible debt_convertible
rename ABS abs
rename Censored censored_abs
order censored*, last
gen corporate_debt = private_debt - abs
replace corporate_debt = private_debt if missing(abs) & ~missing(private_debt)
save $cmns1/temp/tic_disaggregated/tic_disaggregated, replace
save $cmns1/holdings_master/TIC-Disaggregated-Clean-Main, replace

cap program drop process_tic_currency
program process_tic_currency
    qui tostring Total_All Total_Gov USD_Gov LC_Gov Total_Private USD_Private LC_Private, replace
    foreach var in "Total_All" "Total_Gov" "USD_Gov" "LC_Gov" "Total_Private" "USD_Private" "LC_Private" {
        qui replace `var' = "0" if `var' == "*"
        qui replace `var' = "0" if `var' == "* "
        qui replace `var' = "0" if `var' == "*  "
    }
    qui drop if missing(Countryorregion)
    clean_rows
    bysort Countryorregion: gen n = _n
    assert n == 1
    drop n
    qui destring Total_All Total_Gov USD_Gov LC_Gov Total_Private USD_Private LC_Private, replace
    foreach var in "Total_All" "Total_Gov" "USD_Gov" "LC_Gov" "Total_Private" "USD_Private" "LC_Private" {
        capture confirm numeric variable `var'
        if _rc == 7 error 666
    }
    process_names
    merge_iso
    assert ~missing(iso)
    keep Countryorregion Total_All Total_Gov USD_Gov LC_Gov Total_Private USD_Private LC_Private iso
    order Countryorregion iso Total_All Total_Gov USD_Gov LC_Gov Total_Private USD_Private LC_Private
end

* 2017
qui import delimited $raw/TIC/Annual/shca2017_appendix/shc_app11_2017.csv, delimit(",") /// 
    rowrange(12:) encoding(ISO-8859-1)clear
rename v1 Countryorregion
rename v2 Total_All
rename v3 Total_Gov
rename v4 USD_Gov
rename v5 LC_Gov
rename v6 Total_Private
rename v7 USD_Private
rename v8 LC_Private
process_tic_currency
gen year = 2017
qui save $cmns1/temp/tic_disaggregated/currency/tic_currency_2017, replace

* 2016
qui import delimited $raw/TIC/Annual/shc2016_appendix/shc_app11_2016.csv, delimit(",") ///
    rowrange(12:) encoding(ISO-8859-1)clear
rename v1 Countryorregion
rename v2 Total_All
rename v3 Total_Gov
rename v4 USD_Gov
rename v5 LC_Gov
rename v6 Total_Private
rename v7 USD_Private
rename v8 LC_Private
process_tic_currency
gen year = 2016
qui save $cmns1/temp/tic_disaggregated/currency/tic_currency_2016, replace

* 2015
qui import delimited $raw/TIC/Annual/shca2015_appendix_and_exhibits/shc15_app11.csv, delimit(",") ///
    rowrange(12:) encoding(ISO-8859-1)clear
rename v1 Countryorregion
rename v2 Total_All
rename v3 Total_Gov
rename v4 USD_Gov
rename v5 LC_Gov
rename v6 Total_Private
rename v7 USD_Private
rename v8 LC_Private
process_tic_currency
gen year = 2015
qui save $cmns1/temp/tic_disaggregated/currency/tic_currency_2015, replace

* 2014
qui import excel using $raw/TIC/Annual/shc2014_appendix/shc14_app11_fixed.xlsx, clear
rename A Countryorregion
rename B Total_All
rename C Total_Gov
rename D USD_Gov
rename E LC_Gov
rename F Total_Private
rename G USD_Private
rename H LC_Private
qui replace Total_All = "2" if strpos(Countryorregion, "Saint Vincent and the Grenadi")
process_tic_currency
gen year = 2014
qui save $cmns1/temp/tic_disaggregated/currency/tic_currency_2014, replace

* 2013
qui import delimited $raw/TIC/Annual/shc2013_appendix/appendix_tab11.csv, delimit(",") ///
    rowrange(12:) encoding(ISO-8859-1) clear
rename v1 Countryorregion
rename v2 Total_All
rename v3 Total_Gov
rename v4 USD_Gov
rename v5 LC_Gov
rename v6 Total_Private
rename v7 USD_Private
rename v8 LC_Private
process_tic_currency
gen year = 2013
qui save $cmns1/temp/tic_disaggregated/currency/tic_currency_2013, replace

forval year = 2009/2012 {
    import excel using $raw/TIC/Annual/TIC`year'.xlsx, clear sheet("A11") cellrange(A7)
    rename (A B C D E F G H) (v1 v2 v3 v4 v5 v6 v7 v8)
    foreach x in "I" "J" "K" "L" "M" "N" "O" "P" "Q" {
        cap drop `x'
    }
    rename v1 Countryorregion
    rename v2 Total_All
    rename v3 Total_Gov
    rename v4 USD_Gov
    rename v5 LC_Gov
    rename v6 Total_Private
    rename v7 USD_Private
    rename v8 LC_Private
    process_tic_currency
    gen year = `year'
    qui save $cmns1/temp/tic_disaggregated/currency/tic_currency_`year', replace   
}

* 2008
qui import excel using $raw/TIC/Annual/TIC2008.xlsx, clear sheet("TB29") cellrange(A7)
rename (A B C D E F G H) (v1 v2 v3 v4 v5 v6 v7 v8)
foreach x in "I" "J" "K" "L" "M" "N" "O" "P" "Q" {
    cap drop `x'
}
rename v1 Countryorregion
rename v2 Total_All
rename v3 Total_Gov
rename v4 USD_Gov
rename v5 LC_Gov
rename v6 Total_Private
rename v7 USD_Private
rename v8 LC_Private
process_tic_currency
gen year = 2008
qui save $cmns1/temp/tic_disaggregated/currency/tic_currency_2008, replace

* 2007
import excel using $raw/TIC/Annual/TIC2007.xlsx, clear sheet("Table 29") cellrange(A6)
rename (A B C D E F G H) (v1 v2 v3 v4 v5 v6 v7 v8)
foreach x in "I" "J" "K" "L" "M" "N" "O" "P" "Q" {
    cap drop `x'
}
rename v1 Countryorregion
rename v2 Total_All
rename v3 Total_Gov
rename v4 USD_Gov
rename v5 LC_Gov
rename v6 Total_Private
rename v7 USD_Private
rename v8 LC_Private
process_tic_currency
gen year = 2007
qui save $cmns1/temp/tic_disaggregated/currency/tic_currency_2007, replace

* Put it together
clear
forval year = 2007/2017 {
    append using $cmns1/temp/tic_disaggregated/currency/tic_currency_`year'
}
rename Countryorregion Country_Name
rename iso Country_ISO
rename year Year
order Year Country_Name Country_ISO
gsort -Year Country_Name
foreach var of varlist * {
    assert ~missing(`var')
}
save $cmns1/holdings_master/TIC-Debt-Currency-Bilaterals.dta, replace

log close
