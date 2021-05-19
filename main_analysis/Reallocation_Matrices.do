* ---------------------------------------------------------------------------------------------------
* Reallocation_Matrices: Produces reallocation matrices from Morningstar holdings data
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Reallocation_Matrices, replace

* ---------------------------------------------------------------------------------------------------
* Construct reallocation shares
* ---------------------------------------------------------------------------------------------------

* Sumstats for reallocation shares
use $cmns1/holdings_master/mns_issuer_summary, clear
collapse (sum) marketvalue_usd, by(DomicileCountryId asset_class cgs_domicile country_bg year)
save $scratch/sumstats_for_reallocation_shares_disagg, replace

* Class B: all bonds
use $scratch/sumstats_for_reallocation_shares_disagg, clear
keep if inlist(DomicileCountryId, "USA", "EMU", "GBR", "CAN", "CHE", "AUS") | inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL")
keep if strpos(asset_class, "Bond")
collapse (sum) marketvalue_usd, by(DomicileCountryId cgs_domicile country_bg year)
drop if country_bg == ""
bysort DomicileCountryId cgs_domicile year: egen tot_residency = total(marketvalue_usd)
gen reallocation_share = marketvalue_usd / tot_residency
save $scratch/reallocation_shares_disagg_B, replace

* Class E: equities
use $scratch/sumstats_for_reallocation_shares_disagg, clear
keep if inlist(DomicileCountryId, "USA", "EMU", "GBR", "CAN", "CHE", "AUS") | inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL")
keep if asset_class == "Equity"
collapse (sum) marketvalue_usd, by(DomicileCountryId cgs_domicile country_bg year)
drop if country_bg == ""
bysort DomicileCountryId cgs_domicile year: egen tot_residency = total(marketvalue_usd)
gen reallocation_share = marketvalue_usd / tot_residency
save $scratch/reallocation_shares_disagg_E, replace

* Class BC: corporate bonds
use $scratch/sumstats_for_reallocation_shares_disagg, clear
keep if inlist(DomicileCountryId, "USA", "EMU", "GBR", "CAN", "CHE", "AUS") | inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL")
keep if asset_class == "Bonds - Corporate"
collapse (sum) marketvalue_usd, by(DomicileCountryId cgs_domicile country_bg year)
drop if country_bg == ""
bysort DomicileCountryId cgs_domicile year: egen tot_residency = total(marketvalue_usd)
gen reallocation_share = marketvalue_usd / tot_residency
save $scratch/reallocation_shares_disagg_BC, replace

* Class BSALS: government bonds
use $scratch/sumstats_for_reallocation_shares_disagg, clear
keep if inlist(DomicileCountryId, "USA", "EMU", "GBR", "CAN", "CHE", "AUS") | inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL")
keep if asset_class == "Bonds - Government"
collapse (sum) marketvalue_usd, by(DomicileCountryId cgs_domicile country_bg year)
drop if country_bg == ""
bysort DomicileCountryId cgs_domicile year: egen tot_residency = total(marketvalue_usd)
gen reallocation_share = marketvalue_usd / tot_residency
save $scratch/reallocation_shares_disagg_BSALS, replace

* Class BSF: structured finance
use $scratch/sumstats_for_reallocation_shares_disagg, clear
keep if inlist(DomicileCountryId, "USA", "EMU", "GBR", "CAN", "CHE", "AUS") | inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL")
keep if asset_class == "Bonds - Structured Finance"
collapse (sum) marketvalue_usd, by(DomicileCountryId cgs_domicile country_bg year)
drop if country_bg == ""
bysort DomicileCountryId cgs_domicile year: egen tot_residency = total(marketvalue_usd)
gen reallocation_share = marketvalue_usd / tot_residency
save $scratch/reallocation_shares_disagg_BSF, replace

* Append all the reallocation shares
clear
gen mns_class = ""
foreach _class in "B" "E" "BC" "BSALS" "BSF" {
    append using $scratch/reallocation_shares_disagg_`_class'
    replace mns_class = "`_class'" if missing(mns_class)
}
keep DomicileCountryId mns_class year cgs_domicile country_bg reallocation_share
save $scratch/reallocation_shares_disagg, replace

* ---------------------------------------------------------------------------------------------------
* Construct the matrices
* ---------------------------------------------------------------------------------------------------

* Data for all classes save all bonds
use $cmns1/holdings_master/mns_security_summary, clear
keep if inlist(DomicileCountryId, "USA", "EMU", "GBR", "CAN", "CHE", "AUS") | ///
    inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL")
replace asset_class = "E" if asset_class == "Equity"
replace asset_class = "BC" if asset_class == "Bonds - Corporate"
replace asset_class = "BSALS" if asset_class == "Bonds - Government"
replace asset_class = "BSF" if asset_class == "Bonds - Structured Finance"
drop if missing(asset_class)
collapse (sum) marketvalue_usd, by(DomicileCountryId cgs_domicile asset_class)
rename marketvalue_usd tot_residency_portfolio
save $scratch/mns_residency_portfolios_tmp1, replace

* Data for all bonds
use $cmns1/holdings_master/mns_security_summary, clear
keep if inlist(DomicileCountryId, "USA", "EMU", "GBR", "CAN", "CHE", "AUS") | ///
    inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL")
replace asset_class = "B" if strpos(asset_class, "Bond")
drop if missing(asset_class)
collapse (sum) marketvalue_usd, by(DomicileCountryId cgs_domicile asset_class)
rename marketvalue_usd tot_residency_portfolio
save $scratch/mns_residency_portfolios_tmp2, replace

* Append the above
use $scratch/mns_residency_portfolios_tmp1, clear
append using $scratch/mns_residency_portfolios_tmp2
drop if missing(cgs_domicile)
rename asset_class mns_class
save $scratch/mns_residency_portfolios, replace

* Reallocation matrices
use $cmns1/holdings_master/mns_security_summary, clear
collapse (sum) marketvalue_usd, by(cgs_domicile country_bg asset_class DomicileCountryId year)
rename year Year
rename asset_class Asset_Class
rename cgs_domicile Issuer_Residency
rename DomicileCountryId Investor
rename country_bg Issuer_Nationality
bys Investor Year Asset_Class Issuer_Residency: egen totVal = total(marketvalue_usd)
gen Reallocation_Share = marketvalue_usd / totVal
drop marketvalue_usd totVal
save $cmns1/temp/reallocation_matrices, replace
save $cmns1/holdings_master/Reallocation-Matrices, replace

* Export to full matrix format
cap restore
forval year=2007/2017 {

    di "Processing year `year'"
    
    * Prepare the data
    use $scratch/reallocation_shares_disagg, clear
    qui keep if inlist(DomicileCountryId, "USA", "EMU", "GBR", "CAN", "CHE", "AUS") | inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL")
    qui drop if missing(cgs_domicile)
    qui drop if cgs_domicile == "XSN"
    qui keep if year == `year'
    qui gen keeper = 0
    qui replace keeper = 1 if inlist(mns_class, "E", "B", "B_TIC")
    qui replace keeper = 1 if inlist(mns_class, "BC", "BSALS", "BSF")
    qui drop if keeper == 0
    qui gen virtual_mns_class = mns_class
    qui replace virtual_mns_class = "B" if virtual_mns_class == "B_TIC"
    qui mmerge DomicileCountryId cgs_domicile virtual_mns_class using $scratch/mns_residency_portfolios, ///
        unmatched(m) umatch(DomicileCountryId cgs_domicile mns_class)
    qui gen th_iso = 0
    qui replace th_iso = 1 if inlist(cgs_domicile, $tax_haven_1)
    qui replace th_iso = 1 if inlist(cgs_domicile, $tax_haven_2)
    qui replace th_iso = 1 if inlist(cgs_domicile, $tax_haven_3)
    qui replace th_iso = 1 if inlist(cgs_domicile, $tax_haven_4)
    qui replace th_iso = 1 if inlist(cgs_domicile, $tax_haven_5)
    qui replace th_iso = 1 if inlist(cgs_domicile, $tax_haven_6)
    qui replace th_iso = 1 if inlist(cgs_domicile, $tax_haven_7)
    qui replace th_iso = 1 if inlist(cgs_domicile, $tax_haven_8)
    qui drop if tot_residency_portfolio < 1e8 & th_iso == 0
    drop virtual_mns_class

    * Exclude XSN from BC, BSALS
    qui drop if country_bg == "XSN" & ~inlist(mns_class, "B", "B_TIC")
    bys DomicileCountryId mns_class cgs_domicile: egen totShare = total(reallocation_share)
    qui replace reallocation_share = reallocation_share / totShare
    drop totShare
    cap drop _merge

    * UAE issue
    qui replace cgs_domicile = "ARE" if cgs_domicile == "UAE"
    qui replace country_bg = "ARE" if country_bg == "UAE"
    collapse (sum) reallocation_share, by(DomicileCountryId mns_class cgs_domicile country_bg year)

    * Beta
    qui drop if mns_class == "B_TIC"
    qui drop if mns_class == "BSF" & DomicileCountryId != "USA"
    qui drop if mns_class == "BSALS" & DomicileCountryId != "USA"
    qui drop if DomicileCountryId == "NZL"
    qui drop if mns_class == "B" & DomicileCountryId == "USA"

    * Sanity check
    bys DomicileCountryId mns_class cgs_domicile: egen totShare = total(reallocation_share)
    qui sum totShare, detail
    drop totShare
    qui save $scratch/allocation_matrices_web_full, replace

    * Export matrices
    use $scratch/allocation_matrices_web_full, clear
    qui levelsof DomicileCountryId, local(countries)
    qui levelsof mns_class, local(classes)
    foreach country of local countries {
        foreach class of local classes {
            if ~("`class'" == "B_TIC" & "`country'" != "USA") {

                use $scratch/allocation_matrices_web_full, clear

                if "`class'" == "B" {
                    local _lab = "All_Bonds"
                }
                if "`class'" == "E" {
                    local _lab = "Equities"
                }
                if "`class'" == "BC" {
                    local _lab = "Corporate_Bonds"
                }
                if "`class'" == "BSALS" {
                    local _lab = "Sovereign_Agency_Muni_Bonds"
                }
                if "`class'" == "BSF" {
                    local _lab = "Asset_Backed_Securities"
                }
                if "`class'" == "B_TIC" {
                    local _lab = "All_Bonds_TIC_Weights"
                }

                qui keep if DomicileCountryId == "`country'"
                qui keep if mns_class == "`class'"
                qui count
                if `r(N)' > 0 {

                qui levelsof country_bg, local(destinations)
                qui levelsof cgs_domicile, local(origins)
                foreach destination of local destinations {
                    local new = _N + 1
                    qui set obs `new'
                    qui replace mns_class = "`class'" if _n == _N
                    qui replace DomicileCountryId = "`country'" if _n == _N
                    qui replace year = `year' if _n == _N
                    qui replace cgs_domicile = "`destination'" if _n == _N
                    qui replace country_bg = "`destination'" if _n == _N
                    qui replace reallocation_share = 0 if _n == _N
                }
                foreach origin of local origins {
                    local new = _N + 1
                    qui set obs `new'
                    qui replace mns_class = "`class'" if _n == _N
                    qui replace DomicileCountryId = "`country'" if _n == _N
                    qui replace year = `year' if _n == _N
                    qui replace cgs_domicile = "`origin'" if _n == _N
                    qui replace country_bg = "`origin'" if _n == _N
                    qui replace reallocation_share = 0 if _n == _N
                }
                collapse (sum) reallocation_share, by(DomicileCountryId year mns_class cgs_domicile country_bg)
                bys DomicileCountryId year mns_class cgs_domicile: egen totShare = total(reallocation_share)
                qui replace reallocation_share = 1 if totShare == 0
                drop totShare

                qui count
                if `r(N)' > 0 {
                    qui reshape wide reallocation_share, i(mns_class DomicileCountryId cgs_domicile year) j(country_bg) string
                    foreach destination of local destinations {
                        qui replace reallocation_share`destination' = 0 if missing(reallocation_share`destination')
                        qui replace reallocation_share`destination' = reallocation_share`destination' * 100
                    }
                    format %8.1f reallocation_share*
                    foreach var of varlist reallocation_share* {
                        qui replace `var' = 0.0 if missing(`var')
                    }
                    rename reallocation_share* *
                    rename cgs_domicile Immediate_Destination
                    drop mns_class DomicileCountryId year
                    cap drop keeper
                    
                    foreach var of varlist * {
                        label var `var' ""
                    }
                    
                    qui export excel using $scratch/Reallocation_Matrix_`country'_`_lab'_`year', ///
                        sheet("Reallocation Matrix") firstrow(variables) cell(A1) replace
                    rename Immediate_Destination Residency
                    qui save $cmns1/reallocation_matrices/Reallocation_Matrix_`country'_`_lab'_`year', replace
                }
                }

            }
        }
    }
}

* Exhibit for paper
use $cmns1/temp/reallocation_matrices, clear
keep if Year == 2017
keep if Investor == "USA"
keep if Asset_Class == "Bonds - Corporate"
keep if inlist(Issuer_Residency, "BMU", "BRA", "CAN", "CHN", "CYM") | ///
    inlist(Issuer_Residency, "DEU", "GBR", "HKG", "IND", "IRL") | ///
    inlist(Issuer_Residency, "JPN", "LUX", "PAN", "RUS", "USA")
replace Issuer_Nationality = "RoW" if ~(inlist(Issuer_Nationality, "BMU", "BRA", "CAN", "CHN", "CYM") | ///
    inlist(Issuer_Nationality, "DEU", "GBR", "HKG", "IND", "IRL") | ///
    inlist(Issuer_Nationality, "JPN", "LUX", "PAN", "RUS", "USA"))
keep Issuer_Residency Issuer_Nationality Reallocation_Share
collapse (sum) Reallocation_Share, by(Issuer_Residency Issuer_Nationality)
replace Reallocation_Share = Reallocation_Share * 100
replace Reallocation_Share = . if Reallocation_Share < 0.05
reshape wide Reallocation_Share, i(Issuer_Residency) j(Issuer_Nationality) string
rename Reallocation_Share* *
sort Issuer_Residency
order Issuer_Residency BMU  BRA CAN CHN CYM DEU GBR HKG IND IRL JPN LUX PAN RUS USA RoW
export excel using $cmns1/tables/reallocation_matrix_example_usa_bc, ///
    replace firstrow(variables)

* ---------------------------------------------------------------------------------------------------
* Example of time variation in reallocation matrices: Petrobras Global Finance
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_master/Reallocation-Matrices, clear
keep if Investor == "USA"
keep if Asset_Class == "Bonds - Corporate"
keep if Issuer_Nationality == "BRA"
qui reshape wide Reallocation_Share, i(Investor Asset_Class Issuer_Residency Issuer_Nationality) j(Year)
foreach var of varlist Reallocation_Share* {
    qui replace `var' = 0 if missing(`var')
}
qui reshape long
keep if Issuer_Residency == "NLD"
gsort Year
drop if Year < 2007

line Reallocation_Share Year, graphregion(color(white)) xtitle("") xlab(2007(2)2017) ytitle("Reallocation Share" " ") ///
    title("Netherlands to Brazil Entry in U.S. Corporate Bond Reallocation Matrix" " ", color(black) size(medsmall)) ///
    lcolor(red) xline(2012, lcolor(black) lpattern(dash)) ///
    text(.146 2006.86 "Petrobras Global Finance BV Established →", place(v) color(gray%90) size(small)) ///
    ylab(0 "0%" .05 "5%" .1 "10%" .15 "15%")

graph export $cmns1/graphs/time_variation_petrobras.pdf, as(pdf) replace

log close
