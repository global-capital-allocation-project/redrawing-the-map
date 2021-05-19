* ---------------------------------------------------------------------------------------------------
* Home_Bias: Produces analysis of home bias in tax havens (Figure 10 and Table A.15 in the paper)
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Home_Bias, replace

* ---------------------------------------------------------------------------------------------------
* Preparing data for home bias regressions
* ---------------------------------------------------------------------------------------------------

* Aggregate reallocaiton shares
use $cmns1/holdings_master/Reallocation-Matrices.dta, clear
qui gen mns_class = ""
qui replace mns_class = "BC" if Asset_Class == "Bonds - Corporate"
qui replace mns_class = "BSALS" if Asset_Class == "Bonds - Government"
qui replace mns_class = "E" if Asset_Class == "Equity"
qui replace mns_class = "BSF" if Asset_Class == "Bonds - Structured Finance"
drop if mns_class == "BSALS"
drop Asset_Class
rename mns_class Asset_Class
qui reshape wide Reallocation_Share, i(Year Investor Issuer_Residency Asset_Class) j(Issuer_Nationality) string
qui reshape long
qui drop if Investor == "NZL"
keep if inlist(Issuer_Residency, $tax_haven_1) | inlist(Issuer_Residency, $tax_haven_2) | inlist(Issuer_Residency, $tax_haven_3) ///
    | inlist(Issuer_Residency, $tax_haven_4) | inlist(Issuer_Residency, $tax_haven_5) | inlist(Issuer_Residency, $tax_haven_6) ///
    | inlist(Issuer_Residency, $tax_haven_7) | inlist(Issuer_Residency, $tax_haven_8)
replace Issuer_Nationality = "EMU" if inlist(Issuer_Nationality, $eu1) | inlist(Issuer_Nationality, $eu2) | inlist(Issuer_Nationality, $eu3)
qui replace Reallocation_Share = 0 if missing(Reallocation_Share)
gcollapse (sum) Reallocation_Share, by(Year Investor Issuer_Residency Asset_Class Issuer_Nationality)
save $cmns1/temp/th_home_bias/aggregate_shares, replace

* Holdings data from Morningstar for TH home bias
use $cmns1/holdings_master/mns_issuer_summary.dta, clear
qui gen mns_class = ""
qui replace mns_class = "BC" if asset_class == "Bonds - Corporate"
qui replace mns_class = "BSALS" if asset_class == "Bonds - Government"
qui replace mns_class = "E" if asset_class == "Equity"
qui replace mns_class = "BSF" if asset_class == "Bonds - Structured Finance"
drop if mns_class == "BSALS"
drop asset_class
rename mns_class asset_class
replace country_bg = "EMU" if inlist(country_bg, $eu1) | inlist(country_bg, $eu2) | inlist(country_bg, $eu3)
qui drop if Domicile == "NZL"
keep if inlist(cgs_domicile, $tax_haven_1) | inlist(cgs_domicile, $tax_haven_2) | inlist(cgs_domicile, $tax_haven_3) ///
    | inlist(cgs_domicile, $tax_haven_4) | inlist(cgs_domicile, $tax_haven_5) | inlist(cgs_domicile, $tax_haven_6) ///
    | inlist(cgs_domicile, $tax_haven_7) | inlist(cgs_domicile, $tax_haven_8)
gcollapse (sum) marketvalue_usd, by(year DomicileCountryId cgs_domicile country_bg asset_class)
rename (year DomicileCountryId cgs_domicile asset_class country_bg) ///
    (Year Investor Issuer_Residency Asset_Class Issuer_Nationality)
rename marketvalue_usd Holdings
replace Holdings = Holdings / 1e9
rename Holdings Holdings_MNS
drop if missing(Issuer_Residency) | missing(Issuer_Nationality) | missing(Asset_Class) | missing(Year) | missing(Investor)
gsort Year Investor Issuer_Residency Asset_Class Issuer_Nationality
save $cmns1/temp/th_home_bias/holdings_mns, replace

* Raw version of CPIS
use $cmns1/holdings_master/TIC-CPIS-Master-Main.dta, clear
keep if Year == 2017
drop if Investor == "USA" & Asset_Class == "Debt Securities"
keep if inlist(Asset_Class, "Common Equity", "Equity Securities", "Corporate Debt Securities")
replace Asset_Class = "E" if inlist(Asset_Class, "Common Equity", "Equity Securities")
replace Asset_Class = "BC" if Asset_Class == "Corporate Debt Securities"
replace Asset_Class = "BC" if Asset_Class == "Debt Securities"
replace Position = 0 if missing(Position)
rename Position CPIS_Raw
rename Issuer Issuer_Residency
save $cmns1/temp/th_home_bias/cpis_raw, replace

* Disaggregated version of CPIS
use $cmns1/holdings_master/CPIS-Clean-Main-Disagg-EMU.dta, clear
rename investor Investor
drop investor_name censored issuer_name
rename issuer Issuer_Residency
rename year Year
rename position CPIS_Raw
replace Investor = "EMU" if inlist(Investor, $eu1) | inlist(Investor, $eu2) | inlist(Investor, $eu3)
keep if Year == 2017
gen Asset_Class = "BC" if asset_class == "Debt (All)"
replace Asset_Class = "E" if asset_class == "Equity (All)"
gcollapse (sum) CPIS_Raw, by(Investor Issuer_Residency Year Asset_Class)
save $cmns1/temp/th_home_bias/cpis_raw_extras, replace

* Adding in TIC data
use $cmns1/holdings_master/TIC-Disaggregated-Clean-Main.dta, clear
keep if year == 2017
keep iso year corporate_debt common_equity
gen Investor = "USA"
rename iso Issuer_Residency
rename year Year
rename common_equity CPIS_RefinedE
rename corporate_debt CPIS_RefinedBC
qui reshape long CPIS_Refined, i(Year Issuer_Residency Investor) j(Asset_Class) string
replace CPIS_Refined = 0 if missing(CPIS_Refined)
save $cmns1/temp/th_home_bias/cpis_refined_entries, replace

* Merge the data sources to generate CPIS weights
use $cmns1/temp/th_home_bias/cpis_raw.dta, clear
qui mmerge Year Investor Asset_Class Issuer_Residency using $cmns1/temp/th_home_bias/cpis_raw_extras.dta, unmatched(b) update
qui mmerge Year Investor Asset_Class Issuer_Residency using $cmns1/temp/th_home_bias/cpis_refined_entries, unmatched(m)
replace CPIS_Refined = CPIS_Raw if missing(CPIS_Refined)
drop _merge
save $cmns1/temp/th_home_bias/cpis_weights, replace

* Consolidated dataframe for home bias regressions
use $cmns1/temp/th_home_bias/aggregate_shares, clear
drop if missing(Issuer_Residency) | missing(Issuer_Nationality) | missing(Asset_Class) | missing(Year) | missing(Investor)
qui mmerge Year Investor Issuer_Residency Asset_Class Issuer_Nationality using $cmns1/temp/th_home_bias/holdings_mns
drop _merge
qui replace Holdings_MNS = 0 if missing(Holdings_MNS)
order Year Investor Asset_Class Issuer_Residency
bys Year Investor Asset_Class Issuer_Residency: egen Tot_Residency_MNS = total(Holdings_MNS)
keep if inlist(Issuer_Nationality, "USA", "EMU", "CAN", "GBR") | inlist(Issuer_Nationality, "AUS", "CHE", "DNK", "NOR", "SWE")
gen Home = 0
replace Home = 1 if Investor == Issuer_Nationality
bys Year Asset_Class Issuer_Residency: egen Tot_Tax_Haven_MNS = total(Holdings_MNS)
qui mmerge Year Investor Asset_Class Issuer_Residency using $cmns1/temp/th_home_bias/cpis_weights, unmatched(m)
count if missing(CPIS_Raw) & Year == 2017 & Tot_Residency_MNS > 0 & Asset_Class != "BSF"
count if missing(CPIS_Refined) & Year == 2017 & Tot_Residency_MNS > 0 & Asset_Class != "BSF"
qui replace CPIS_Raw = 0 if missing(CPIS_Raw) & Year == 2017 
qui replace CPIS_Refined = 0 if missing(CPIS_Refined) & Year == 2017
bys Year Asset_Class Issuer_Residency: egen Tot_Tax_Haven_CPIS_Raw = total(CPIS_Raw)
bys Year Asset_Class Issuer_Residency: egen Tot_Tax_Haven_CPIS_Refined = total(CPIS_Refined)
cap drop Tot_Tax_Haven_MNS
bys Year Asset_Class Issuer_Residency: egen Tot_Tax_Haven_MNS = total(Tot_Residency_MNS)
foreach var in "CPIS_Raw" "CPIS_Refined" "Tot_Tax_Haven_CPIS_Raw" "Tot_Tax_Haven_CPIS_Refined" {
    qui replace `var' = 1e-10 if `var' <= 0
}
save $cmns1/temp/th_home_bias/aggregate_regdata, replace

* ---------------------------------------------------------------------------------------------------
* Running home bias regressions
* ---------------------------------------------------------------------------------------------------

use $cmns1/temp/th_home_bias/aggregate_regdata.dta, clear
egen IK_Indicator = group(Issuer_Residency Issuer_Nationality)
gen Ones = 1

foreach weight_type in "IJ_CPIS_Raw" "I_CPIS_Raw" "IJ_CPIS_Ref" "I_CPIS_Ref" "UW" {
foreach year in 2017 {

    * Define weights
    if "`weight_type'" == "IJ_CPIS_Raw" {
        local aw_var = "CPIS_Raw"
    }
    if "`weight_type'" == "I_CPIS_Raw" {
        local aw_var = "Tot_Tax_Haven_CPIS_Raw"
    }
    if "`weight_type'" == "IJ_CPIS_Ref" {
        local aw_var = "CPIS_Refined"
    }
    if "`weight_type'" == "I_CPIS_Ref" {
        local aw_var = "Tot_Tax_Haven_CPIS_Refined"
    }
    if "`weight_type'" == "UW" {
        local aw_var = "Ones"
    }
        
    * Regressions including zeros
    di "Processing `weight_type', Baseline"
    qui reg Reallocation_Share Home i.IK_Indicator if Year == `year' & Asset_Class == "BC" [aw = `aw_var'], ///
        vce(cluster IK_Indicator)
    est store bc_`year'_`weight_type', title("BC, `weight_type'")

    qui reg Reallocation_Share Home i.IK_Indicator if Year == `year' & Asset_Class == "E" [aw = `aw_var'], ///
         vce(cluster IK_Indicator)
    est store e_`year'_`weight_type', title("E, `weight_type'")
    
    * Regressions excluding zeros
    di "Processing `weight_type', No Zeroes"
    qui reg Reallocation_Share Home i.IK_Indicator if Year == `year' & Asset_Class == "BC" & Reallocation_Share > 0 [aw = `aw_var'], ///
        vce(cluster IK_Indicator)
    est store bc_nz_`year'_`weight_type', title("BC, `weight_type', NoZero")

    qui reg Reallocation_Share Home i.IK_Indicator if Year == `year' & Asset_Class == "E" & Reallocation_Share > 0 [aw = `aw_var'], ///
         vce(cluster IK_Indicator)
    est store e_nz_`year'_`weight_type', title("E, `weight_type', NoZero")

}
}

cap rm $cmns1/tables/home_bias_regressions.xls

foreach weight_type in "IJ_CPIS_Raw" "IJ_CPIS_Ref" "I_CPIS_Raw" "I_CPIS_Ref" "UW" {

    estout bc_2017_`weight_type' e_2017_`weight_type' bc_nz_2017_`weight_type' e_nz_2017_`weight_type' ///
        using $cmns1/tables/home_bias_regressions.xls, ///
        cells(b(star fmt(2)) se(par fmt(2))) label stats(r2 N) ///
        keep(Home _cons) append

}

* ---------------------------------------------------------------------------------------------------
* Long versions of issuance distribution matrices
* ---------------------------------------------------------------------------------------------------

* Long matrix: equities
import excel using $cmns1/issuance_based_matrices/Equity_xls/Issuance_Distribution_Matrix_Equities_2017.xls, ///
    clear firstrow
rename * share*
rename shareresidency Residency
qui reshape long share, i(Residency) j(Nationality) string
gen Year = 2017
save $scratch/issuance_matrix_long_E, replace

* Long matrix: corporate bonds
import excel using $cmns1/issuance_based_matrices/Corporate_Bonds_xls/Issuance_Distribution_Matrix_Corporate_Bonds_2017.xls, ///
    clear firstrow
rename * share*
rename shareresidency Residency
qui reshape long share, i(Residency) j(Nationality) string
replace share = share / 100
bys Residency: egen totShare = total(share)
drop totShare
gen Year = 2017
save $scratch/issuance_matrix_long_BC, replace

* Long matrix: all bonds
import excel using $cmns1/issuance_based_matrices/All_Bonds_xls/Issuance_Distribution_Matrix_All_Bonds_2017.xls, ///
    clear firstrow
rename * share*
rename shareresidency Residency
qui reshape long share, i(Residency) j(Nationality) string
replace share = share / 100
bys Residency: egen totShare = total(share)
drop totShare
gen Year = 2017
save $scratch/issuance_matrix_long_B, replace

* ---------------------------------------------------------------------------------------------------
* Additional restatements for home bias barcharts
* ---------------------------------------------------------------------------------------------------

* Corrected merged TIC-CPIS with all bonds category for USA
use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
keep if Investor == "USA" & Year == 2017
keep if inlist(Asset_Class_Code, "BC", "BG", "BSF")
gcollapse (sum) Position_Residency, by(Year Investor Issuer)
gen Asset_Class_Code = "B"
gen Asset_Class = "All Bonds"
tempfile usa_b
save `usa_b', replace

use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
keep if Year == 2017
append using `usa_b'
replace Asset_Class_Code = "E" if Asset_Class_Code == "EF"
save $cmns1/temp/corrected_cpis_with_usa_b, replace

* Reallocation matrices for USA all bonds
forval year = 2007/2017 {
    use $cmns1/holdings_master/mns_issuer_summary.dta, clear
    qui keep if DomicileCountryId == "USA"
    qui drop if asset_class == "Equity"
    qui keep if year == `year'
    gcollapse (sum) marketvalue_usd, by(cgs_domicile country_bg)
    bys cgs_domicile: egen totVal = total(marketvalue_usd)
    gen share = marketvalue_usd / totVal
    keep cgs_domicile country_bg share
    qui reshape wide share, i(cgs_domicile) j(country_bg) string
    foreach var of varlist share* {
        qui replace `var' = 0 if missing(`var')
        qui replace `var' = `var' * 100
    }
    rename share* *
    rename cgs_domicile A
    qui export excel using $scratch/Reallocation_Matrix_USA_All_Bonds_`year'.xls, replace firstrow(variables)    
}

* Matrix multiplication: Issuance-based
foreach investor in "AUS" "CAN" "CHE" "DNK" "EMU" "GBR" "NOR" "SWE" "USA" {

    use $cmns1/temp/corrected_cpis_with_usa_b, clear
    keep if inlist(Asset_Class_Code, "BC", "B", "E", "EF")
    qui replace Asset_Class_Code = "E" if Asset_Class_Code == "EF"
    
    qui levelsof Asset_Class_Code if Investor == "`investor'", local(classes)
    foreach class of local classes {
        
        * Set locals
        di "Processing `investor' - `class'"
        if inlist("`class'", "E") {
            local matrix_class = "Equities"
        }
        if inlist("`class'", "BC") {
            local matrix_class = "Corporate_Bonds"
        }
        if inlist("`class'", "B") {
            local matrix_class = "All_Bonds"
        }
        
        * Reconstruct positions
        use $cmns1/temp/corrected_cpis_with_usa_b, clear
        keep if Investor == "`investor'"
        keep if Asset_Class_Code == "`class'"
        keep if Year == 2017
        keep Year Investor Asset_Class_Code Issuer Position_Residency
        rename Issuer Residency
        qui mmerge Year Residency using $scratch/issuance_matrix_long_`class', unmatched(m)
        qui replace share = 1 if _merge == 1
        qui replace Nationality = Residency if _merge == 1
        drop _merge
        gen share_th_only = share
        gen th = 0
        qui replace th = 1 if inlist(Residency, $tax_haven_1) | inlist(Residency, $tax_haven_2) | inlist(Residency, $tax_haven_3) | inlist(Residency, $tax_haven_4) | inlist(Residency, $tax_haven_5) | inlist(Residency, $tax_haven_6) | inlist(Residency, $tax_haven_7) | inlist(Residency, $tax_haven_8)
        qui replace share_th_only = 1 if th == 0 & Residency == Nationality
        qui replace share_th_only = 0 if th == 0 & Residency != Nationality
        gen Position_Nationality_Full = share * Position_Residency
        gen Position_Nationality_TH_Only = share_th_only * Position_Residency
        save $scratch/issuance_rescaled_flows_`investor'_`class', replace
        gcollapse (sum) Position_Nationality_Full Position_Nationality_TH_Only, by(Year Nationality)
        save $scratch/issuance_based_positions, replace

        * Merge with original file
        use $cmns1/temp/corrected_cpis_with_usa_b, clear
        keep if Investor == "`investor'"
        keep if Asset_Class_Code == "`class'"
        keep if Year == 2017
        keep Year Investor Asset_Class_Code Issuer Position_Residency
        rename Issuer Nationality
        qui mmerge Year Nationality using $scratch/issuance_based_positions.dta, unmatched(m) uname(IBD_)
        drop _merge
        gsort -Year -Position_Residency
        foreach var of varlist *Position* {
            qui replace `var' = `var' / 1e3
        }
        save $scratch/issuance_based_positions_`investor'_`class', replace

    }
}

* Matrix multiplication: Holdings-based
foreach investor in "USA" "EMU" "CAN" "GBR" "AUS" "DNK" "NOR" "SWE" "CHE" {

    use $cmns1/temp/corrected_cpis_with_usa_b, clear
    qui levelsof Asset_Class_Code if Investor == "`investor'", local(classes)
    foreach class of local classes {
        
        * Set locals
        di "Processing `investor' - `class'"
        if inlist("`class'", "E", "EF") {
            local matrix_class = "Equities"
        }
        if inlist("`class'", "BC") {
            local matrix_class = "Corporate_Bonds"
        }
        if inlist("`class'", "B") {
            local matrix_class = "All_Bonds"
        }
        if inlist("`class'", "BG") {
            local matrix_class = "Sovereign_Agency_Muni_Bonds"
        }       
        if inlist("`class'", "BSF") {
            local matrix_class = "Asset_Backed_Securities"
        }
        
        * Import matrices
        forval year = 2007/2017 {

            * Read and reshape
            if "`investor'" == "USA" & "`class'" == "B" {
                import excel using $scratch/Reallocation_Matrix_`investor'_`matrix_class'_`year'.xls, clear firstrow
                rename * share*
                rename shareA Residency
            }
            else {
                import excel using $scratch/Reallocation_Matrix_`investor'_`matrix_class'_`year'.xls, clear firstrow
                rename * share*
                rename shareImmediate Residency
            }
            qui reshape long share, i(Residency) j(Nationality) string
            qui replace share = share / 100

            * Don't reallocate LUX for CPIS countries            
            qui replace share = 1 if "`class'" == "EF" & "`investor'" != "USA" & Residency == "LUX" & Nationality == "LUX"
            qui replace share = 0 if "`class'" == "EF" & "`investor'" != "USA" & Residency == "LUX" & Nationality != "LUX"
            gen Year = `year'
            qui save $scratch/matrix_long_`year', replace
        }
        clear
        forval year = 2007/2017 {
            append using $scratch/matrix_long_`year'
        }
        sort Year Residency
        save $scratch/matrix_long.dta, replace

        * Reconstruct positions
        use $cmns1/temp/corrected_cpis_with_usa_b, clear
        qui replace Position_Residency = 0 if Investor == "EMU" & Issuer == "LUX" & Asset_Class_Code == "EF"
        keep if Investor == "`investor'"
        keep if Asset_Class_Code == "`class'"
        keep Year Investor Asset_Class_Code Issuer Position_Residency
        rename Issuer Residency
        qui mmerge Year Residency using $scratch/matrix_long.dta, unmatched(m)
        qui replace share = 1 if _merge == 1
        qui replace Nationality = Residency if _merge == 1
        drop _merge
        gen share_th_only = share
        gen th = 0
        qui replace th = 1 if inlist(Residency, $tax_haven_1) | inlist(Residency, $tax_haven_2) | inlist(Residency, $tax_haven_3) | inlist(Residency, $tax_haven_4) | inlist(Residency, $tax_haven_5) | inlist(Residency, $tax_haven_6) | inlist(Residency, $tax_haven_7) | inlist(Residency, $tax_haven_8)
        qui replace share_th_only = 1 if th == 0 & Residency == Nationality
        qui replace share_th_only = 0 if th == 0 & Residency != Nationality
        gen Position_Nationality_Full = share * Position_Residency
        gen Position_Nationality_TH_Only = share_th_only * Position_Residency
        save $scratch/holdings_rescaled_flows_`investor'_`class', replace

    }
}

* ---------------------------------------------------------------------------------------------------
* Home bias barcharts
* ---------------------------------------------------------------------------------------------------

* Prepare data for the barcharts
foreach class in "B" "E" {

    clear
    foreach investor in "USA" "EMU" "CAN" "GBR" "AUS" "DNK" "NOR" "SWE" "CHE" {
        append using $scratch/holdings_rescaled_flows_`investor'_`class'.dta
    }
    keep if Year == 2017
    keep if th == 1
    drop if missing(Nationality)
    replace Nationality = "EMU" if inlist(Nationality, $eu1) | inlist(Nationality, $eu2) | inlist(Nationality, $eu3)
    replace Nationality = "Foreign" if Nationality != Investor
    replace Nationality = "Domestic" if Nationality == Investor
    gcollapse (sum) Position_Nationality_TH_Only, by(Investor Nationality)
    rename Position_Nationality_TH_Only Position
    bys Investor: egen Tot_Holdings = total(Position)
    gen Domestic_Share_Holdings = Position / Tot_Holdings
    keep if Nationality == "Domestic"
    keep Investor Domestic_Share_Holdings
    save $scratch/domestic_shares_holdings_`class', replace


    clear
    foreach investor in "USA" "EMU" "CAN" "GBR" "AUS" "DNK" "NOR" "SWE" "CHE" {
        append using $scratch/issuance_rescaled_flows_`investor'_`class'.dta
    }
    keep if Year == 2017
    keep if th == 1
    drop if missing(Nationality)
    replace Nationality = "EMU" if inlist(Nationality, $eu1) | inlist(Nationality, $eu2) | inlist(Nationality, $eu3)
    replace Nationality = "Foreign" if Nationality != Investor
    replace Nationality = "Domestic" if Nationality == Investor
    gcollapse (sum) Position_Nationality_TH_Only, by(Investor Nationality)
    rename Position_Nationality_TH_Only Position
    bys Investor: egen Tot_Holdings = total(Position)
    gen Domestic_Share_Issuance = Position / Tot_Holdings
    keep if Nationality == "Domestic"
    keep Investor Domestic_Share_Issuance
    qui mmerge Investor using $scratch/domestic_shares_holdings_`class'.dta
    drop _merge
    save $scratch/home_bias_data_new_`class', replace
    
}

* Bonds plot
use $scratch/home_bias_data_new_B, clear
graph bar (asis) Domestic_Share_Holdings Domestic_Share_Issuance, over(Investor, sort(Investor)) ///
    graphregion(color(white)) nofill ///
    xsize(6) ytitle("Share of Tax Haven Investment" "That is Domestic Under Nationality" " ") ///
    legend(label(1 "Baseline ({&Omega}{sub:j})") label(2  "Issuance Distribution Matrix ({it:b})") rows(1)) ///
    bar(1, color(blue) fintensity(inten40)) bar(2, color(red) fintensity(inten40))

graph export $cmns1/graphs/home_bias_all_bonds.pdf, as(pdf) replace


* Equities plot
use $scratch/home_bias_data_new_E, clear
graph bar (asis) Domestic_Share_Holdings Domestic_Share_Issuance, over(Investor, sort(Investor)) ///
    graphregion(color(white)) nofill ///
    xsize(6) ytitle("Share of Tax Haven Investment" "That is Domestic Under Nationality" " ") ///
    legend(label(1 "Baseline ({&Omega}{sub:j})") label(2  "Issuance Distribution Matrix ({it:b})") rows(1)) ///
    bar(1, color(blue) fintensity(inten40)) bar(2, color(red) fintensity(inten40))

graph export $cmns1/graphs/home_bias_equities.pdf, as(pdf) replace


log close
