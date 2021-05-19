* ---------------------------------------------------------------------------------------------------
* Restated_TIC_CPIS: Produces nationality-basis restatements of bilateral portfolios based on 
* reallocation matrices estimated from Morningstar holdings data
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Restated_TIC_CPIS, replace

* ---------------------------------------------------------------------------------------------------
* Generate an amended version of Norway's CPIS reporting (due to reporting issue)
* ---------------------------------------------------------------------------------------------------

* Country names to ISO codes
import excel using $raw/Macro/Concordances/country_and_currency_codes.xlsx, clear firstrow
keep country_name iso_country_code
bys country_name: keep if _n == 1
save $scratch/country_name_to_iso, replace

* Import enhanced CPIS for Norway
forval year=2014/2017 {
foreach code in "B" "EF" {
    import excel using $raw/CPIS/enhanced_cpis/Enhanced_CPIS_NOR_`code'_`year'.xls, clear firstrow cellrange(A6)
    keep Investmentin C GeneralGovernment
    rename C Total
    qui drop if missing(Investmentin)
    qui drop if missing(Total)
    qui drop if Investmentin == "World"
    rename Investmentin country_name
    qui mmerge country_name using $scratch/country_name_to_iso.dta, unmatched(m) umatch(country_name)
    qui replace iso_country_code = "BHS" if country_name == "Bahamas, The"
    qui replace iso_country_code = "VGB" if country_name == "British Virgin Islands"
    qui replace iso_country_code = "HKG" if country_name == "China, P.R.: Hong Kong"
    qui replace iso_country_code = "CHN" if country_name == "China, P.R.: Mainland"
    qui replace iso_country_code = "HRV" if country_name == "Croatia, Rep. of"
    qui replace iso_country_code = "CUW" if country_name == "Curaçao, Kingdom of the Netherlands"
    qui replace iso_country_code = "CZE" if country_name == "Czech Rep."
    qui replace iso_country_code = "DOM" if country_name == "Dominican Rep."
    qui replace iso_country_code = "EGY" if country_name == "Egypt, Arab Rep. of"
    qui replace iso_country_code = "EST" if country_name == "Estonia, Rep. of"
    qui replace iso_country_code = "XSN" if country_name == "International Organizations"
    qui replace iso_country_code = "KAZ" if country_name == "Kazakhstan, Rep. of"
    qui replace iso_country_code = "KOR" if country_name == "Korea, Rep. of"
    qui replace iso_country_code = "MHL" if country_name == "Marshall Islands, Rep. of the"
    qui replace iso_country_code = "NLD" if country_name == "Netherlands, The"
    qui replace iso_country_code = "POL" if country_name == "Poland, Rep. of"
    qui replace iso_country_code = "SVK" if country_name == "Slovak Rep."
    qui replace iso_country_code = "SVN" if country_name == "Slovenia, Rep. of"
    qui replace iso_country_code = "VIR" if country_name == "United States Virgin Islands"
    qui replace iso_country_code = "VEN" if country_name == "Venezuela, Rep. Bolivariana de"
    qui replace iso_country_code = "VNM" if country_name == "Vietnam"
    qui replace iso_country_code = "OTH" if country_name == "Not Specified (including Confidential)"
    qui replace iso_country_code = "AZE" if country_name == "Azerbaijan, Rep. of"
    qui replace iso_country_code = "TWN" if country_name == "Taiwan Province of China"
    qui replace iso_country_code = "PUS" if country_name == "US Pacific Islands"
    qui replace iso_country_code = "BHR" if country_name == "Bahrain, Kingdom of"
    qui replace iso_country_code = "AFG" if country_name == "Afghanistan, Islamic Rep. of"
    qui replace iso_country_code = "AND" if country_name == "Andorra, Principality of"
    qui replace iso_country_code = "SWZ" if country_name == "Eswatini, Kingdom of"
    qui replace iso_country_code = "MOZ" if country_name == "Mozambique, Rep. of"
    qui replace iso_country_code = "TZA" if country_name == "Tanzania, United Rep. of"
    qui replace iso_country_code = "MAC" if country_name == "China, P.R.: Macao"
    qui replace iso_country_code = "SSD" if country_name == "South Sudan, Rep. of"
    qui replace iso_country_code = "LAO" if country_name == "Lao People's Dem. Rep."
    assert ~missing(iso_country_code)
    drop _merge
    qui replace GeneralGovernment = 0 if missing(GeneralGovernment)
    gen NonSWF = Total - GeneralGovernment
    assert NonSWF >= 0 
    gen Investor = "NOR"
    gen Year = `year'
    gen Asset_Class_Code = "`code'"
    qui save $scratch/nor_enhanced_cpis_`code'_`year', replace
}
}

* Append enhanced CPIS
clear
forval year=2014/2017 {
foreach code in "B" "EF" {
    append using $scratch/nor_enhanced_cpis_`code'_`year'
}
}
save $scratch/nor_enhanced_cpis, replace

use $scratch/nor_enhanced_cpis, clear
drop if iso_country_code == "OTH"
bys Year Asset_Class_Code: egen totGG = total(GeneralGovernment)
gcollapse (sum) GeneralGovernment, by(Year Asset_Class_Code)
save $scratch/nor_ecpis_gengov_totals, replace

* Scale SWF holdings
forval year = 2014/2017 {
    use $cmns1/temp/norway_swf/all_`year'_adjusted.dta, clear
    collapse (sum) MarketValueUSD, by(Asset_Class Residency)
    rename Residency Issuer
    rename MarketValueUSD SWF
    replace SWF = SWF / 1e6
    tempfile corrected_cpis_swf
    gen Asset_Class_Code = "B" if Asset_Class == "Bonds"
    replace Asset_Class_Code = "EF" if Asset_Class == "Equity"
    gen Year = `year'
    drop if missing(Issuer)
    qui mmerge Year Asset_Class_Code using $scratch/nor_ecpis_gengov_totals.dta, umatch(Year Asset_Class_Code) unmatched(m)
    assert _merge == 3
    drop _merge
    bys Year Asset_Class_Code: egen totSWF = total(SWF)
    replace SWF = SWF * (GeneralGovernment / totSWF)
    keep Year Asset_Class_Code Issuer SWF
    save $cmns1/temp/norway_swf/all_`year'_adjusted_scaled, replace
}
clear
forval year = 2014/2017 {
    append using $cmns1/temp/norway_swf/all_`year'_adjusted_scaled
}
gsort Year Asset_Class_Code Issuer
save $cmns1/temp/norway_swf/all_adjusted_scaled, replace

* Make the adjustment, but without the domestic component
use $cmns1/holdings_master/TIC-CPIS-Augmented-Main, clear
cap drop *Name* Position_Nationality* Residency_Flag
keep if Investor == "NOR"
drop if Issuer == "NOR"
qui keep if Year >= 2014
keep Year Investor Asset_Class_Code Issuer Position_Residency
qui mmerge Year Asset_Class_Code Issuer using $cmns1/temp/norway_swf/all_adjusted_scaled
drop if Issuer == "OTH"
assert _merge ~= 2
drop _merge
qui mmerge Year Asset_Class_Code Issuer using $scratch/nor_enhanced_cpis, unmatched(m) umatch(Year Asset_Class_Code iso_country_code)
drop _merge
drop country_name Total GeneralGovernment
foreach var of varlist Position_Residency SWF NonSWF {
    qui replace `var' = 0 if missing(`var')
}
qui replace Position_Residency = SWF + NonSWF
rename Position_Residency Corrected_CPIS
rename NonSWF Non_SWF
keep Year Asset_Class Issuer Corrected_CPIS SWF Non_SWF
save $scratch/corrected_cpis_pre, replace

* Domestic imputation
use $cmns1/holdings_master/mns_issuer_summary.dta, clear
qui keep if Domicile == "NOR"
qui drop if issuer_number == "L8882U" & asset_class == "Equity"
qui replace asset_class = "Bonds" if asset_class != "Equity"
qui replace cgs_domicile = "XB" if cgs_domicile != "NOR"
collapse (sum) marketvalue_usd, by(asset_class cgs_domicile year)
bys year asset_class: egen totVal = total(marketvalue_usd)
gen share = marketvalue_usd / totVal
qui keep if cgs_domicile == "XB"
keep year asset_class cgs_domicile share
drop cgs_domicile
rename share mns_foreign_share
tempfile foreign_shares
rename asset_class Asset_Class
rename year Year
gen Asset_Class_Code = "B" if Asset_Class == "Bonds"
qui replace Asset_Class_Code = "EF" if Asset_Class == "Equity"
drop Asset_Class
save $scratch/nor_foreign_shares, replace

use $scratch/corrected_cpis_pre, clear
collapse (sum) Non_SWF, by(Year Asset_Class_Code)
qui mmerge Year Asset_Class_Code using $scratch/nor_foreign_shares, unmatched(m)
drop _merge
gen Domestic_Imputation = ((1 - mns_foreign_share) / mns_foreign_share) * Non_SWF
drop mns_foreign_share
keep Year Asset_Class Domestic_Imputation
gen Issuer = "NOR"
rename Domestic_Imputation Corrected_CPIS
tempfile domestic_impute
qui save `domestic_impute', replace

* Add the domestic imputation
use $scratch/corrected_cpis_pre, clear
drop if Issuer == "NOR"
append using `domestic_impute'
gen Investor = "NOR"
keep Year Investor Issuer Asset_Class_Code Corrected_CPIS
count
gsort Investor Year Asset_Class_Code Issuer
save $scratch/norway_cpis_adjustments_v2, replace

* Make Norway correction
use $cmns1/holdings_master/TIC-CPIS-Augmented-Main, clear
cap drop *Name* Position_Nationality* Residency_Flag
qui mmerge Investor Year Asset_Class_Code Issuer using $scratch/norway_cpis_adjustments_v2, unmatched(m)
replace Position_Residency = Corrected_CPIS if Investor == "NOR" & Year >= 2014
drop Corrected_CPIS _merge
save $cmns1/temp/country_portfolios_residency_nor_adjusted, replace

* ---------------------------------------------------------------------------------------------------
* Carry out the fund shares adjustments: benchmark estimates
* ---------------------------------------------------------------------------------------------------

* Data for Ireland adjustment
use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
keep if Issuer == "IRL" & Investor != "USA"
keep if Asset_Class == "Common Equity and Fund Shares"
qui keep if Year >= 2007
qui mmerge Year using $cmns1/temp/fund_shares_corrections/irl_common_shares.dta, unmatched(m) umatch(year) 
assert _merge == 3
drop _merge
replace row_common_share = 1 if row_common_share > 1
replace Position_Residency = Position_Residency * emu_common_share if Investor == "EMU"
replace Position_Residency = Position_Residency * row_common_share if Investor != "EMU"
replace Asset_Class = "Common Equity"
replace Asset_Class_Code = "E"
drop *share
save $cmns1/temp/fund_shares_corrections/irl_imputed_common, replace

* Data for Netherlands adjustment
use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
keep if Issuer == "NLD" & Investor != "USA"
keep if Asset_Class == "Common Equity and Fund Shares"
qui keep if Year >= 2007
qui mmerge Year using $cmns1/temp/fund_shares_corrections/nld_common_shares.dta, unmatched(m) umatch(year) 
assert _merge == 3
drop _merge
replace row_common_share = 1 if row_common_share > 1
replace Position_Residency = Position_Residency * emu_common_share if Investor == "EMU"
replace Position_Residency = Position_Residency * row_common_share if Investor != "EMU"
replace Asset_Class = "Common Equity"
replace Asset_Class_Code = "E"
drop *share
save $cmns1/temp/fund_shares_corrections/nld_imputed_common, replace

* Data for Cyprus adjustment
use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
keep if Issuer == "CYP" & Investor != "USA"
keep if Asset_Class == "Common Equity and Fund Shares"
qui keep if Year >= 2007
qui mmerge Year using $cmns1/temp/fund_shares_corrections/cyp_common_shares.dta, unmatched(m) umatch(year) 
assert _merge == 3
drop _merge
replace row_common_share = 1 if row_common_share > 1
replace Position_Residency = Position_Residency * emu_common_share if Investor == "EMU"
replace Position_Residency = Position_Residency * row_common_share if Investor != "EMU"
replace Asset_Class = "Common Equity"
replace Asset_Class_Code = "E"
drop *share
save $cmns1/temp/fund_shares_corrections/cyp_imputed_common, replace

* Data for Caymans adjustment
use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
keep if Issuer == "CYM" & Investor != "USA"
keep if Asset_Class == "Common Equity and Fund Shares"
qui mmerge Year using $cmns1/temp/fund_shares_corrections/cym_common_shares.dta, unmatched(m) umatch(year) 
assert _merge == 3
drop _merge
replace row_common_share = 1 if row_common_share > 1
replace Position_Residency = Position_Residency * row_common_share
replace Asset_Class = "Common Equity"
replace Asset_Class_Code = "E"
drop *share
save $cmns1/temp/fund_shares_corrections/cym_imputed_common, replace

* Data for other tax havens adjustment
use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
keep if Investor != "USA"
keep if inlist(Issuer, "CUW", "GGY", "HKG", "IMN", "JEY") | inlist(Issuer, "PAN", "VGB", "BMU", "BHS")
keep if Asset_Class == "Common Equity and Fund Shares"
qui mmerge Year Issuer using $cmns1/temp/fund_shares_corrections/other_common_shares_fillin.dta, unmatched(m) 
assert _merge == 3
drop _merge
replace row_common_share = 1 if row_common_share > 1
replace Position_Residency = Position_Residency * row_common_share
replace Asset_Class = "Common Equity"
replace Asset_Class_Code = "E"
drop *share
save $cmns1/temp/fund_shares_corrections/other_imputed_common, replace

* Performing adjustments
use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
keep if inlist(Asset_Class, "Common Equity", "Common Equity and Fund Shares")
replace Asset_Class = "Common Equity"
replace Asset_Class_Code = "E"
qui keep if Year >= 2007

* IRL adjustment
qui mmerge Year Investor Issuer using $cmns1/temp/fund_shares_corrections/irl_imputed_common.dta, ///
    uname(Adj_) ukeep(Position_Residency)
assert _merge == 3 if Issuer == "IRL" & Investor != "USA"
drop _merge
replace Position_Residency = Adj_Position_Residency if ~missing(Adj_Position_Residency)
drop Adj_Position_Residency

* CYM adjustment
qui mmerge Year Investor Issuer using $cmns1/temp/fund_shares_corrections/cym_imputed_common.dta, ///
    unmatched(m) uname(Adj_) ukeep(Position_Residency)
assert _merge == 3 if Issuer == "CYM" & Investor != "USA"
drop _merge
replace Position_Residency = Adj_Position_Residency if ~missing(Adj_Position_Residency)
drop Adj_Position_Residency

* NLD adjustment
qui mmerge Year Investor Issuer using $cmns1/temp/fund_shares_corrections/nld_imputed_common.dta, ///
    unmatched(m) uname(Adj_) ukeep(Position_Residency)
assert _merge == 3 if Issuer == "NLD" & Investor != "USA"
drop _merge
replace Position_Residency = Adj_Position_Residency if ~missing(Adj_Position_Residency)
drop Adj_Position_Residency

* CYP adjustment
qui mmerge Year Investor Issuer using $cmns1/temp/fund_shares_corrections/cyp_imputed_common.dta, ///
    unmatched(m) uname(Adj_) ukeep(Position_Residency)
assert _merge == 3 if Issuer == "CYP" & Investor != "USA"
drop _merge
replace Position_Residency = Adj_Position_Residency if ~missing(Adj_Position_Residency)
drop Adj_Position_Residency

* OTH adjustment
qui mmerge Year Investor Issuer using $cmns1/temp/fund_shares_corrections/other_imputed_common.dta, ///
    unmatched(m) uname(Adj_) ukeep(Position_Residency)
assert _merge == 3 if Investor != "USA" & (inlist(Issuer, "CUW", "GGY", "HKG", "IMN", "JEY") | inlist(Issuer, "PAN", "VGB", "BMU", "BHS"))
drop _merge
replace Position_Residency = Adj_Position_Residency if ~missing(Adj_Position_Residency)
drop Adj_Position_Residenc

save $cmns1/temp/fund_shares_corrections/tic_cpis_with_funds_adjustment, replace

* ---------------------------------------------------------------------------------------------------
* Carry out the fund shares adjustments: robustness estimates
* ---------------------------------------------------------------------------------------------------

* Data for Caymans adjustment
use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
keep if Issuer == "CYM" & Investor != "USA"
keep if Asset_Class == "Common Equity and Fund Shares"
qui mmerge Year using $cmns1/temp/fund_shares_corrections/cym_common_shares.dta, unmatched(m) umatch(year) 
assert _merge == 3
drop _merge
replace row_common_share = 1 if row_common_share > 1
replace Position_Residency = Position_Residency * us_common_share
replace Asset_Class = "Common Equity"
replace Asset_Class_Code = "E"
drop *share
save $cmns1/temp/fund_shares_corrections/cym_imputed_common_robustness, replace

* Data for other tax havens adjustment
use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
keep if Investor != "USA"
keep if inlist(Issuer, "CUW", "GGY", "HKG", "IMN", "JEY") | inlist(Issuer, "PAN", "VGB", "BMU", "BHS")
keep if Asset_Class == "Common Equity and Fund Shares"
qui mmerge Year Issuer using $cmns1/temp/fund_shares_corrections/other_common_shares_fillin.dta, unmatched(m) 
assert _merge == 3
drop _merge
replace row_common_share = 1 if row_common_share > 1
replace Position_Residency = Position_Residency * us_common_share
replace Asset_Class = "Common Equity"
replace Asset_Class_Code = "E"
drop *share
save $cmns1/temp/fund_shares_corrections/other_imputed_common_robustness, replace

* Performing the adjustments
use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
keep if inlist(Asset_Class, "Common Equity", "Common Equity and Fund Shares")
replace Asset_Class = "Common Equity"
replace Asset_Class_Code = "E"
qui keep if Year >= 2007

* IRL adjustment
qui mmerge Year Investor Issuer using $cmns1/temp/fund_shares_corrections/irl_imputed_common.dta, ///
    uname(Adj_) ukeep(Position_Residency)
assert _merge == 3 if Issuer == "IRL" & Investor != "USA"
drop _merge
replace Position_Residency = Adj_Position_Residency if ~missing(Adj_Position_Residency)
drop Adj_Position_Residency

* CYM adjustment
qui mmerge Year Investor Issuer using $cmns1/temp/fund_shares_corrections/cym_imputed_common_robustness.dta, ///
    unmatched(m) uname(Adj_) ukeep(Position_Residency)
assert _merge == 3 if Issuer == "CYM" & Investor != "USA"
drop _merge
replace Position_Residency = Adj_Position_Residency if ~missing(Adj_Position_Residency)
drop Adj_Position_Residency

* NLD adjustment
qui mmerge Year Investor Issuer using $cmns1/temp/fund_shares_corrections/nld_imputed_common.dta, ///
    unmatched(m) uname(Adj_) ukeep(Position_Residency)
assert _merge == 3 if Issuer == "NLD" & Investor != "USA"
drop _merge
replace Position_Residency = Adj_Position_Residency if ~missing(Adj_Position_Residency)
drop Adj_Position_Residency

* CYP adjustment
qui mmerge Year Investor Issuer using $cmns1/temp/fund_shares_corrections/cyp_imputed_common.dta, ///
    unmatched(m) uname(Adj_) ukeep(Position_Residency)
assert _merge == 3 if Issuer == "CYP" & Investor != "USA"
drop _merge
replace Position_Residency = Adj_Position_Residency if ~missing(Adj_Position_Residency)
drop Adj_Position_Residency

* OTH adjustment
qui mmerge Year Investor Issuer using $cmns1/temp/fund_shares_corrections/other_imputed_common_robustness.dta, ///
    unmatched(m) uname(Adj_) ukeep(Position_Residency)
assert _merge == 3 if Investor != "USA" & (inlist(Issuer, "CUW", "GGY", "HKG", "IMN", "JEY") | inlist(Issuer, "PAN", "VGB", "BMU", "BHS"))
drop _merge
replace Position_Residency = Adj_Position_Residency if ~missing(Adj_Position_Residency)
drop Adj_Position_Residency

save $cmns1/temp/fund_shares_corrections/tic_cpis_with_funds_adjustment_robustness.dta, replace

* ---------------------------------------------------------------------------------------------------
* Matrix multiplication: bonds
* ---------------------------------------------------------------------------------------------------

* Original positions in tax havens
use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
keep if inlist(Issuer, "IRL", "CYM", "NLD", "CYP") | inlist(Issuer, "CUW", "GGY", "HKG", "IMN", "JEY") | inlist(Issuer, "PAN", "VGB", "BMU", "BHS")
keep if inlist(Asset_Class_Code, "E", "EF")
replace Asset_Class_Code = "E"
keep Year Investor Asset_Class_Code Issuer Position_Residency
save $scratch/th_original_positions, replace

* Matrix multiplication: iterate over investors and asset classes
foreach investor in "USA" "EMU" "CAN" "GBR" "AUS" "DNK" "NOR" "SWE" "CHE" {

    use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
    qui drop if Investor == "USA" & Asset_Class_Code == "B"
    qui drop if inlist(Asset_Class_Code, "E", "EF")
    qui levelsof Asset_Class_Code if Investor == "`investor'", local(classes)
    foreach class of local classes {
        
        * Set locals
        di "Processing `investor' - `class'"
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
            import excel using $scratch/Reallocation_Matrix_`investor'_`matrix_class'_`year'.xls, clear firstrow
            rename * share*
            rename shareImmediate_Destination Residency
            qui reshape long share, i(Residency) j(Nationality) string
            qui replace share = share / 100
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
        use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
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
        save $scratch/new_rescaled_flows_`investor'_`class', replace
        gcollapse (sum) Position_Nationality_Full Position_Nationality_TH_Only, by(Year Nationality)
        save $scratch/reconstructed_positions, replace

        * Merge with original file
        use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
        keep if Investor == "`investor'"
        keep if Asset_Class_Code == "`class'"
        keep Year Investor Asset_Class_Code Issuer Position_Residency
        rename Issuer Nationality
        qui mmerge Year Nationality using $scratch/reconstructed_positions.dta, unmatched(m)
        drop _merge
        gsort -Year -Position_Nationality_TH_Only
        foreach var of varlist *Position* {
            qui replace `var' = `var' / 1e3
        }
        save $scratch/reconstruction_`investor'_`class', replace

    }
}

* ---------------------------------------------------------------------------------------------------
* Matrix multiplication: equities
* ---------------------------------------------------------------------------------------------------

* Matrix multiplication: iterate over investors and asset classes
foreach investor in "USA" "EMU" "CAN" "GBR" "AUS" "DNK" "NOR" "SWE" "CHE" {

    di "Estimating `investor'"

    use $cmns1/temp/fund_shares_corrections/tic_cpis_with_funds_adjustment, clear
    qui levelsof Asset_Class_Code if Investor == "`investor'", local(classes)
    foreach class of local classes {
        
        * Set locals
        assert "`class'" == "E"
        di "Processing `investor' - `class'"
        if inlist("`class'", "E") {
            local matrix_class = "Equities"
        }
        
        * Import matrices
        forval year = 2007/2017 {

            * Read and reshape
            import excel using $scratch/Reallocation_Matrix_`investor'_`matrix_class'_`year'.xls, clear firstrow
            rename * share*
            rename shareImmediate_Destination Residency
            qui reshape long share, i(Residency) j(Nationality) string
            qui replace share = share / 100

            * Don't reallocate LUX for CPIS countries            
            qui replace share = 1 if "`class'" == "E" & "`investor'" != "USA" & Residency == "LUX" & Nationality == "LUX"
            qui replace share = 0 if "`class'" == "E" & "`investor'" != "USA" & Residency == "LUX" & Nationality != "LUX"
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
        use $cmns1/temp/fund_shares_corrections/tic_cpis_with_funds_adjustment, clear
        qui replace Position_Residency = 0 if Investor == "EMU" & Issuer == "LUX" & Asset_Class_Code == "E"
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
        save $scratch/new_rescaled_flows_`investor'_`class', replace
        gcollapse (sum) Position_Nationality_Full Position_Nationality_TH_Only, by(Year Nationality)
        save $scratch/funds_adjustment_nationality_positions, replace

        * Merge with original file
        use $cmns1/temp/fund_shares_corrections/tic_cpis_with_funds_adjustment, clear
        qui replace Position_Residency = 0 if Investor == "EMU" & Issuer == "LUX" & Asset_Class_Code == "E"
        keep if Investor == "`investor'"
        keep if Asset_Class_Code == "`class'"
        keep Year Investor Asset_Class_Code Issuer
        rename Issuer Nationality
        qui mmerge Year Nationality using $scratch/funds_adjustment_nationality_positions.dta, unmatched(m)
        drop _merge
        gsort -Year -Position_Nationality_TH_Only
        foreach var of varlist *Position* {
            qui replace `var' = `var' / 1e3
        }
        save $scratch/reconstruction_`investor'_`class', replace

    }
}

* ---------------------------------------------------------------------------------------------------
* Matrix multiplication: equities, robustness
* ---------------------------------------------------------------------------------------------------

* Matrix multiplication: iterate over investors and asset classes
foreach investor in "USA" "EMU" "CAN" "GBR" "AUS" "DNK" "NOR" "SWE" "CHE" {

    di "Estimating `investor'"

    use $cmns1/temp/fund_shares_corrections/tic_cpis_with_funds_adjustment_robustness, clear
    qui levelsof Asset_Class_Code if Investor == "`investor'", local(classes)
    foreach class of local classes {
        
        * Set locals
        assert "`class'" == "E"
        di "Processing `investor' - `class'"
        if inlist("`class'", "E") {
            local matrix_class = "Equities"
        }
        
        * Import matrices
        forval year = 2007/2017 {

            * Read and reshape
            import excel using $scratch/Reallocation_Matrix_`investor'_`matrix_class'_`year'.xls, clear firstrow
            rename * share*
            rename shareImmediate_Destination Residency
            qui reshape long share, i(Residency) j(Nationality) string
            qui replace share = share / 100

            * Don't reallocate LUX for CPIS countries
            qui replace share = 1 if "`class'" == "E" & "`investor'" != "USA" & Residency == "LUX" & Nationality == "LUX"
            qui replace share = 0 if "`class'" == "E" & "`investor'" != "USA" & Residency == "LUX" & Nationality != "LUX"
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
        use $cmns1/temp/fund_shares_corrections/tic_cpis_with_funds_adjustment_robustness, clear
        qui replace Position_Residency = 0 if Investor == "EMU" & Issuer == "LUX" & Asset_Class_Code == "E"
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
        save $scratch/new_rescaled_flows_`investor'_`class'_robustness, replace
        gcollapse (sum) Position_Nationality_Full Position_Nationality_TH_Only, by(Year Nationality)
        save $scratch/funds_adjustment_nationality_positions_robustness, replace

        * Merge with original file
        use $cmns1/temp/fund_shares_corrections/tic_cpis_with_funds_adjustment_robustness, clear
        qui replace Position_Residency = 0 if Investor == "EMU" & Issuer == "LUX" & Asset_Class_Code == "E"
        keep if Investor == "`investor'"
        keep if Asset_Class_Code == "`class'"
        keep Year Investor Asset_Class_Code Issuer Position_Residency
        rename Issuer Nationality
        qui mmerge Year Nationality using $scratch/funds_adjustment_nationality_positions_robustness.dta, unmatched(m)
        drop _merge
        gsort -Year -Position_Nationality_TH_Only
        foreach var of varlist *Position* {
            qui replace `var' = `var' / 1e3
        }
        save $scratch/reconstruction_`investor'_`class'_robustness, replace

    }
}

* ---------------------------------------------------------------------------------------------------
* Matrix multiplication: append results
* ---------------------------------------------------------------------------------------------------

* Append all results
cap restore
clear
preserve
foreach investor in "USA" "EMU" "CAN" "GBR" "AUS" "DNK" "NOR" "SWE" "CHE" {

    use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
    qui replace Asset_Class_Code = "E" if Asset_Class_Code == "EF"
    qui levelsof Asset_Class_Code if Investor == "`investor'", local(classes)
    foreach class of local classes {
        restore
        append using $scratch/reconstruction_`investor'_`class'
        preserve
    }
}
rename Nationality Issuer
qui replace Asset_Class_Code = "E" if Asset_Class_Code == "EF"
qui mmerge Year Investor Asset_Class_Code Issuer using $cmns1/temp/fund_shares_corrections/tic_cpis_with_funds_adjustment, ///
    unmatched(m) ukeep(Position_Residency) uname(U_)
replace Position_Residency = U_Position_Residency if missing(Position_Residency)
replace Position_Residency = 0 if missing(Position_Residency)
drop U_Position_Residency
drop _merge
gsort -Year Investor Issuer

* Fund shares addback
qui mmerge Year Investor Asset_Class_Code Issuer using $scratch/th_original_positions.dta, unmatched(m) uname(Orig_)
qui replace Position_Residency = Position_Residency / 1e3 if inlist(Asset_Class_Code, "E", "EF")
qui replace Orig_Position_Residency = Orig_Position_Residency / 1e3 if inlist(Asset_Class_Code, "E", "EF")
gen Funds_Addback = Orig_Position_Residency - Position_Residency
gen Position_Residency_Com = Position_Residency
gen Position_Nationality_TH_Only_Com = Position_Nationality_TH_Only
qui replace Position_Residency = Position_Residency + Funds_Addback if ~missing(Funds_Addback) & inlist(Asset_Class_Code, "E", "EF")
qui replace Position_Nationality_TH_Only = Position_Nationality_TH_Only + Funds_Addback if ~missing(Funds_Addback) & inlist(Asset_Class_Code, "E", "EF")
qui replace Position_Nationality_Full = Position_Nationality_Full + Funds_Addback if ~missing(Funds_Addback) & inlist(Asset_Class_Code, "E", "EF")
drop _merge Orig_Position_Residency Funds_Addback

order Year Investor Asset_Class_Code Issuer Position_Residency Position_Nationality_TH_Only Position_Nationality_Full

* Save data
save $cmns1/holdings_based_restatements/nationality_estimates, replace
export excel using $cmns1/holdings_based_restatements/nationality_estimates.xls, firstrow(variables) replace

* ---------------------------------------------------------------------------------------------------
* Clean restatements outcome file
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates, clear
drop Position_Nationality_TH_Only_Com
rename Position_Residency_Com Estimated_Common_Equity

* TH indicators
gen TH = 0
qui replace TH = 1 if inlist(Issuer, $tax_haven_1)
qui replace TH = 1 if inlist(Issuer, $tax_haven_2)
qui replace TH = 1 if inlist(Issuer, $tax_haven_3)
qui replace TH = 1 if inlist(Issuer, $tax_haven_4)
qui replace TH = 1 if inlist(Issuer, $tax_haven_5)
qui replace TH = 1 if inlist(Issuer, $tax_haven_6)
qui replace TH = 1 if inlist(Issuer, $tax_haven_7)
qui replace TH = 1 if inlist(Issuer, $tax_haven_8)
replace Estimated_Common_Equity = . if ~inlist(Asset_Class_Code, "E", "EF")
replace Estimated_Common_Equity = . if TH == 0
replace Estimated_Common_Equity = . if Investor == "USA"
drop TH

qui mmerge Investor using $raw/Macro/Concordances/country_names.dta, umatch(ISO_Country_Code) unmatched(m)
rename Country_Name Investor_Name
replace Investor_Name = "European Monetary Union" if Investor == "EMU"
drop _merge

qui mmerge Issuer using $raw/Macro/Concordances/country_names.dta, umatch(ISO_Country_Code) unmatched(m)
rename Country_Name Issuer_Name
drop _merge

gen Asset_Class = "All Bonds" if Asset_Class_Code == "B"
replace Asset_Class = "Common Equity and Fund Shares" if Asset_Class_Code == "E" & Investor != "USA"
replace Asset_Class_Code = "EF" if Asset_Class_Code == "E" & Investor != "USA"
replace Asset_Class = "Common Equity" if Asset_Class_Code == "E" & Investor == "USA"
replace Asset_Class = "Corporate Bonds" if Asset_Class_Code == "BC"
replace Asset_Class = "Government Bonds" if Asset_Class_Code == "BG"
replace Asset_Class = "Asset-Backed Securities" if Asset_Class_Code == "BSF"
assert ~missing(Asset_Class)

order Year Investor_Name Investor Asset_Class Asset_Class_Code Issuer_Name Issuer Position_Residency ///
    Position_Nationality_TH_Only Position_Nationality_Full Estimated_Common_Equity

foreach var of varlist * {
    label var `var' ""
}

foreach var of varlist Position_* Estimated_Common_Equity {
    replace `var' = `var' * 1e3
}

gsort Year Investor Asset_Class_Code Issuer
format %30s Investor_Name Issuer_Name

replace Estimated_Common_Equity = 0 if Issuer == "LUX" & Asset_Class_Code == "EF"

drop if Investor == "NOR" & Year < 2014

* Save data
save $cmns1/holdings_based_restatements/Country_Portfolios_Nationality, replace
export excel using $cmns1/holdings_based_restatements/Country_Portfolios_Nationality.xls, firstrow(variables) replace

* ---------------------------------------------------------------------------------------------------
* Matrix multiplication: append results, robustness
* ---------------------------------------------------------------------------------------------------

* Append all results
cap restore
clear
preserve
foreach investor in "USA" "EMU" "CAN" "GBR" "AUS" "DNK" "NOR" "SWE" "CHE" {

    use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
    qui replace Asset_Class_Code = "E" if Asset_Class_Code == "EF"
    qui keep if Asset_Class_Code == "E"
    qui levelsof Asset_Class_Code if Investor == "`investor'", local(classes)
    foreach class of local classes {
        restore
        append using $scratch/reconstruction_`investor'_`class'_robustness
        preserve
    }
}
rename Nationality Issuer
qui replace Asset_Class_Code = "E" if Asset_Class_Code == "EF"
qui mmerge Year Investor Asset_Class_Code Issuer using $cmns1/temp/fund_shares_corrections/tic_cpis_with_funds_adjustment_robustness, ///
    unmatched(m) ukeep(Position_Residency) uname(U_)
replace Position_Residency = U_Position_Residency // if missing(Position_Residency)
replace Position_Residency = 0 if missing(Position_Residency)
drop U_Position_Residency
drop _merge
gsort -Year Investor Issuer

* Fund shares addback
qui mmerge Year Investor Asset_Class_Code Issuer using $scratch/th_original_positions.dta, unmatched(m) uname(Orig_)
qui replace Position_Residency = Position_Residency / 1e3 if inlist(Asset_Class_Code, "E", "EF")
qui replace Orig_Position_Residency = Orig_Position_Residency / 1e3 if inlist(Asset_Class_Code, "E", "EF")
gen Funds_Addback = Orig_Position_Residency - Position_Residency
gen Position_Residency_Com = Position_Residency
gen Position_Nationality_TH_Only_Com = Position_Nationality_TH_Only
qui replace Position_Residency = Position_Residency + Funds_Addback if ~missing(Funds_Addback) & inlist(Asset_Class_Code, "E", "EF")
qui replace Position_Nationality_TH_Only = Position_Nationality_TH_Only + Funds_Addback if ~missing(Funds_Addback) & inlist(Asset_Class_Code, "E", "EF")
qui replace Position_Nationality_Full = Position_Nationality_Full + Funds_Addback if ~missing(Funds_Addback) & inlist(Asset_Class_Code, "E", "EF")
drop _merge Orig_Position_Residency Funds_Addback

order Year Investor Asset_Class_Code Issuer Position_Residency Position_Nationality_TH_Only Position_Nationality_Full

* Save data
save $cmns1/holdings_based_restatements/nationality_estimates_funds_robustness, replace

* All equity data from TIC
use $cmns1/holdings_master/TIC-Disaggregated-Clean-Main.dta if year == 2017, clear
keep iso total_equity
rename iso Issuer
rename total_equity TIC_All
save $scratch/tic_all_equity, replace

* ---------------------------------------------------------------------------------------------------
* Append source-destination data
* ---------------------------------------------------------------------------------------------------

clear
foreach investor in "USA" "EMU" "CAN" "GBR" "AUS" "DNK" "NOR" "SWE" "CHE" {
    foreach class in "E" "BC" "BG" "BSF" "B" "EF" {
        cap append using $scratch/new_rescaled_flows_`investor'_`class'
    }
}
rename share Reallocation_Share_FN
rename share_th_only Reallocation_Share_TH_Only
drop th
drop Position_Residency Reallocation_Share_*
order Year Asset_Class_Code Investor Residency Nationality
drop if Investor == "NOR" & Year < 2014
drop if Investor == "AUS" & Year < 2017
save $cmns1/holdings_based_restatements/source_destination_data, replace

use $cmns1/holdings_based_restatements/source_destination_data.dta , clear
foreach var of varlist _all {
    local new_name = lower("`var'")
    rename `var' `new_name'
}
export delimited $cmns1/holdings_based_restatements/source_destination_data_lower.csv, nolab replace

* Data for network flows chart: USA
use $cmns1/holdings_based_restatements/source_destination_data.dta, clear
keep if Investor == "USA" & Asset_Class_Code == "BC" & Year == 2017
keep if inlist(Residency, $tax_haven_1) | inlist(Residency, $tax_haven_2) | inlist(Residency, $tax_haven_3) | ///
    inlist(Residency, $tax_haven_4) | inlist(Residency, $tax_haven_5) | inlist(Residency, $tax_haven_6) | ///
    inlist(Residency, $tax_haven_7) | inlist(Residency, $tax_haven_8)
keep if inlist(Nationality, "BRA", "CHN", "IND", "RUS", "ZAF")
keep Residency Nationality Position_Nationality_TH_Only
qui replace Residency = "Other Tax Havens" if ~inlist(Residency, "VGB", "CYM", "IRL", "LUX", "NLD")
collapse (sum) Position_Nationality_TH_Only, by(Residency Nationality)
replace Residency = "British Virgin Islands" if Residency == "VGB"
replace Residency = "Cayman Islands" if Residency == "CYM"
replace Residency = "Ireland" if Residency == "IRL"
replace Residency = "Luxembourg" if Residency == "LUX"
replace Residency = "Netherlands" if Residency == "NLD"
replace Nationality = "Brazil" if Nationality == "BRA"
replace Nationality = "China" if Nationality == "CHN"
replace Nationality = "India" if Nationality == "IND"
replace Nationality = "Russia" if Nationality == "RUS"
replace Nationality = "South Africa" if Nationality == "ZAF"
rename Residency Conduit
rename Nationality Destination
rename Position_Nationality_TH_Only freq
export excel $cmns1/temp/raw_data_for_network_plot_usa.xlsx, replace firstrow(variables)

* Data for network flows chart: EMU
use $cmns1/holdings_based_restatements/source_destination_data.dta, clear
keep if Investor == "EMU" & Asset_Class_Code == "B" & Year == 2017
keep if inlist(Residency, $tax_haven_1) | inlist(Residency, $tax_haven_2) | inlist(Residency, $tax_haven_3) | ///
    inlist(Residency, $tax_haven_4) | inlist(Residency, $tax_haven_5) | inlist(Residency, $tax_haven_6) | ///
    inlist(Residency, $tax_haven_7) | inlist(Residency, $tax_haven_8)
keep if inlist(Nationality, "BRA", "CHN", "IND", "RUS", "ZAF")
keep Residency Nationality Position_Nationality_TH_Only
qui replace Residency = "Other Tax Havens" if ~inlist(Residency, "VGB", "CYM", "IRL", "LUX", "NLD")
collapse (sum) Position_Nationality_TH_Only, by(Residency Nationality)
replace Residency = "British Virgin Islands" if Residency == "VGB"
replace Residency = "Cayman Islands" if Residency == "CYM"
replace Residency = "Ireland" if Residency == "IRL"
replace Residency = "Luxembourg" if Residency == "LUX"
replace Residency = "Netherlands" if Residency == "NLD"
replace Nationality = "Brazil" if Nationality == "BRA"
replace Nationality = "China" if Nationality == "CHN"
replace Nationality = "India" if Nationality == "IND"
replace Nationality = "Russia" if Nationality == "RUS"
replace Nationality = "South Africa" if Nationality == "ZAF"
rename Residency Conduit
rename Nationality Destination
rename Position_Nationality_TH_Only freq
export excel $cmns1/temp/raw_data_for_network_plot_emu.xlsx, replace firstrow(variables)

log close
