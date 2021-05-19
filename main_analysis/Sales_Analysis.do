* ---------------------------------------------------------------------------------------------------
* Sales_Analysis: Produces analysis of restatements based on geography of sales (GeoRev), together 
* with accompanying figures and tables
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Sales_Analysis, replace

global treatment_list "baseline threshA threshB threshC threshD threshE"

* ---------------------------------------------------------------------------------------------------
* Sales: firm-level table (Table A.14 in the paper)
* ---------------------------------------------------------------------------------------------------

* creating country of max sales
use $sales/full_merge_baseline.dta, clear
keep if year==2017
collapse (sum) marketvalue_usd (firstnm) cgs_domicile country_bg factset_entity_id est*, by(issuer_num name_short asset_class)

* rounding to avoid numerical issues
foreach x of varlist est_pct* {
    qui gen temp`x'=round(`x',.000001)
    drop `x'
    qui gen `x'=temp`x'
    drop temp`x'
}

* find maximum sales share
egen maxpct=rowmax(est_pct*)
replace maxpct=round(maxpct,.000001)
gen sales_maxco=""
foreach x of varlist est_pct* {
    local temp=subinstr("`x'","est_pct","",.)
    qui replace sales_maxco="`temp'" if `x'==maxpct
}
drop est_pct* 
save $sales/georev_sales_max_2017.dta, replace

* sort data by market values
use $sales/georev_sales_max_2017.dta, clear
order name_short market cgs_domicile country_bg sales_maxco maxpct
gsort -market
order name_short market cgs_domicile country_bg sales_maxco maxpct
replace maxpct=round(maxpct*100,.1)
save $sales/firm_table_data, replace

* firm-level comparison for the case max sales = nationality
use $sales/firm_table_data, clear
keep if cgs_dom~=country_bg & country_bg==sales_maxco
drop if name==""
drop if factset_entity_id==""
qui keep if asset_class == "E"
gen n=_n
tostring n, replace
replace name_short=n+". "+ name_short
export excel name_short cgs_domicile country_bg sales_maxco maxpct using ///
    $cmns1/tables/georev_nationality_corresponds_to_sales.xls if _n<=10 & name~="", firstrow(variables) replace

* firm-level comparison for the case max sales = residency
use $sales/firm_table_data, clear
keep if cgs_dom~=country_bg & cgs_dom==sales_maxco
drop if name==""
drop if factset_entity_id==""
qui keep if asset_class == "E"
gen n=_n
tostring n, replace
replace name_short=n+". "+ name_short
export excel name_short cgs_domicile country_bg sales_maxco maxpct using ///
    $cmns1/tables/georev_residency_corresponds_to_sales.xls if _n<=10 & name~="", firstrow(variables) replace 

* ---------------------------------------------------------------------------------------------------
* Sales-based restatement: process matrices
* ---------------------------------------------------------------------------------------------------

foreach treatment of global treatment_list {

    if "`treatment'" == "baseline" {
        global inv_list "AUS CAN CHE DNK EMU GBR NOR SWE USA"
    }
    else {
        global inv_list "USA EMU"
    }

    foreach investor of global inv_list {
    forval year = 2007/2017 {
    foreach class in "E" "B" "BC" {
    
        di "Processing `investor', `class', `year'"
        use $cmns1/temp/sales/matrices/`investor'_`class'_`year'_sales_`treatment'.dta, clear
        rename * Share*
        rename Sharecgs Issuer
        qui reshape long Share, i(Issuer) j(Nationality) string
        qui replace Share = 0 if missing(Share)
        qui replace Share = Share / 100
        gen Year = `year'
        gen Investor = "`investor'"
        gen Asset_Class_Code = "`class'"
        save $scratch/sales_matrix_long_`investor'_`class'_`year'_`treatment', replace
        
    }
    }
    }

    clear
    foreach investor of global inv_list {
    forval year = 2007/2017 {
    foreach class in "E" "B" "BC" {
        append using $scratch/sales_matrix_long_`investor'_`class'_`year'_`treatment'
    }
    }
    }
    save $scratch/sales_matrices_`treatment', replace

}

* ---------------------------------------------------------------------------------------------------
* Sales-based restatement: bonds
* ---------------------------------------------------------------------------------------------------

* Matrix multiplication: Iterate over investors and asset classes
foreach treatment of global treatment_list {

    if "`treatment'" == "baseline" {
        global inv_list "AUS CAN CHE DNK EMU GBR NOR SWE USA"
    }
    else {
        global inv_list "USA EMU"
    }

    foreach investor of global inv_list {

        use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
        qui drop if Investor == "USA" & Asset_Class_Code == "B"
        qui drop if inlist(Asset_Class_Code, "E", "EF", "BG", "BSF")
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

            * Import matrices
            use $scratch/sales_matrices_`treatment', clear
            qui keep if Investor == "`investor'"
            qui keep if Asset_Class_Code == "`class'"
            rename Issuer Residency
            rename Share share
            keep Year Residency Nationality share
            sort Year Residency Nationality
            save $scratch/_mat_long_sales, replace

            * Reconstruct positions
            use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
            keep if Investor == "`investor'"
            keep if Asset_Class_Code == "`class'"
            keep Year Investor Asset_Class_Code Issuer Position_Residency
            rename Issuer Residency
            qui mmerge Year Residency using $scratch/_mat_long_sales, unmatched(m)
            qui replace share = 1 if _merge == 1
            qui replace Nationality = Residency if _merge == 1
            drop _merge
            gen Position_Sales = share * Position_Residency
            gcollapse (sum) Position_Sales, by(Year Nationality)
            save $scratch/_sales_positions, replace

            * Merge with original file
            use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
            keep if Investor == "`investor'"
            keep if Asset_Class_Code == "`class'"
            keep Year Investor Asset_Class_Code Issuer Position_Residency
            rename Issuer Nationality
            qui mmerge Year Nationality using $scratch/_sales_positions.dta, unmatched(m)
            drop _merge
            gsort -Year -Position_Sales
            foreach var of varlist *Position* {
                qui replace `var' = `var' / 1e3
            }
            save $scratch/sales_positions_`investor'_`class'_`treatment', replace

        }
    }
}

* ---------------------------------------------------------------------------------------------------
* Sales-based restatement: equities
* ---------------------------------------------------------------------------------------------------

* Matrix multiplication: Iterate over investors and asset classes
foreach treatment of global treatment_list {

    if "`treatment'" == "baseline" {
        global inv_list "AUS CAN CHE DNK EMU GBR NOR SWE USA"
    }
    else {
        global inv_list "USA EMU"
    }

    foreach investor of global inv_list {

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
            use $scratch/sales_matrices_`treatment', clear
            qui keep if Investor == "`investor'"
            qui keep if Asset_Class_Code == "`class'"
            rename Issuer Residency
            rename Share share
            keep Year Residency Nationality share
            sort Year Residency Nationality
            save $scratch/_mat_long_sales, replace

            * Reconstruct positions
            use $cmns1/temp/fund_shares_corrections/tic_cpis_with_funds_adjustment, clear
            qui replace Position_Residency = 0 if Investor == "EMU" & Issuer == "LUX" & Asset_Class_Code == "E"
            keep if Investor == "`investor'"
            keep if Asset_Class_Code == "`class'"
            keep Year Investor Asset_Class_Code Issuer Position_Residency
            rename Issuer Residency
            qui mmerge Year Residency using $scratch/_mat_long_sales, unmatched(m)
            qui replace share = 1 if _merge == 1
            qui replace Nationality = Residency if _merge == 1
            drop _merge
            gen Position_Sales = share * Position_Residency
            gcollapse (sum) Position_Sales, by(Year Nationality)
            save $scratch/_sales_positions, replace

            * Merge with original file
            use $cmns1/temp/fund_shares_corrections/tic_cpis_with_funds_adjustment, clear
            qui replace Position_Residency = 0 if Investor == "EMU" & Issuer == "LUX" & Asset_Class_Code == "E"
            keep if Investor == "`investor'"
            keep if Asset_Class_Code == "`class'"
            keep Year Investor Asset_Class_Code Issuer
            rename Issuer Nationality
            qui mmerge Year Nationality using $scratch/_sales_positions.dta, unmatched(m)
            drop _merge
            gsort -Year -Position_Sales
            foreach var of varlist *Position* {
                qui replace `var' = `var' / 1e3
            }
            save $scratch/sales_positions_`investor'_`class'_`treatment', replace

        }
    }
}

* ---------------------------------------------------------------------------------------------------
* Sales-based restatement: append
* ---------------------------------------------------------------------------------------------------

* Append all results
foreach treatment of global treatment_list {

    if "`treatment'" == "baseline" {
        global inv_list "AUS CAN CHE DNK EMU GBR NOR SWE USA"
    }
    else {
        global inv_list "USA EMU"
    }

    cap restore
    clear
    preserve
    foreach investor of global inv_list {

        use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
        qui replace Asset_Class_Code = "E" if Asset_Class_Code == "EF"
        qui drop if inlist(Asset_Class_Code, "BG", "BSF")
        qui levelsof Asset_Class_Code if Investor == "`investor'", local(classes)
        foreach class of local classes {
            restore
            append using $scratch/sales_positions_`investor'_`class'_`treatment'
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
    gen Position_Sales_Com = Position_Sales
    qui replace Position_Residency = Position_Residency + Funds_Addback if ~missing(Funds_Addback) & inlist(Asset_Class_Code, "E", "EF")
    qui replace Position_Sales = Position_Sales + Funds_Addback if ~missing(Funds_Addback) & inlist(Asset_Class_Code, "E", "EF")
    drop _merge Orig_Position_Residency Funds_Addback

    order Year Investor Asset_Class_Code Issuer Position_Residency Position_Sales
    rename Position_Residency_Com Position_Residency_Common_Eq
    drop Position_Sales_Com
    replace Position_Residency_Com = . if Asset_Class_Code != "E"

    * Save data
    save $cmns1/alternative_restatements/sales_restatements_`treatment', replace

}

* ---------------------------------------------------------------------------------------------------
* Figure: China barchart (Figure A.8 in the paper)
* ---------------------------------------------------------------------------------------------------

foreach treatment of global treatment_list {

    use $cmns1/alternative_restatements/sales_restatements_`treatment', clear
    keep if Year == 2017 & Asset_Class_Code == "E"
    drop Position_Residency_Common_Eq
    replace Issuer="EMU" if inlist(Issuer,$eu1)==1 |  inlist(Issuer,$eu1)==1 |  inlist(Issuer,$eu3)==1
    bys Investor Year: egen total_ext_sales=sum(Position_Sales) if Investor~=Issuer
    qui gen pct_ext_sales = Position_Sales/total_ext_sales
    keep if Issuer == "CHN"

    graph bar (asis) pct_ext_sales, over(Investor) ///
        graphregion(color(white)) nofill xsize(6) b1title(" " "Investing Country") ///
        ytitle("Share of External Equity Portfolio in China" " ") ///
        bar(1, color(blue) fintensity(inten40)) ///
        ylab(0(.05).15, gmin gmax)

    if "`treatment'" == "baseline" {
        graph export $cmns1/graphs/china_extsales_investment_equities.pdf, as(pdf) replace
    }
    else {
        graph export $cmns1/graphs/china_extsales_investment_equities_`treatment'.pdf, as(pdf) replace
    }

}

* ---------------------------------------------------------------------------------------------------
* Figures: China exposures, time series (Figure 11 in the paper)
* ---------------------------------------------------------------------------------------------------

foreach treatment of global treatment_list {

    use $cmns1/alternative_restatements/sales_restatements_`treatment', clear
    keep if Asset_Class_Code == "E"
    qui mmerge Year Investor Asset_Class_Code Issuer using ///
        $cmns1/holdings_based_restatements/nationality_estimates.dta, unmatched(m)
    drop _merge
    replace Issuer="EMU" if inlist(Issuer,$eu1)==1 |  inlist(Issuer,$eu1)==1 |  inlist(Issuer,$eu3)==1

    cap drop Position_Residency_Com*
    bysort Investor Year: egen total=sum(Position_Sales)
    bysort Investor Year: egen total_ext_sales=sum(Position_Sales) if Investor~=Issuer
    bysort Investor Year: egen total_ext_res=sum(Position_Res) if Investor~=Issuer
    bysort Investor Year: egen total_ext_nat=sum(Position_Nationality_TH_Only) if Investor~=Issuer

    qui gen pct_ext_sales = Position_Sales/total_ext_sales
    qui gen pct_ext_res = Position_Re/total_ext_res
    qui gen pct_ext_nat = Position_Nationality_TH_Only/total_ext_nat
    qui gen pct_sales = Position_Sales/total
    qui gen pct_res = Position_Re/total
    qui gen pct_nat = Position_Nationality_TH_Only/total

    twoway (line pct_ext_res Year, lwidth(medthick) lcolor(red)) ///
        (line pct_ext_nat Year, lwidth(medthick) lcolor(blue) lpattern(longdash)) ///
        (line pct_ext_sales Year,  ytitle("Share of External U.S. Equity Investment" " ") xtitle("") ///
        lwidth(medthick) lcolor(green) lpattern(shortdash)) if Investor=="USA" & Issuer=="CHN", ///
        graphregion(color(white)) legend(order(1 "Residency" 2 "Nationality" 3 "Sales") rows(1)) xlabel(2007(2)2017) ///
        ylab(0(.05).15, gmax)

    if "`treatment'" == "baseline" {
        graph export $cmns1/graphs/china_time_series_external_usa.pdf, as(pdf) replace
    }
    else {
        graph export $cmns1/graphs/china_time_series_external_usa_`treatment'.pdf, as(pdf) replace
    }

    twoway (line pct_ext_res Year, lwidth(medthick) lcolor(red)) ///
        (line pct_ext_nat Year, lwidth(medthick) lcolor(blue) lpattern(longdash)) ///
        (line pct_ext_sales Year,  ytitle("Share of External EMU Equity Investment" " ") xtitle("") ///
        lwidth(medthick) lcolor(green) lpattern(shortdash)) if Investor=="EMU" & Issuer=="CHN", ///
        graphregion(color(white)) legend(order(1 "Residency" 2 "Nationality" 3 "Sales") rows(1)) xlabel(2007(2)2017) ///
        ylab(0(.05).15, gmax)

    if "`treatment'" == "baseline" {
        graph export $cmns1/graphs/china_time_series_external_emu.pdf, as(pdf) replace
    }
    else {
        graph export $cmns1/graphs/china_time_series_external_emu_`treatment'.pdf, as(pdf) replace
    }

}

log close
