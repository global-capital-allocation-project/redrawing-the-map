* ---------------------------------------------------------------------------------------------------
* Credit_Parent_Analysis: Produces the guarantor-based restatements
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Credit_Parent_Analysis, replace

* ---------------------------------------------------------------------------------------------------
* Mapping: factset entity ID to primary equity CUSIP
* ---------------------------------------------------------------------------------------------------

* Primary listings dta
use fsym_id fsym_primary_equity_id fsym_primary_listing_id using $raw/Factset/fds_stata/sym_coverage.dta, clear
gsort fsym_id
by fsym_id: keep if _n == 1
drop if missing(fsym_id)
tempfile primary_listings
save `primary_listings', replace

* Mapping from Factset entity ID to CUSIP9 codes
use $raw/Factset/fds_stata/sym_sec_entity.dta, clear
mmerge fsym_id using `primary_listings', unmatched(m)
drop _merge
mmerge fsym_primary_equity_id using $raw/Factset/fds_stata/sym_cusip.dta, unmatched(m) umatch(fsym_id) uname(pe_)
mmerge fsym_primary_listing_id using $raw/Factset/fds_stata/sym_cusip.dta, unmatched(m) umatch(fsym_id) uname(pl_)
drop _merge
replace pe_cusip = pl_cusip if missing(pe_cusip)
keep factset_entity_id pe_cusip
drop if missing(pe_cusip)
duplicates drop
gen issuer_number = substr(pe_cusip, 1, 6)
mmerge issuer_number using $cmns1/country_master/cmns_aggregation.dta, unmatched(m)
rename issuer_number pe_issuer_number
rename issuer_name pe_issuer_name
rename cgs_domicile pe_country
replace pe_country = country_bg if pe_issuer_number == cusip6_up_bg
drop _merge *source* cusip6_up_bg country_bg issuer_name_up
save $cmns1/temp/factset_entity_id_to_primary_cusip9, replace

* ---------------------------------------------------------------------------------------------------
* Credit parent aggregation
* ---------------------------------------------------------------------------------------------------

* Credit parent data from Factset
use $raw/Factset/fds_stata/ent_entity_affiliates.dta, clear
keep if aff_type_code==6
drop aff_type_code pct_held
rename factset_entity_id factset_cp_id
rename factset_affiliated_entity_id factset_entity_id 
save $cmns1/temp/factset_credit_parent_aggregation.dta, replace

* CUSIP-based version
use $cmns1/temp/factset_credit_parent_aggregation.dta, clear
mmerge factset_entity_id using $cmns1/temp/factset_entity_id_to_primary_cusip6.dta, unmatched(m)
drop if missing(issuer_number)
drop _merge universe_type
drop factset_entity_id
mmerge factset_cp_id using $cmns1/temp/factset_entity_id_to_primary_cusip6.dta, unmatched(m) ///
    uname(cp_) umatch(factset_entity_id)
drop cp_universe_type _merge factset_cp_id
drop if missing(cp_issuer_number)
duplicates drop
gen priority = 1
replace priority = 0 if issuer_number != cp_issuer_number
gsort issuer_number priority
by issuer_number: keep if _n == 1
drop priority
save $cmns1/temp/factset_cp_aggregation_cusip, replace

* Merge with holdings
use $cmns1/holdings_master/mns_security_summary.dta, clear
drop tax_haven*
mmerge issuer_number using $cmns1/temp/factset_cp_aggregation_cusip.dta, unmatched(m)
drop _merge
mmerge cp_issuer_number using $cmns1/country_master/cmns_aggregation.dta, unmatched(m) ///
    umatch(issuer_number) uname(cp_)
cap drop _merge *source*
cap drop cp_cusip6_up_bg cp_country_bg cp_issuer_name_up
save $cmns1/temp/mns_security_summary_credit_parent, replace

* Merge in fsym and then credit parent
use $cmns1/holdings_master/mns_security_summary.dta, clear
mmerge cusip using $cmns1/security_master/gcap_security_master_cusip.dta, unmatched(m)
drop asset_class1 asset_class2 asset_class3 class_code* maturity_date issuance_date coupon_percent _merge
drop tax_haven*
mmerge fsym_id using $raw/Factset/fds_stata/sym_sec_entity.dta, unmatched(m)
drop _merge
keep if year == 2017
count
count if missing(factset_entity_id)
mmerge factset_entity_id using $cmns1/temp/factset_credit_parent_aggregation.dta, unmatched(m)
drop _merge
mmerge factset_cp_id using $raw/Factset/fds_stata/ent_entity_coverage.dta, umatch(factset_entity_id) uname(cp_) unmatched(m)
drop _merge
keep year cusip DomicileCountryId issuer_number asset_class currency_id issuer_name cgs_domicile cusip6_up_bg country_bg issuer_name_up marketvalue_usd isin figi security_name currency fsym_id factset_entity_id factset_cp_id cp_entity_name cp_entity_proper_name cp_iso_country cp_iso_country_incorp
mmerge cp_iso_country using $raw/Macro/Concordances/iso2_iso3.dta, unmatched(m) umatch(iso2)
replace cp_iso_country = iso3
drop iso3 _merge
mmerge cp_iso_country_incorp using $raw/Macro/Concordances/iso2_iso3.dta, unmatched(m) umatch(iso2)
replace cp_iso_country_incorp = iso3
drop iso3 _merge
save $cmns1/temp/mns_security_summary_cp, replace

* Establish cross-country changes
use $cmns1/temp/mns_security_summary_cp, clear
mmerge factset_cp_id using $cmns1/temp/factset_entity_id_to_primary_cusip9.dta, unmatched(m) umatch(factset_entity_id)
drop if asset_class == "Equity"
gsort -marketvalue_usd
drop _merge
gen cp_country_reassign = 0
replace cp_country_reassign = 1 if ~missing(cp_iso_country_incorp) & country_bg != cp_iso_country_incorp
gen cp_country_assigment = cp_iso_country_incorp

* Changes at issuer level
keep if cp_country_reassign == 1
gcollapse (sum) marketvalue_usd, by(issuer_number cgs_domicile country_bg ///
    cp_iso_country cp_iso_country_incorp issuer_name issuer_name_up ///
    cp_entity_name pe_country asset_class cusip6_up_bg factset_entity_id ///
    factset_cp_id pe_issuer_number pe_issuer_name cp_country_reassign cp_country_assigment) 
gsort -marketvalue_usd
qui replace asset_class = subinstr(asset_class, " - ", ", ", .)
keep if inlist(asset_class, "Bonds, Structured Finance", "Bonds, Corporate")
drop if inlist(issuer_number, "706451", "F43596", "761735", "G2R74B", "771196", "L4155J") // Quasi-sovereigns and corner cases
order cgs_domicile country_bg cp_iso_country cp_iso_country_incorp pe_country issuer_name issuer_name_up cp_entity_name 
drop if country_bg == pe_country 
drop if country_bg == cp_iso_country & issuer_name_up == cp_entity_name
replace marketvalue_usd = marketvalue_usd / 1e9
gsort -marketvalue_usd
drop cp_country* factset_entity_id factset_cp_id pe_* cusip6_up_bg
order issuer_name cgs_domicile issuer_name_up country_bg cp_entity_name cp_iso_country_incorp cp_iso_country
save $cmns1/temp/credit_parent_changes_2017, replace

* ---------------------------------------------------------------------------------------------------
* Guarantor-based matrices
* ---------------------------------------------------------------------------------------------------

* Standard matrices
foreach investor in "USA" "EMU" "CAN" "GBR" "AUS" "DNK" "NOR" "SWE" "CHE" {

    use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
    qui drop if Investor == "USA" & Asset_Class_Code == "B"
    qui levelsof Asset_Class_Code if Investor == "`investor'", local(classes)
    foreach class of local classes {
        
        * Locals
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
        forval year = 2017/2017 {
            import excel using $scratch/Reallocation_Matrix_`investor'_`matrix_class'_`year'.xls, clear firstrow
            rename * share*
            rename shareImmediate_Destination Residency
            qui reshape long share, i(Residency) j(Nationality) string
            qui replace share = share / 100
            qui replace share = 1 if "`class'" == "EF" & "`investor'" != "USA" & Residency == "LUX" & Nationality == "LUX"
            qui replace share = 0 if "`class'" == "EF" & "`investor'" != "USA" & Residency == "LUX" & Nationality != "LUX"
            gen Year = `year'
            qui save $scratch/matrix_long_`investor'_`class'_`year', replace
        }
        clear
        forval year = 2017/2017 {
            append using $scratch/matrix_long_`investor'_`class'_`year'
        }
        sort Year Residency
        save $scratch/matrix_long_`investor'_`class'.dta, replace

    }
}

* Establish issuer-level changes
forval year = 2007/2017 {

    * Merge in fsym and then credit parent
    use $cmns1/holdings_master/mns_security_summary.dta, clear
    mmerge cusip using $cmns1/security_master/gcap_security_master_cusip.dta, unmatched(m)
    drop asset_class1 asset_class2 asset_class3 class_code* maturity_date issuance_date coupon_percent _merge
    drop tax_haven*
    mmerge fsym_id using $raw/Factset/fds_stata/sym_sec_entity.dta, unmatched(m)
    drop _merge
    keep if year == `year'
    count
    count if missing(factset_entity_id)
    mmerge factset_entity_id using $cmns1/temp/factset_credit_parent_aggregation.dta, unmatched(m)
    drop _merge
    mmerge factset_cp_id using $raw/Factset/fds_stata/ent_entity_coverage.dta, umatch(factset_entity_id) uname(cp_) unmatched(m)
    drop _merge
    keep year cusip DomicileCountryId issuer_number asset_class currency_id issuer_name cgs_domicile cusip6_up_bg country_bg issuer_name_up marketvalue_usd isin figi security_name currency fsym_id factset_entity_id factset_cp_id cp_entity_name cp_entity_proper_name cp_iso_country cp_iso_country_incorp
    mmerge cp_iso_country using $raw/Macro/Concordances/iso2_iso3.dta, unmatched(m) umatch(iso2)
    replace cp_iso_country = iso3
    drop iso3 _merge
    mmerge cp_iso_country_incorp using $raw/Macro/Concordances/iso2_iso3.dta, unmatched(m) umatch(iso2)
    replace cp_iso_country_incorp = iso3
    drop iso3 _merge
    save $cmns1/temp/mns_security_summary_cp_`year', replace

    * Establish cross-country changes
    use $cmns1/temp/mns_security_summary_cp_`year', clear
    mmerge factset_cp_id using $cmns1/temp/factset_entity_id_to_primary_cusip9.dta, unmatched(m) umatch(factset_entity_id)
    drop if asset_class == "Equity"
    gsort -marketvalue_usd
    drop _merge
    gen cp_country_reassign = 0
    replace cp_country_reassign = 1 if ~missing(cp_iso_country_incorp) & country_bg != cp_iso_country_incorp
    gen cp_country_assigment = cp_iso_country_incorp

    * Changes at issuer level
    keep if cp_country_reassign == 1
    gcollapse (sum) marketvalue_usd, by(issuer_number cgs_domicile country_bg ///
        cp_iso_country cp_iso_country_incorp issuer_name issuer_name_up ///
        cp_entity_name pe_country asset_class cusip6_up_bg factset_entity_id ///
        factset_cp_id pe_issuer_number pe_issuer_name cp_country_reassign cp_country_assigment) 
    gsort -marketvalue_usd
    qui replace asset_class = subinstr(asset_class, " - ", ", ", .)
    keep if inlist(asset_class, "Bonds, Structured Finance", "Bonds, Corporate")
    drop if inlist(issuer_number, "706451", "F43596", "761735", "G2R74B", "771196", "L4155J") // Quasi-sovereigns and corner cases
    order cgs_domicile country_bg cp_iso_country cp_iso_country_incorp pe_country issuer_name issuer_name_up cp_entity_name 
    drop if country_bg == pe_country 
    drop if country_bg == cp_iso_country & issuer_name_up == cp_entity_name
    replace marketvalue_usd = marketvalue_usd / 1e9
    gsort -marketvalue_usd
    drop cp_country* factset_entity_id factset_cp_id pe_* cusip6_up_bg
    order issuer_name cgs_domicile issuer_name_up country_bg cp_entity_name cp_iso_country_incorp cp_iso_country
    save $cmns1/temp/credit_parent_changes_`year', replace 

}

* Matrices
forval year = 2017/2017 {
    use $cmns1/holdings_master/mns_issuer_summary.dta if year == `year', clear
    qui replace asset_class = subinstr(asset_class, " - ", ", ", .)
    mmerge issuer_number asset_class using $cmns1/temp/credit_parent_changes_`year', unmatched(m) uname(u_)
    drop _merge tax_haven*
    replace country_bg = u_cp_iso_country_incorp if ~missing(u_cp_iso_country_incorp)
    replace asset_class = "Bonds" if Domicile != "USA" & strpos(asset_class, "Bonds")
    gcollapse (sum) marketvalue_usd, by(year DomicileCountryId cgs_domicile country_bg asset_class)
    rename cgs_domicile Residency
    rename country_bg Nationality
    rename Domicile Investor
    rename year Year
    drop if asset_class == "Equity"
    drop if asset_class == "Bonds, Sovranational"
    gen Asset_Class_Code = "BC" if asset_class == "Bonds, Corporate"
    replace Asset_Class_Code = "BG" if asset_class == "Bonds, Government"
    replace Asset_Class_Code = "BSF" if asset_class == "Bonds, Structured Finance"
    replace Asset_Class_Code = "B" if asset_class == "Bonds"
    assert ~missing(Asset_Class_Code)
    drop asset_class
    bys Year Investor Residency Asset_Class_Code: egen totVal = total(marketvalue_usd)
    gen share = marketvalue_usd / totVal
    drop marketvalue_usd totVal
    gsort Investor Year Asset_Class_Code Residency
    save $cmns1/temp/credit_parent_reallocation_matrices_long_`year', replace
}

* ---------------------------------------------------------------------------------------------------
* Guarantor-based restatement: bonds
* ---------------------------------------------------------------------------------------------------

* Append matrices
clear
forval year = 2017/2017 {
    append using $cmns1/temp/credit_parent_reallocation_matrices_long_`year'
}
save $scratch/guarantor_matrices, replace

* Matrix multiplication: Iterate over investors and asset classes
foreach investor in "USA" "EMU" "CAN" "GBR" "AUS" "DNK" "NOR" "SWE" "CHE" {

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
        use $scratch/guarantor_matrices, clear
        qui keep if Investor == "`investor'"
        qui keep if Asset_Class_Code == "`class'"
        keep Year Residency Nationality share
        sort Year Residency Nationality
        save $scratch/_mat_long_guarantor, replace

        * Reconstruct positions
        use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
        keep if Investor == "`investor'"
        keep if Asset_Class_Code == "`class'"
        keep Year Investor Asset_Class_Code Issuer Position_Residency
        rename Issuer Residency
        mmerge Year Residency using $scratch/_mat_long_guarantor, unmatched(m)
        qui replace share = 1 if _merge == 1
        qui replace Nationality = Residency if _merge == 1
        drop _merge
        gen share_th_only = share
        gen th = 0
        qui replace th = 1 if inlist(Residency, $tax_haven_1) | inlist(Residency, $tax_haven_2) | inlist(Residency, $tax_haven_3) | inlist(Residency, $tax_haven_4) | inlist(Residency, $tax_haven_5) | inlist(Residency, $tax_haven_6) | inlist(Residency, $tax_haven_7) | inlist(Residency, $tax_haven_8)
        qui replace share_th_only = 1 if th == 0 & Residency == Nationality
        qui replace share_th_only = 0 if th == 0 & Residency != Nationality
        gen Position_Guarantor_Full = share * Position_Residency
        gen Position_Guarantor_THO = share_th_only * Position_Residency
        gcollapse (sum) Position_Guarantor_Full Position_Guarantor_THO, by(Year Nationality)
        save $scratch/_guarantor_positions, replace

        * Merge with original file
        use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
        keep if Investor == "`investor'"
        keep if Asset_Class_Code == "`class'"
        keep Year Investor Asset_Class_Code Issuer Position_Residency
        rename Issuer Nationality
        mmerge Year Nationality using $scratch/_guarantor_positions.dta, unmatched(m)
        drop _merge
        gsort -Year -Position_Guarantor_Full
        foreach var of varlist *Position* {
            qui replace `var' = `var' / 1e3
        }
        save $scratch/guarantor_positions_`investor'_`class', replace

    }
}

* ---------------------------------------------------------------------------------------------------
* Guarantor-based restatement: append
* ---------------------------------------------------------------------------------------------------

* Append all results
cap restore
clear
preserve
foreach investor in "USA" "EMU" "CAN" "GBR" "AUS" "DNK" "NOR" "SWE" "CHE" {

    use $cmns1/temp/country_portfolios_residency_nor_adjusted, clear
    qui replace Asset_Class_Code = "E" if Asset_Class_Code == "EF"
    qui drop if inlist(Asset_Class_Code, "BG", "BSF", "E")
    qui levelsof Asset_Class_Code if Investor == "`investor'", local(classes)
    foreach class of local classes {
        restore
        append using $scratch/guarantor_positions_`investor'_`class'
        preserve
    }
}
rename Nationality Issuer
qui replace Asset_Class_Code = "E" if Asset_Class_Code == "EF"
mmerge Year Investor Asset_Class_Code Issuer using $cmns1/temp/fund_shares_corrections/tic_cpis_with_funds_adjustment, unmatched(m) ukeep(Position_Residency) uname(U_)
replace Position_Residency = U_Position_Residency if missing(Position_Residency)
replace Position_Residency = 0 if missing(Position_Residency)
drop U_Position_Residency
gsort -Year Investor Issuer
drop _merge
order Year Investor Asset_Class_Code Issuer Position_Residency Position_Guarantor_Full

* Save data
save $cmns1/alternative_restatements/guarantor_restatements, replace

log close
