* ---------------------------------------------------------------------------------------------------
* Summary_Data: Produces summaries of the Morningstar holdings data
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Summary_Data, replace

* ---------------------------------------------------------------------------------------------------
* Holdings data summaries: version with aggregated EMU
* ---------------------------------------------------------------------------------------------------

forval year = 2007/2017 {

    di "Processing `year'"

    * Read in raw Morningstar holdings data
    use cusip marketvalue lcu_per_usd currency_id DomicileCountryId mns_class mns_subclass using $raw/morningstar_holdings/HD_`year'_y, clear
    cap drop cusip6 cgs_domicile
    qui gen issuer_number = substr(cusip, 1, 6)

    * Convert positions to USD and collapse
    qui gen marketvalue_usd = marketvalue / lcu_per_usd
    collapse (sum) marketvalue_usd (firstnm) currency_id, by(DomicileCountryId cusip mns_class mns_subclass issuer_number)
    qui drop if cusip == "#N/A N/A" | missing(cusip)
    qui gen asset_class = "Equity" if mns_class == "E"
    qui replace asset_class = "Bonds - Corporate" if mns_class == "B" & ~inlist(mns_subclass, "S", "A", "LS", "SF", "SV")
    qui replace asset_class = "Bonds - Government" if mns_class == "B" & inlist(mns_subclass, "S", "A", "LS")
    qui replace asset_class = "Bonds - Structured Finance" if mns_class == "B" & inlist(mns_subclass, "SF")
    qui replace asset_class = "Bonds - Sovranational" if mns_class == "B" & inlist(mns_subclass, "SV")
    qui drop if missing(asset_class)
    forvalues j = 1(1)3 {
        qui replace DomicileCountryId = "EMU" if inlist(DomicileCountryId,${eu`j'})
    }
    collapse (sum) marketvalue_usd (firstnm) currency_id, by(DomicileCountryId cusip asset_class issuer_number)
    
    * Keep only the countriest that we study
    qui keep if inlist(DomicileCountryId, "USA", "EMU", "GBR", "CAN", "CHE", "AUS") | inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL")

    * Merge in the CMNS aggregation file
    qui mmerge issuer_number using $cmns1/country_master/cmns_aggregation, unmatched(m)
    qui drop if _merge == 1
    qui drop if missing(cgs_domicile) | missing(country_bg)
    cap drop _merge 
    qui drop *source*
    format %35s *name*

    * Merge in information from OpenFIGI
    qui mmerge cusip using $raw/figi/figi_master_compact_cusip_unique, unmatched(m)
    cap drop _merge
    qui drop marketsector
    qui drop name securitydescription
    qui drop isin
    qui drop securitytype2 cusip6 
    cap drop _merge

    * Tax haven indicators
    qui gen tax_haven = 0
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_1)
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_2)
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_3)
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_4)
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_5)
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_6)
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_7)
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_8)
    order marketvalue_usd, last

    save $cmns1/temp/mns_sec_summary_`year', replace

}

* Append all the years
clear
gen year = .
forval year = 2007/2017 {
    append using $cmns1/temp/mns_sec_summary_`year'
    replace year = `year' if missing(year)
}
sort year DomicileCountryId cusip
format %35s *name*
drop figi
drop if marketvalue_usd == 0
compress

* Summary data at security level
mmerge cusip using $raw/figi/figi_master_compact_cusip_unique.dta, unmatched(m) uname(f_)
replace asset_class = "Bonds - Structured Finance" if asset_class == "Bonds - Corporate" & f_marketsector == "Mtge"
drop f_* _merge
save $cmns1/holdings_master/mns_security_summary, replace

* Summary data at issuer level
use $cmns1/holdings_master/mns_security_summary, clear
gcollapse (sum) marketvalue_usd, by(year DomicileCountryId issuer_number asset_class issuer_name cgs_domicile cusip6_up_bg country_bg issuer_name_up tax_haven)
save $cmns1/holdings_master/mns_issuer_summary, replace

* ---------------------------------------------------------------------------------------------------
* Holdings data summaries: version with disaggregated EMU
* ---------------------------------------------------------------------------------------------------

forval year = 2007/2017 {

    di "Processing `year'"

    * Read in raw Morningstar holdings data
    use cusip marketvalue lcu_per_usd currency_id DomicileCountryId mns_class mns_subclass using $raw/morningstar_holdings/HD_`year'_y, clear
    cap drop cusip6 cgs_domicile
    qui gen issuer_number = substr(cusip, 1, 6)

    * Convert positions to USD and collapse
    qui gen marketvalue_usd = marketvalue / lcu_per_usd
    collapse (sum) marketvalue_usd (firstnm) currency_id, by(DomicileCountryId cusip mns_class mns_subclass issuer_number)
    qui drop if cusip == "#N/A N/A" | missing(cusip)
    qui gen asset_class = "Equity" if mns_class == "E"
    qui replace asset_class = "Bonds - Corporate" if mns_class == "B" & ~inlist(mns_subclass, "S", "A", "LS", "SF", "SV")
    qui replace asset_class = "Bonds - Government" if mns_class == "B" & inlist(mns_subclass, "S", "A", "LS")
    qui replace asset_class = "Bonds - Structured Finance" if mns_class == "B" & inlist(mns_subclass, "SF")
    qui replace asset_class = "Bonds - Sovranational" if mns_class == "B" & inlist(mns_subclass, "SV")
    qui drop if missing(asset_class)
    collapse (sum) marketvalue_usd (firstnm) currency_id, by(DomicileCountryId cusip asset_class issuer_number)

    * Keep only the countriest that we study
    qui keep if inlist(DomicileCountryId, "USA", "GBR", "CAN", "CHE", "AUS") | ///
        inlist(DomicileCountryId, "SWE", "DNK", "NOR", "NZL") | ///
        inlist(DomicileCountryId, $eu1) | ///
        inlist(DomicileCountryId, $eu2) | ///
        inlist(DomicileCountryId, $eu3)

    * Merge in the CMNS aggregation file
    qui mmerge issuer_number using $cmns1/country_master/cmns_aggregation, unmatched(m)
    qui drop if _merge == 1
    qui drop if missing(cgs_domicile) | missing(country_bg)
    cap drop _merge 
    qui drop *source*
    format %35s *name*

    * Merge in information from OpenFIGI
    qui mmerge cusip using $raw/figi/figi_master_compact_cusip_unique, unmatched(m)
    cap drop _merge
    qui drop marketsector
    qui drop name securitydescription
    qui drop isin
    qui drop securitytype2 cusip6 
    cap drop _merge

    * Tax haven indicators
    qui gen tax_haven = 0
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_1)
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_2)
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_3)
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_4)
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_5)
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_6)
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_7)
    qui replace tax_haven = 1 if inlist(cgs_domicile, $tax_haven_8)
    order marketvalue_usd, last

    save $cmns1/temp/mns_sec_summary_`year'_disaggregated_emu, replace

}

* Append all the years
clear
gen year = .
forval year = 2007/2017 {
    append using $cmns1/temp/mns_sec_summary_`year'_disaggregated_emu
    replace year = `year' if missing(year)
}
sort year DomicileCountryId cusip
format %35s *name*
drop figi
drop if marketvalue_usd == 0
compress

* Summary data at security level
mmerge cusip using $raw/figi/figi_master_compact_cusip_unique.dta, unmatched(m) uname(f_)
replace asset_class = "Bonds - Structured Finance" if asset_class == "Bonds - Corporate" & f_marketsector == "Mtge"
drop f_* _merge
save $cmns1/holdings_master/mns_security_summary_disaggregated_emu, replace

* Summary data at issuer level
use $cmns1/holdings_master/mns_security_summary_disaggregated_emu, clear
gcollapse (sum) marketvalue_usd, by(year DomicileCountryId issuer_number asset_class issuer_name cgs_domicile cusip6_up_bg country_bg issuer_name_up tax_haven tax_haven_broad)
save $cmns1/holdings_master/mns_issuer_summary_disaggregated_emu, replace

log close
