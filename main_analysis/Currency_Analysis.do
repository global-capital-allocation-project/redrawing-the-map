* ---------------------------------------------------------------------------------------------------
* Currency_Analysis: Produces estimates of the currency composition of emerging market external
* portfolio debt (Table A.11 in the paper)
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Currency_Analysis, replace

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
    qui duplicates drop
end

* ---------------------------------------------------------------------------------------------------
* Corporate/sovereign shares
* ---------------------------------------------------------------------------------------------------

* Overall investment volume
use $cmns1/holdings_based_restatements/nationality_estimates, clear
drop if strpos(Asset_Class, "Equit")
collapse (sum) Position_Residency Position_Nationality_TH_Only, by(Year Investor Issuer)
bys Year Issuer: egen totR = total(Position_Residency)
bys Year Issuer: egen totN = total(Position_Nationality)
gen investor_share_R = Position_Residency / totR
gen investor_share_N = Position_N / totN
keep Year Investor Issuer investor_share_R investor_share_N
save $cmns1/temp/currency_investor_weights, replace

* Corporate and sovereign shares for USA
use $cmns1/holdings_based_restatements/Country_Portfolios_Nationality, clear
keep if inlist(Asset_Class, "Corporate Bonds", "Government Bonds")
keep if Investor == "USA"
replace Asset_Class = "BC" if strpos(Asset_Class, "Corporate")
replace Asset_Class = "BS" if strpos(Asset_Class, "Government")
keep Year Investor Issuer Position_Residency Position_Nationality_TH_Only Asset_Class
bys Year Investor Issuer: egen totR = total(Position_Residency)
bys Year Investor Issuer: egen totN = total(Position_N)
gen class_share_R = Position_Residency / totR
gen class_share_N = Position_N / totN
keep Year Investor Issuer Asset_Class class_share*
rename class_share_R class_share_R_
rename class_share_N class_share_N_
reshape wide class_share_R_ class_share_N_, i(Year Investor Issuer) j(Asset_Class) string
save $cmns1/temp/currency_corp_sov_shares_us, replace

* Corporate and sovereign shares for non-USA
use $cmns1/holdings_master/mns_issuer_summary, clear
drop if DomicileCountryId == "USA"
keep if inlist(asset_class, "Bonds - Corporate", "Bonds - Government")
collapse (sum) marketvalue_usd, by(year DomicileCountryId asset_class cgs_domicile)
replace asset_class = "BC" if asset_class == "Bonds - Corporate"
replace asset_class = "BS" if asset_class == "Bonds - Government"
bys year DomicileCountryId cgs_domicile: egen totVal = total(marketvalue_usd)
gen class_share_R_ = marketvalue_usd / totVal
rename year Year
rename DomicileCountryId Investor
rename cgs_domicile Issuer
drop marketvalue_usd totVal
qui reshape wide class_share_R_, i(Year Investor Issuer) j(asset_class) string
save $cmns1/temp/currency_corp_sov_shares_nonus_r, replace

use $cmns1/holdings_master/mns_issuer_summary, clear
drop if DomicileCountryId == "USA"
keep if inlist(asset_class, "Bonds - Corporate", "Bonds - Government")
gen th = 0
qui replace th = 1 if inlist(cgs_domicile, $tax_haven_1)
qui replace th = 1 if inlist(cgs_domicile, $tax_haven_2)
qui replace th = 1 if inlist(cgs_domicile, $tax_haven_3)
qui replace th = 1 if inlist(cgs_domicile, $tax_haven_4)
qui replace th = 1 if inlist(cgs_domicile, $tax_haven_5)
qui replace th = 1 if inlist(cgs_domicile, $tax_haven_6)
qui replace th = 1 if inlist(cgs_domicile, $tax_haven_7)
qui replace th = 1 if inlist(cgs_domicile, $tax_haven_8)
replace country_bg = cgs_domicile if ~missing(cgs_domicile) & th == 0
collapse (sum) marketvalue_usd, by(year DomicileCountryId asset_class country_bg)
replace asset_class = "BC" if asset_class == "Bonds - Corporate"
replace asset_class = "BS" if asset_class == "Bonds - Government"
bys year DomicileCountryId country_bg: egen totVal = total(marketvalue_usd)
gen class_share_N_ = marketvalue_usd / totVal
rename year Year
rename DomicileCountryId Investor
rename country_bg Issuer
drop marketvalue_usd totVal
qui reshape wide class_share_N_, i(Year Investor Issuer) j(asset_class) string
save $cmns1/temp/currency_corp_sov_shares_nonus_n, replace

* Merge all corporate/sovereign shares
use $cmns1/temp/currency_corp_sov_shares_nonus_r, clear
qui mmerge Year Investor Issuer using $cmns1/temp/currency_corp_sov_shares_nonus_n
drop _merge
save $cmns1/temp/currency_corp_sov_shares_nonus, replace

use $cmns1/temp/currency_corp_sov_shares_nonus, clear
append using $cmns1/temp/currency_corp_sov_shares_us
save $cmns1/temp/currency_corp_sov_shares, replace

* ---------------------------------------------------------------------------------------------------
* Local currency shares
* ---------------------------------------------------------------------------------------------------

* Summary data
use $cmns1/holdings_master/mns_security_summary, clear
keep year Domicile asset_class currency_id cgs_dom country_bg marketvalue_usd
keep if regexm(asset_class,"Bonds - Corp")==1 | regexm(asset_class,"Bonds - Government")==1 
replace asset_class="BC" if regexm(asset_class,"Bonds - Corp")==1
replace asset_class="BS" if regexm(asset_class,"Bonds - Government")==1
drop if curr=="" | cgs_dom=="" | country_bg==""
rename mark mv
gen th_cgs_dom = 0
replace th_cgs_dom = 1 if inlist(cgs_dom, $tax_haven_1) | inlist(cgs_dom, $tax_haven_2) | inlist(cgs_dom, $tax_haven_3) ///
    | inlist(cgs_dom, $tax_haven_4) | inlist(cgs_dom, $tax_haven_5) | inlist(cgs_dom, $tax_haven_6) | inlist(cgs_dom, $tax_haven_7) ///
    | inlist(cgs_dom, $tax_haven_8)
replace country_bg = cgs_dom if country_bg != cgs_dom & th_cgs_dom == 0

collapse (sum) mv, by(year Dom asset currency cgs country)
qui mmerge cgs_dom using $raw/Macro/Concordances/country_currency.dta, umatch(iso_country_code) uname(lc_r_)
drop if _merge==2
qui mmerge country_bg using $raw/Macro/Concordances/country_currency.dta, umatch(iso_country_code) uname(lc_n_)
drop if _merge==2
drop _merge
replace mv=0 if mv<0
save $cmns1/temp/mns_collapsed.dta, replace

* Get currency breakdowns data from TIC
import delimited using $raw/TIC/Annual/shca2017_appendix/shc_app11_2017.csv, clear varnames(1) rowrange(11)
rename v1 Countryorregion
rename v2 total_debt
rename v3 gov_tot
rename v4 gov_usd
rename v5 gov_lc
rename v6 private_tot
rename v7 private_usd
rename v8 private_lc
qui drop if _n == 1
qui drop if missing(Countryorregion)
process_names
clean_rows
qui mmerge Countryorregion using $cmns1/temp/tic_data/tic_xwalk.dta, umatch(country) unmatched(m)
drop _merge
qui drop if missing(iso)
drop Countryorregion
order iso
qui destring total_debt gov_tot gov_usd gov_lc private_tot private_usd private_lc, force replace
gen tic_gov_lc_share = gov_lc / gov_tot
gen tic_corp_lc_share = private_lc / private_tot
keep iso tic*
gen DomicileCountryId = "USA"
gen year = 2017
rename iso iso_country_code
save $scratch/tic_lc_shares, replace

* ---------------------------------------------------------------------------------------------------
* Currency composition data from Morningstar
* ---------------------------------------------------------------------------------------------------

* Local currency shares under residency
use $cmns1/temp/mns_collapsed.dta, clear
collapse (sum) mv (firstnm) lc_r_iso_currency_code, by(year Dom currency cgs_dom asset_class)
bysort Dom cgs_dom year asset_class: egen total_r_ij_=sum(mv)
keep if currency_id==lc_r
gen lc_share_r_=mv/total_r
keep if currency_id==lc_r
rename mv mv_r_
drop lc_r
rename cgs_dom iso_country_code
drop currency_id
qui reshape wide mv_r_ total_r_ij_ lc_share_r_, i(year iso Dom) j(asset_class) str
save $cmns1/temp/cs_lcshare_r_mult_by_investor.dta, replace

* Local currency shares under nationality
use $cmns1/temp/mns_collapsed.dta, clear
collapse (sum) mv (firstnm) lc_n_iso, by(year Dom currency country_bg asset_class)
bysort Dom country_bg year asset_class: egen total_n_ij_=sum(mv)
keep if currency_id==lc_n
gen lc_share_n_=mv/total_n
rename mv mv_n_
drop lc_n
rename country_bg iso_country_code
drop currency_id
qui reshape wide mv_n_ total_n_ij_ lc_share_n_, i(year iso Dom) j(asset_class) str
save $cmns1/temp/cs_lcshare_n_mult_by_investor.dta, replace

* Merge local currency data by investor
use $cmns1/temp/cs_lcshare_r_mult_by_investor, clear
qui mmerge year iso Dom using $cmns1/temp/cs_lcshare_n_mult_by_investor
drop _merge mv_* total*
qui mmerge year iso Dom using $scratch/tic_lc_shares.dta, unmatched(m)
replace lc_share_r_BC = tic_corp_lc_share if ~missing(tic_corp_lc_share)
replace lc_share_r_BS = tic_gov_lc_share if ~missing(tic_gov_lc_share)
replace lc_share_n_BS = tic_gov_lc_share if ~missing(tic_gov_lc_share)
drop _merge tic*
save $cmns1/temp/cs_lcshare_mult_by_investor, replace

use $cmns1/holdings_based_restatements/nationality_estimates.dta, clear
keep if inlist(Asset_Class_Code, "BC", "BG", "B")
drop Position_Nationality_Full Position_Residency_Com Position_Nationality_TH_Only_Com
rename Position_Nationality_TH_Only Position_Nationality
qui reshape wide Position_Residency Position_Nationality, i(Year Investor Issuer) j(Asset_Class_Code) string
qui mmerge Year Investor Issuer using $cmns1/temp/currency_corp_sov_shares.dta, unmatched(m)
drop _merge
rename Year year
rename Investor DomicileCountryId
rename Issuer iso_country_code
rename Position_ResidencyBC holdings_r_BC
rename Position_NationalityBC holdings_n_BC
rename Position_ResidencyBG holdings_r_BS
rename Position_NationalityBG holdings_n_BS

replace holdings_r_BC = Position_ResidencyB * class_share_R_BC if Dom != "USA"
replace holdings_n_BC = Position_NationalityB * class_share_N_BC if Dom != "USA"
replace holdings_r_BS = Position_ResidencyB * class_share_R_BS if Dom != "USA"
replace holdings_n_BS = Position_NationalityB * class_share_N_BS if Dom != "USA"
drop class_share* Position*
save $scratch/bilateral_positions_currency, replace

cap restore
use $cmns1/temp/cs_lcshare_mult_by_investor, clear
qui mmerge year iso Dom using $scratch/bilateral_positions_currency, unmatched(m)
qui keep if year == 2017
drop _merge
foreach var of varlist holdings* {
    qui replace `var' = 0 if missing(`var')
    qui replace `var' = 0 if `var' < 0 
}

preserve
collapse (mean) lc_share_r_BC [aw = holdings_r_BC], by(year iso)
qui save $scratch/lc_share_r_BC, replace
restore

preserve
collapse (mean) lc_share_n_BC [aw = holdings_r_BC], by(year iso)
qui save $scratch/lc_share_n_BC, replace
restore

preserve
collapse (mean) lc_share_r_BS [aw = holdings_r_BS], by(year iso)
qui save $scratch/lc_share_r_BS, replace
restore

preserve
collapse (mean) lc_share_n_BS [aw = holdings_r_BS], by(year iso)
qui save $scratch/lc_share_n_BS, replace
restore

use $scratch/lc_share_r_BC, clear
foreach x in "r_BS" "n_BC" "n_BS" {
    qui mmerge year iso using $scratch/lc_share_`x'
}
drop _merge
save $cmns1/temp/cs_lcshare_mult_tic_cpis_weights, replace

* ---------------------------------------------------------------------------------------------------
* Multilateral estimates
* ---------------------------------------------------------------------------------------------------

* Multilateral asset class shares
use $cmns1/temp/currency_corp_sov_shares, clear
qui mmerge Year Investor Issuer using $cmns1/temp/currency_investor_weights, unmatched(m)
keep if _merge == 3
drop _merge
keep if inlist(Investor, "USA", "CAN", "EMU", "GBR") | inlist(Investor, "AUS", "NOR", "SWE", "DNK", "CHE")
replace investor_share_R = 0 if investor_share_R < 0
replace investor_share_N = 0 if investor_share_N < 0
collapse (mean) class_share_R_BC class_share_R_BS [aw = investor_share_R], by(Year Issuer)
save $cmns1/temp/currency_multilateral_asset_share_r, replace

use $cmns1/temp/currency_corp_sov_shares, clear
qui mmerge Year Investor Issuer using $cmns1/temp/currency_investor_weights, unmatched(m)
keep if _merge == 3
drop _merge
keep if inlist(Investor, "USA", "CAN", "EMU", "GBR") | inlist(Investor, "AUS", "NOR", "SWE", "DNK", "CHE")
replace investor_share_R = 0 if investor_share_R < 0
replace investor_share_N = 0 if investor_share_N < 0
collapse (mean) class_share_N_BC class_share_N_BS [aw = investor_share_N], by(Year Issuer)
save $cmns1/temp/currency_multilateral_asset_share_n, replace

use $cmns1/temp/currency_multilateral_asset_share_r, clear
mmerge Year Issuer using $cmns1/temp/currency_multilateral_asset_share_n
drop _merge
save $cmns1/temp/currency_multilateral_asset_share, replace

* Currency composition
use $cmns1/temp/cs_lcshare_mult_tic_cpis_weights, clear
keep year iso_country_code lc_share*
rename iso_country_code Issuer
rename year Year
rename lc_share_r* lc_share_R*
rename lc_share_n* lc_share_N*
save $cmns1/temp/currency_multilateral_lc_share, replace

* Merge
use $cmns1/temp/currency_multilateral_asset_share, clear
qui mmerge Year Issuer using $cmns1/temp/currency_multilateral_lc_share
drop _merge
gen lc_share_agg_R = lc_share_R_BC * class_share_R_BC + lc_share_R_BS * class_share_R_BS
gen lc_share_agg_N = lc_share_N_BC * class_share_N_BC + lc_share_N_BS * class_share_N_BS
drop if Year < 2007
save $cmns1/temp/currency_all_shares, replace

* Currency estimates table
use $cmns1/temp/currency_all_shares, clear
keep if inlist(Issuer, "ARG", "BRA", "CHL", "CHN") | inlist(Issuer, "IDN", "IND", "ISR") | inlist(Issuer, "MEX", "MYS") | inlist(Issuer, "RUS", "THA", "TUR", "ZAF")
keep if Year == 2017
count

keep Issuer lc_share_agg_R lc_share_agg_N class_share_R_BC class_share_N_BC ///
    lc_share_R_BC lc_share_N_BC lc_share_R_BS lc_share_N_BS

order Issuer class_share_R_BC class_share_N_BC lc_share_agg_R lc_share_agg_N  ///
    lc_share_R_BC lc_share_N_BC lc_share_R_BS lc_share_N_BS
sort Issuer

export excel using $cmns1/tables/currency_table.xls, replace firstrow(variables)

log close
