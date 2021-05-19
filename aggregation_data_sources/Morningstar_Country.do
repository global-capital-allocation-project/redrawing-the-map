* --------------------------------------------------------------------------------------------------
* Morningstar_Country: This jobs finds the modal country code assigned to each CUSIP in the 
* Morningstar holdings data
* --------------------------------------------------------------------------------------------------
cap log close
log using $cmns1/logs/Build_Morningstar_Country, replace
set seed 1

* Minimum number of reporting funds for us to include CUSIP/country combination in the output
local min_reporting_funds_for_mode = 5
local min_reporting_funds_for_mode_th = 1
local submode_tolerance = 0.7
local submode_tolerance_th = 0.1
local max_countries_in_mode_or_sub = 3
local max_countries_in_mode_or_sub_th = 10

* Prepare the FIGI file: Issuer level
* To assign marketsector: we are conservative and assign issuers to the market sector
* for which we want to have the strictest SDC filters
use $raw/figi/figi_master_compact, clear
keep cusip marketsector
gen cusip6 = substr(cusip, 1, 6)
gen all_sector = marketsector
bysort cusip6 (marketsector): replace all_sector = all_sector + ", " + marketsector[_n-1] if _n>1 & marketsector != marketsector[_n-1]
bysort cusip6 (marketsector): replace all_sector = all_sector[_n-1] if all_sector[_n-1] != all_sector[_n] & strpos(all_sector[_n-1], marketsector) > 0
bysort cusip6 (marketsector): replace all_sector = all_sector[_N]
gen consolidated_sector = ""
replace consolidated_sector = "Other" if all_sector == "Curncy, Comdty"
replace consolidated_sector = "Other" if all_sector == "Equity, Comdty"
replace consolidated_sector = "Other" if all_sector == "Equity, Corp"
replace consolidated_sector = "Govt" if all_sector == "Govt, Corp"
replace consolidated_sector = "Govt" if all_sector == "Govt, Equity"
replace consolidated_sector = "Other" if inlist(all_sector, "Index, Comdty", "Index, Curncy", "Index, Equity")
replace consolidated_sector = "Corp" if inlist(all_sector, "Mtge, Corp", "Mtge, Equity")
replace consolidated_sector = "Agency_Structured" if inlist(all_sector, "Mtge, Govt")
replace consolidated_sector = "Muni" if inlist(all_sector, "Muni, Corp", "Muni, Equity", "Muni, Govt", "Muni, Mtge")
replace consolidated_sector = "Corp" if all_sector == "Pfd"
replace consolidated_sector = "Agency_Structured" if strpos(all_sector, "Pfd") & consolidated_sector == ""
replace consolidated_sector = "Corp" if inlist(marketsector, "Corp", "Equity") & consolidated_sector == ""
replace consolidated_sector = "Other" if inlist(marketsector, "Comdty", "Curncy") & consolidated_sector == ""
replace consolidated_sector = "Other" if inlist(marketsector, "Index") & consolidated_sector == ""
replace consolidated_sector = "Agency_Structured" if inlist(marketsector, "Mtge") & consolidated_sector == ""
replace consolidated_sector = "Govt" if inlist(marketsector, "Govt") & consolidated_sector == ""
replace consolidated_sector = "Muni" if inlist(marketsector, "Muni") & consolidated_sector == ""
replace consolidated_sector = "Agency_Structured" if all_sector == "Mtge, " & consolidated_sector == ""
replace consolidated_sector = "Other" if consolidated_sector == ""
collapse (firstnm) consolidated_sector, by(cusip6)
save $cmns1/temp/figi_cusip6_sectype, replace

* Append monthly Morningstar holdings files and keep only relevant variables.
* See Maggiori, Neiman, and Schreger (JPE 2019) for details on construction of the
* Morningstar holdings sample. The raw files that are necessary for this job are
* referred to as "step3" files in the build code of MNS.
clear
foreach holdingname in "NonUS" "US" { 
	forvalues x=2007/2017 {
		append using $raw/morningstar_holdings/`holdingname'_`x'_m_step3.dta, keep(cusip6 iso_co MasterPo)
	} 
}
save $cmns1/temp/morningstar_country/morningstar_holdings_appended.dta, replace

* Perform the country assignments
use $cmns1/temp/morningstar_country/morningstar_holdings_appended.dta, clear
drop if cusip6=="" | iso_co==""
replace iso_co="ANT" if iso_co=="AN"
replace iso_co="SRB" if iso_co=="CS"
replace iso_co="FXX" if iso_co=="FX"
replace iso_co="XSN" if iso_co=="S2"
replace iso_co="XSN" if iso_co=="XS"
replace iso_co="YUG" if iso_co=="YU"
replace iso_co="ZAR" if iso_co=="ZR"

* Collapse to fund-country-CUSIP level
gen counter = 1 if !missing(iso_country_code)
bysort cusip6 iso_co MasterPort: egen country_fund_count = sum(counter)
drop counter
gcollapse (firstnm) country_fund_count, by(cusip6 iso_country_code MasterPortfolioId)
save $cmns1/temp/morningstar_country/morningstar_holdings_collapsed, replace

* --------------------------------------------------------------------------------------------------
* Within-fund country assignments
* --------------------------------------------------------------------------------------------------

* Link to CGS domicile: TH vs. non-TH
use $cmns1/temp/morningstar_country/morningstar_holdings_collapsed, clear
drop if cusip == "#N/A N"
mmerge cusip using $cmns1/aggregation_sources/cgs_compact_complete.dta, umatch(issuer_number) ukeep(domicile) unmatched(m) uname(cgs_)
gen th_residency = 0
forvalues j=1(1)10 {
    cap replace th_residency = 1 if (inlist(cgs_domicile,${tax_haven_`j'}))
}
cap drop _merge

* For each fund, keep only the modal country assignment for each CUSIP, if this is unique
bysort cusip6 MasterPortfolioId: egen country_fund_count_max = max(country_fund_count)
drop if country_fund_count < `submode_tolerance' * country_fund_count_max & th_residency == 0
drop if country_fund_count < `submode_tolerance_th' * country_fund_count_max & th_residency == 1
by cusip6 MasterPortfolioId: egen n_countries_in_mode_or_submode = nvals(iso_country_code)

* Find out how many countries are in the submode and mode, within each fund
gen in_mode = 0
replace in_mode = 1 if country_fund_count == country_fund_count_max
bysort cusip6 in_mode MasterPortfolioId: egen countries_in_mode = nvals(iso_country_code)
replace countries_in_mode = . if in_mode == 0
bysort cusip6 MasterPortfolioId: egen _countries_in_mode = max(countries_in_mode)
replace countries_in_mode = _countries_in_mode
drop _countries_in_mode
gen countries_in_submode = n_countries_in_mode_or_submode - countries_in_mode

* Generate tax haven indicators
gen country_fund_nth = 1
forvalues j=1(1)10 {
    cap replace country_fund_nth=0 if (inlist(iso_country_code,${tax_haven_`j'}))
}

* If there are non-TH countries in the mode, drop all the TH countries
bysort cusip6 in_mode MasterPortfolioId: egen max_nth_in_modal_category = max(country_fund_nth)
bysort cusip6 MasterPortfolioId: egen max_nth = max(country_fund_nth)
gen max_nth_in_mode = max_nth_in_modal_category if in_mode == 1
by cusip6 MasterPortfolioId: egen _max_nth_in_mode = max(max_nth_in_mode)
replace max_nth_in_mode = _max_nth_in_mode
drop _max_nth_in_mode
gen max_nth_in_submode = max_nth_in_modal_category if in_mode == 0
by cusip6 MasterPortfolioId: egen _max_nth_in_submode = max(max_nth_in_submode)
replace max_nth_in_submode = _max_nth_in_submode
replace max_nth_in_submode = 0 if missing(max_nth_in_submode)
drop _max_nth_in_submode
drop if in_mode == 1 & max_nth_in_mode == 1 & country_fund_nth == 0
drop if in_mode == 0 & max_nth_in_mode == 1

* If there are two or more non-TH countries in the mode, pick one at random
bysort cusip6 in_mode MasterPortfolioId: gen _rand = runiform()
by cusip6 in_mode MasterPortfolioId: egen max_rand = max(_rand)  
drop if in_mode == 1 & countries_in_mode > 1 & _rand < max_rand & max_nth_in_modal_category == 1

* There are no non-TH countries in the mode, but there is one non-TH country in the submode
* There are no non-TH countries in the mode, but there are >1 non-TH countries in the submode; 
* pick via count order and then at random
drop if in_mode == 1 & max_nth_in_mode == 0 & max_nth_in_submode == 1 
drop if in_mode == 0 & country_fund_nth == 0 & max_nth_in_submode == 1
by cusip6 in_mode MasterPortfolioId: egen max_count_in_modal_category = max(country_fund_count)
drop if in_mode == 0 & country_fund_nth == 1 & max_nth_in_submode == 1 & country_fund_count < max_count_in_modal_category
cap drop max_rand
by cusip6 in_mode MasterPortfolioId: egen max_rand = max(_rand)
drop if in_mode == 0 & country_fund_nth == 1 & max_nth_in_submode == 1 & _rand < max_rand

* There is only one TH country in the mode and there are only TH countries in the submode
* There are >1 TH countries in the mode and there are only TH countries in the submode
drop if in_mode == 0 & max_nth_in_submode == 0 & max_nth_in_mode == 0
drop if in_mode == 1 & max_nth_in_submode == 0 & max_nth_in_mode == 0 & _rand < max_rand

// * Sanity check the procedure
// bysort MasterPortfolioId cusip6: egen n_countries_left = nvals(iso_country_code)
// assert n_countries_left == 1
// by MasterPortfolioId cusip6: gen n_vals = _N
// assert n_vals == 1

* Collapse the data to CUSIP-country level
cap drop country_fund_count
gen country_fund_count = 1
gcollapse (sum) country_fund_count, by(cusip6 iso_country_code th_residency)
save $cmns1/temp/morningstar_country/morningstar_holdings_cusip_country, replace

* --------------------------------------------------------------------------------------------------
* Across-funds country assignments
* --------------------------------------------------------------------------------------------------

* Find modal and submodal country assigned to each cusip
use $cmns1/temp/morningstar_country/morningstar_holdings_cusip_country, clear
bysort cusip6: egen country_fund_count_max = max(country_fund_count)
drop if country_fund_count < `submode_tolerance' * country_fund_count_max & th_residency == 0
drop if country_fund_count < `submode_tolerance_th' * country_fund_count_max & th_residency == 1
drop if country_fund_count_max < `min_reporting_funds_for_mode' & th_residency == 0
drop if country_fund_count_max < `min_reporting_funds_for_mode_th' & th_residency == 1
by cusip6: egen n_countries_in_mode_or_submode = nvals(iso_country_code)
drop if n_countries_in_mode_or_submode > `max_countries_in_mode_or_sub' & th_residency == 0
drop if n_countries_in_mode_or_submode > `max_countries_in_mode_or_sub_th' & th_residency == 1

* Use FIGI data to check which issuers are governments (sovereign or local); for these
* we do not accept anything in the submode
mmerge cusip6 using $cmns1/temp/figi_cusip6_sectype, unmatched(m)
drop if country_fund_count != country_fund_count_max & inlist(consolidated_sector, "Govt", "Muni")
drop _merge

* Find modal country assigned to each cusip
gen in_mode = 0
replace in_mode = 1 if country_fund_count == country_fund_count_max
bysort cusip6 in_mode: egen countries_in_mode = nvals(iso_country_code)
replace countries_in_mode = . if in_mode == 0
bysort cusip6: egen _countries_in_mode = max(countries_in_mode)
replace countries_in_mode = _countries_in_mode
drop _countries_in_mode
gen countries_in_submode = n_countries_in_mode_or_submode - countries_in_mode

* Generate tax haven indicators
gen country_fund_nth = 1
forvalues j=1(1)10 {
		cap replace country_fund_nth=0 if (inlist(iso_country_code,${tax_haven_`j'}))
}

* If there are non-TH countries in the mode, drop all the TH countries
by cusip6 in_mode: egen max_nth_in_modal_category = max(country_fund_nth)
by cusip6: egen max_nth = max(country_fund_nth)
gen max_nth_in_mode = max_nth_in_modal_category if in_mode == 1
by cusip6: egen _max_nth_in_mode = max(max_nth_in_mode)
replace max_nth_in_mode = _max_nth_in_mode
drop _max_nth_in_mode
gen max_nth_in_submode = max_nth_in_modal_category if in_mode == 0
by cusip6: egen _max_nth_in_submode = max(max_nth_in_submode)
replace max_nth_in_submode = _max_nth_in_submode
replace max_nth_in_submode = 0 if missing(max_nth_in_submode)
drop _max_nth_in_submode
drop if in_mode == 1 & max_nth_in_mode == 1 & country_fund_nth == 0
drop if in_mode == 0 & max_nth_in_mode == 1

* If there are two or more non-TH countries in the mode, pick one at random
by cusip6 in_mode: gen _rand = runiform()
by cusip6 in_mode: egen max_rand = max(_rand)  
drop if in_mode == 1 & countries_in_mode > 1 & _rand < max_rand & max_nth_in_modal_category == 1

* There are no non-TH countries in the mode, but there is one non-TH country in the submode
* There are no non-TH countries in the mode, but there are >1 non-TH countries in the submode; 
* pick via count order and then at random
drop if in_mode == 1 & max_nth_in_mode == 0 & max_nth_in_submode == 1 
drop if in_mode == 0 & country_fund_nth == 0 & max_nth_in_submode == 1
by cusip6 in_mode: egen max_count_in_modal_category = max(country_fund_count)
drop if in_mode == 0 & country_fund_nth == 1 & max_nth_in_submode == 1 & country_fund_count < max_count_in_modal_category
cap drop max_rand
by cusip6 in_mode: egen max_rand = max(_rand)  
drop if in_mode == 0 & country_fund_nth == 1 & max_nth_in_submode == 1 & _rand < max_rand

* There is only one TH country in the mode and there are only TH countries in the submode
* There are >1 TH countries in the mode and there are only TH countries in the submode
drop if in_mode == 0 & max_nth_in_submode == 0 & max_nth_in_mode == 0
drop if in_mode == 1 & max_nth_in_submode == 0 & max_nth_in_mode == 0 & _rand < max_rand


* Sanity check the procedure
by cusip6: egen n_countries_left = nvals(iso_country_code)
assert n_countries_left == 1
by cusip6: gen n_vals = _N
assert n_vals == 1

* Output dataset
keep cusip6 iso_country_code
save $cmns1/temp/morningstar_country/morningstar_country.dta, replace
save $cmns1/aggregation_sources/morningstar_country.dta, replace

log close
