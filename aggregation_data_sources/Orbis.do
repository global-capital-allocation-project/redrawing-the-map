* --------------------------------------------------------------------------------------------------
* Orbis: This file build the ultimate-parent aggregation data from Bureau van Dijk, which is
* used as input in the CMNS aggregation procedure.
* --------------------------------------------------------------------------------------------------
cap log close
log using $cmns1/logs/Orbis_Build, replace
global temp $cmns1/temp
set more off

* --------------------------------------------------------------------------------------------------
* First we build static identifier data for Orbis from Bureau van Dijk. This data includes a
* record of BvD ID changes over time, as well as mappings from BvD IDs to Legal Entity Identifiers
* (LEIs) and ISINs.
* --------------------------------------------------------------------------------------------------

* Make the new to old BVDID map stationary
use $raw/orbis/header/BvDIDChange.dta, clear
save $temp/orbis/BvDIDChange.dta, replace
use $raw/orbis/header/BvDIDChange.dta, clear
rename NewID NewID_additional
save $temp/orbis/BvDIDChange_additional.dta, replace

forvalues i=1/12 {
	di "Running BVDID flattening, iteration `i'"
	use $temp/orbis/BvDIDChange.dta, clear
	mmerge NewID using $temp/orbis/BvDIDChange_additional.dta, type(n:1) umatch(OldID) unmatched(master)
	egen _maxmerge = max(_merge)
	local maxmerge = _maxmerge
	if `maxmerge' == 1 {
		continue, break
	}
	replace NewID = NewID_additional if _merge == 3
	drop _maxmerge NewID_additional
	cap drop _merge
	save $temp/orbis/BvDIDChange.dta, replace
}

* Prep ALLMASTER_ISSUER plus CGS LEI PLUS
use issuer_number lei_gmei using $temp/cgs/ALLMASTER_ISSUER.dta, clear
drop if missing(lei_gmei) | missing(issuer_number)
gen file = "allmaster"
append using $temp/cgs/lei_plus_formerge.dta
replace file = "leiplus" if missing(file)
duplicates drop issuer_number lei_gmei, force
duplicates tag lei_gmei, gen(_dup)
drop if _dup > 0 & file == "allmaster"
drop _dup
duplicates tag lei_gmei, gen(_dup)
assert _dup == 0
drop _dup
save $temp/cgs/LEI_to_CUSIP6.dta, replace

* Update BVDID in other files
use $raw/orbis/header/ISIN_BvDID.dta, clear
mmerge bvdid using $temp/orbis/BvDIDChange.dta, type(n:1) umatch(OldID) unmatched(master)
replace bvdid = NewID if _merge == 3
drop NewID _merge
save $temp/orbis/ISIN_BvDID.dta, replace

use $raw/orbis/header/LEI_details.dta, clear
mmerge bvdid using $temp/orbis/BvDIDChange.dta, type(n:1) umatch(OldID) unmatched(master)
replace bvdid = NewID if _merge == 3
drop NewID _merge
save $temp/orbis/LEI_details.dta, replace

* --------------------------------------------------------------------------------------------------
* Next we build the Orbis corporate ownership data. We loop over each country.
* For each run, we construct a file with the history of equityholder and subsidiaries information
* for each of the companies in Orbis.
* --------------------------------------------------------------------------------------------------

* Program to process identifiers
global orbis_ownership $raw/orbis/ownership_data
cap program drop process_identifiers
program define process_identifiers
	args bvd_varlist
	local n_extra_vars : word count `bvd_varlist'
	di "Processing `n_extra_vars' variables in bvd_varlist"
	forval i=1/`n_extra_vars' {

		* Get varname
		local varname `: word `i' of `bvd_varlist''
		di "Processing `varname'"

		* Update BVDID
		mmerge `varname' using $temp/orbis/BvDIDChange.dta, type(n:1) umatch(OldID) unmatched(master)
		replace `varname' = NewID if _merge == 3
		drop NewID _merge
		
		* Get ISIN
		mmerge `varname' using $temp/orbis/ISIN_BvDID.dta, type(n:1) umatch(bvdid) unmatched(master)
		rename ISIN `varname'_isin
		drop _merge
		
		* Get LEI
		mmerge `varname' using $temp/orbis/LEI_details.dta, type(n:1) umatch(bvdid) unmatched(master) ukeep(LEI)
		rename LEI `varname'_lei
		drop _merge
		
		* Match LEI to CUSIP6
		mmerge `varname'_lei using $temp/cgs/LEI_to_CUSIP6.dta, type(n:1) umatch(lei_gmei) unmatched(master) ukeep(issuer_number)
		rename issuer_number `varname'_cusip6
		drop _merge
		
		* Match ISIN to CUSIP6
		mmerge `varname'_isin using $temp/cgs/allmaster_essentials_isin.dta, type(n:1) umatch(isin)  unmatched(master) ukeep(issuer_num)
		replace `varname'_cusip6 = issuer_num if missing(`varname'_cusip6)
		drop issuer_num _merge
}
end

* Loop over countries
forval j=1/225 {

	* Find out which country we are processing
	local orbis_country_list : dir "$orbis_ownership" dirs "*"
	local i = 1
	foreach country_tmp of local orbis_country_list {
		if `i' == `j' {
			local country = "`country_tmp'"
			di "Processing country `country'"
		}
		local i = `i' + 1
	}

	* Quit job if country == "03"
	if "`country'" == "03" {
		log close
		exit
	}

	cap confirm file "$orbis_ownership/`country'/SHARE_`country'_Links_allyrs.dta"
	if _rc==0 {

		* Read in relevant info; we use the guo50c field instead of guo50 to ensure we track companies 
		* to ultimate corporate owners rather than individuals
		use $orbis_ownership/`country'/SHARE_`country'_Links_allyrs.dta, clear
		keep bvdid shareholderbvdid directonlyfigures totalonlyfigures informationdate typeofrelation guo50 guo50c file bvdid ISO_final_subsidiary ISO_final_shareholder
		replace guo50 = "NA" if missing(guo50)
		replace guo50c = "NA" if missing(guo50c)
		replace guo50 = guo50c
		drop guo50c
		duplicates drop bvdid guo50 informationdate file, force

		* Convert all ISO2 to ISO3
		mmerge ISO_final_subsidiary using $raw/Macro/Concordances/iso2_iso3.dta, type(n:1) umatch(iso2) unmatched(master)
		replace ISO_final_subsidiary = iso3 if _merge == 3
		drop iso3 _merge
		mmerge ISO_final_shareholder using $raw/Macro/Concordances/iso2_iso3.dta, type(n:1) umatch(iso2) unmatched(master)
		replace ISO_final_shareholder = iso3 if _merge == 3
		drop iso3 _merge

		* Renaming columns
		rename ISO_final_shareholder bvd_iso_country_code_shareholder
		rename ISO_final_subsidiary bvd_iso_country_code_subsidiary
		rename directonlyfigures direct
		rename totalonlyfigures total

		* Some straightforward adjustments to GUO50
		replace guo50 = "" if guo50 == "NA"
		replace guo50 = bvdid if missing(guo50) & bvdid == shareholderbvdid & (direct == 100 | total == 100)
		cap drop missing_guo50
		gen missing_guo50 = 0
		replace missing_guo50 = 1 if missing(guo50)
		drop if missing(guo50)

		* Parse date
		tostring informationdate, replace
		gen informationdate_parsed = date(informationdate, "YMD")
		drop informationdate
		rename informationdate_parsed informationdate
		format informationdate %td

		* Process identifiers for SUB-GUO50 unique links only
		process_identifiers "bvdid guo50"

		* Store output, both in full and in compact versions
		save $temp/orbis/shareholder_info_`country'.dta, replace
		keep bvdid bvd_iso_country_code_subsidiary
		rename bvd_iso_country_code_subsidiary bvd_iso_country_code
		duplicates drop bvdid bvd_iso_country_code, force
		save $temp/orbis/country/shareholder_info_`country'.dta, replace
		use $temp/orbis/shareholder_info_`country'.dta, clear
		keep bvdid bvdid_cusip6 guo50_cusip6 file informationdate guo50
		keep if ~missing(bvdid_cusip6) & ~missing(guo50_cusip6)
		save $temp/orbis/compact/shareholder_info_`country'.dta, replace
	}
	else {
		di "Skipping `country' as data is absent (this is expected for a subset of countries)"
	}

}

* --------------------------------------------------------------------------------------------------
* We consolidate the country-specific Orbis ownership files generates in Orbis_Build_Step2, and 
* produces the appended version of the Orbis ownership database that we use for ultimate parent
* aggregation.
* --------------------------------------------------------------------------------------------------

* Get BVDID - country map
local orbis_country_list : dir "$orbis_ownership" dirs "*"
local i = 1
foreach country of local orbis_country_list {
	if "`country'" != "03" {
		di "Country; processing `country' (`i')"
		cap append using $temp/orbis/country/shareholder_info_`country'.dta, keep(bvdid bvd_iso_country_code)
		local i = `i' + 1
	}
}

* Manual fixes for few BVDID that associate to multiple countries
drop if bvdid == "HK0000074755" & bvd_iso_country_code != "HKG"
drop if bvdid == "NL27069234" & bvd_iso_country_code != "NLD"
drop if bvdid == "RO4034103" & bvd_iso_country_code != "ROU"
drop if bvdid == "GB06423547" & bvd_iso_country_code != "GBR"
drop if bvdid == "GB08398929" & bvd_iso_country_code != "GBR"

* Manual fixes for sovranational issuers
replace bvd_iso_country_code = "XSN" if bvd_iso_country_code == "II"

* Save output
save $temp/orbis/bvdid_to_country.dta, replace

* Consolidate compact files
clear
local i = 1
foreach country of local orbis_country_list {
	if "`country'" != "03" {
		di "Compact files; processing `country' (`i')"
		cap append using $temp/orbis/compact/shareholder_info_`country'.dta
		local i = `i' + 1
	}
}
mmerge guo50 using $temp/orbis/bvdid_to_country.dta, type(n:1) unmatched(master) umatch(bvdid)
cap drop _merge
rename bvd_iso_country_code guo50_iso_country_code
drop if inlist(guo50_iso_country_code, "WW", "XX", "YY", "ZZ")
by bvdid_cusip6 (informationdate), sort: gen byte last_obs = (_n == _N)
keep if last_obs == 1
drop informationdate file last_obs
rename guo50_iso_country_code country_bvd
rename bvdid_cusip6 cusip6
rename guo50_cusip6 up_cusip6
save $cmns1/aggregation_sources/orbis_aggregation.dta, replace

log close
