* --------------------------------------------------------------------------------------------------
* Capital_IQ: This file build the ultimate-parent aggregation data from Capital IQ, which is
* used as input in the CMNS aggregation procedure.
* --------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Build_Capital_IQ, replace

* Import the raw data
import delimited using $raw/ciq/ciq_ultimate_parents.csv, clear
save $temp/ciq_aggregation_full, replace

* Pre-process
use $cmns1/temp/ciq_aggregation_full, clear
drop ïcompanyid
replace up_cusip = subinstr(up_cusip, "CSP_", "", .)
gen ciqup_cusip6 = substr(up_cusip, 1, 6)
drop if ciqup_cusip6==""
drop if ciqup_cusip6=="nan"
drop if ciqup_cusip6=="(Invalid Identifier)"
drop if up_ciqid == "(Invalid Identifier)"
replace cusip = ïcusip if missing(cusip)
drop ïcusip
gen cusip6 = substr(cusip, 1, 6)

* CIQ uses 0 occasionally when the child is equal to the parent
replace ciqup_cusip6 = cusip6 if ciqup_cusip6 == "0"

* Country
mmerge up_country using $raw/Macro/common/country_iso2_to_iso3, umatch(iso2) ukeep(iso3) uname("ciq_")
drop up_country
rename ciq_iso ciqup_country
drop if _merge==2
save $cmns1/temp/ciq_building_temp.dta, replace

* Perform a consistency and modal assignement (step2) for country on ciq ultimate parents
use $cmns1/temp/ciq_building_temp.dta, clear
keep ciqup_cusip6 ciqup_country
gen counter = 1 if !missing(ciqup_country)
bysort ciqup_cusip6 ciqup_country: egen country_count=sum(counter)
drop counter
collapse (firstnm) country_count, by(ciqup_cusip6 ciqup_country)

* Rank the frequency of each country by cusip. Rank 1 are the most frequently assigned countries. Ties are all assigned the same rank
bysort ciqup_cusip6: egen country_count_rank=rank(-country_count), track
drop if country_count_rank>2
gen country_cusip_nfp=1
replace country_cusip_nfp= 0 if inlist(ciqup_country, $tax_haven_1) | inlist(ciqup_country, $tax_haven_2) | inlist(ciqup_country, $tax_haven_3) | inlist(ciqup_country, $tax_haven_4) | inlist(ciqup_country, $tax_haven_5) | inlist(ciqup_country, $tax_haven_6) | inlist(ciqup_country, $tax_haven_7) | inlist(ciqup_country, $tax_haven_8)
bysort ciqup_cusip6: egen country_cusip_count_nfp=sum(country_cusip_nfp)

* Only fiscal paradises: we choose the mode, and at random within the mode 
drop if country_cusip_count_nfp==0 & country_count_rank==2
bysort ciqup_cusip6: gen fp_rand=runiform()
bysort ciqup_cusip6: egen fp_rand_max=max(fp_rand)
drop if country_cusip_count_nfp==0 & country_count_rank==1 & fp_rand<fp_rand_max

* Mixed or only regular countries: we choose the mode if regular country, or the rank 2 if rank 1 only has FPs. If indifferent, we pick at random within the same rank.
drop if country_cusip_count_nfp>=1 & inlist(ciqup_country, $tax_haven_1) | inlist(ciqup_country, $tax_haven_2) | inlist(ciqup_country, $tax_haven_3) | inlist(ciqup_country, $tax_haven_4) | inlist(ciqup_country, $tax_haven_5) | inlist(ciqup_country, $tax_haven_6) | inlist(ciqup_country, $tax_haven_7) | inlist(ciqup_country, $tax_haven_8)
bysort ciqup_cusip6: egen country_count_rank_temp=rank(country_count_rank), track
drop if country_cusip_count_nfp>=1 & country_count_rank_temp>=2
drop fp_rand_max
bysort ciqup_cusip6: egen fp_rand_max=max(fp_rand)
drop if country_cusip_count_nfp>=1 & country_count_rank_temp==1 & fp_rand<fp_rand_max
keep ciqup_cusip6 ciqup_country
save $cmns1/temp/ciq_unique_ciqup_country.dta, replace

* Merge in the ciqup unique country info
use $cmns1/temp/ciq_building_temp.dta, clear
drop ciqup_country
mmerge ciqup_cusip6 using  $cmns1/temp/ciq_unique_ciqup_country.dta, ukeep(ciqup_country)

* Note there are CUSIPs in CIQ that are not in CGS in the two operations above; we left them in
drop if cusip6=="" | cusip6=="000000"
drop if ciqup_cusip6=="" | ciqup_cusip6=="000000"
save $cmns1/temp/ciq_cusip6_ultimateparents.dta, replace

* CIQ maps same cusip6 into multiple ciqup_cusip6. We select a unique match below
use $cmns1/temp/ciq_cusip6_ultimateparents.dta, clear
duplicates drop ciqup_cusip6 cusip6 ciqup_country, force
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count_cusip6=sum(counter)
drop counter

* If the procedure returned original, go with other one
drop if cusip6==ciqup_cusip6 & count_cusip6>1
drop count_cusip6

* Duplicates drop as long as cusip6 and ciqup_country is the same
duplicates drop cusip6 ciqup_country, force

* All that is now left is a few structured finance vehicles and funds with several sponsoring banks; keeping the first
gen counter=1 if !missing(cusip6)
bysort cusip6: egen count_cusip6=sum(counter)
drop counter
bysort cusip6: gen n=_n
keep if n==1
drop n count
save $cmns1/temp/ciq_cusip6_ultimateparents_unique.dta, replace

* Generate consolidated dataset of CIQ company names; for simplicity we take the first record when duplicated
use $cmns1/temp/ciq_aggregation_full, clear
drop ïcompanyid
replace cusip = ïcusip if missing(cusip)
drop ïcusip
gen cusip6 = substr(cusip, 1, 6)
collapse (firstnm) companyname, by(cusip6)
save $cmns1/temp/ciq_names, replace
save $cmns1/aggregation_sources/ciq_names, replace

* Impose a (light) filter for sovereigns, muni bonds, and agency bonds: if the child is explicitly tagged
* as such in OpenFIGI, we run a fuzzy name-match against CGS (the parents all look fine). This filter is 
* less stringent than the one we impose for SDC since the latter showed many more issues than Capital IQ.
* We also drop bad name matches for which we have a different sector classification, although in these cases
* we are less stringent (because of the prior that any potential mistakes are concentrated among sovereign,
* municipal, and agency securities rather than corporates, which are CIQ's primary focus).
local name_match_threshold_ciq = 0.75
local name_match_threshold_ciq_blank = 0.5
use $cmns1/temp/ciq_cusip6_ultimateparents_unique.dta, clear
mmerge cusip6 using $cmns1/temp/figi_cusip6_sectype, unmatched(m) umatch(cusip6)
mmerge cusip6 using $cmns1/aggregation_sources/cgs_compact_complete, umatch(issuer_number) unmatched(m) uname(cgs_)
mmerge ciqup_cusip6 using $cmns1/temp/figi_cusip6_sectype, unmatched(m) umatch(cusip6) uname(up_)
mmerge ciqup_cusip6 using $cmns1/aggregation_sources/cgs_compact_complete, umatch(issuer_number) unmatched(m) uname(up_cgs_)
mmerge ciqup_cusip6 using $cmns1/temp/ciq_names, umatch(cusip6) unmatched(m) uname(up_)
gen ciq_issuer_name = upper(companyname)
gen up_ciq_issuer_name = upper(up_companyname)
format %40s *name*
foreach name_field in "cgs_issuer_name" "ciq_issuer_name" "up_cgs_issuer_name" "up_ciq_issuer_name" {
	replace `name_field' = subinstr(`name_field',  " NOTES ",  "", 30)
	replace `name_field' = subinstr(`name_field',  "SHORT TERM",  "", 30)
	replace `name_field' = subinstr(`name_field',  "MEDIUM TERM",  "", 30)
	replace `name_field' = subinstr(`name_field',  "LONG TERM",  "", 30)
	replace `name_field' = subinstr(`name_field',  "BOOK ENTRY",  "", 30)
	replace `name_field' = subinstr(`name_field',  "PASS THRU TRS",  "", 30)
	replace `name_field' = subinstr(`name_field',  "MEDIUM-TERM NTS ",  "", 30)
	replace `name_field' = subinstr(`name_field',  "SYSTEMWIDE",  "", 30)
	replace `name_field' = subinstr(`name_field',  "SOCIETA PER AZIONI",  "", 30)
	replace `name_field' = subinstr(`name_field',  "SOCIETAS EUROPAEA",  "", 30)
	replace `name_field' = subinstr(`name_field',  "(AMR CORP)",  "", 30)
	replace `name_field' = subinstr(`name_field',  "OBLIGS",  "", 30)
	replace `name_field' = subinstr(`name_field',  "COML PAPER",  "", 30)
	replace `name_field' = subinstr(`name_field',  "LEASE REV",  "", 30)
	replace `name_field' = subinstr(`name_field',  "PASS THOUGH",  "", 30)
	replace `name_field' = subinstr(`name_field',  "FOR FUTURE ISSUES",  "", 30)
	replace `name_field' = subinstr(`name_field',  "CONDUIT",  "", 30)
	replace `name_field' = subinstr(`name_field',  "COML PAPER",  "", 30)
	replace `name_field' = subinstr(`name_field',  "144A",  "", 30)
	replace `name_field' = subinstr(`name_field',  "LN TR",  "", 30)
	replace `name_field' = subinstr(`name_field',  "SR REG",  "", 30)
	replace `name_field' = subinstr(`name_field',  "MEDIUM- TERM",  "", 30)
	replace `name_field' = subinstr(`name_field',  "SHORT- TERM",  "", 30)
	replace `name_field' = subinstr(`name_field',  "LONG- TERM",  "", 30)
	replace `name_field' = subinstr(`name_field',  "NTS-",  "", 30)
	replace `name_field' = subinstr(`name_field',  "SEE 86358R",  "", 30)
	replace `name_field' = subinstr(`name_field',  "SEE 07387A",  "", 30)
	replace `name_field' = subinstr(`name_field',  "ISSUES WITH 10 DAY CALL NOTICE",  "", 30)
}
gen th_cusip6 = 0
forvalues j=1(1)10 {
    cap replace th_cusip6 = 1 if (inlist(cgs_domicile,${tax_haven`j'}))
}
jarowinkler ciq_issuer_name cgs_issuer_name, gen(namedist)
jarowinkler up_ciq_issuer_name up_cgs_issuer_name, gen(up_namedist)
order namedist ciq_issuer_name cgs_issuer_name consolidated_sector
gsort -namedist
drop if inlist(consolidated_sector, "Govt", "Muni", "Agency_Structured") & ~missing(cgs_issuer_name) & ~missing(ciq_issuer_name) & th_cusip6 == 0 & namedist < `name_match_threshold_ciq'
drop if ~missing(cgs_issuer_name) & ~missing(ciq_issuer_name) & th_cusip6 == 0 & namedist < `name_match_threshold_ciq_blank'
save $cmns1/temp/ciq_cusip6_ultimateparents_unique_filtered.dta, replace

* Keep only relevant variables
use $cmns1/temp/ciq_cusip6_ultimateparents_unique_filtered.dta, clear
keep ciqup_country cusip6 ciqup_cusip6 
rename ciqup_country ciq_country_bg
save $cmns1/aggregation_sources/ciq_aggregation.dta, replace

log close
