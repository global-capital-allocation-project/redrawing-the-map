* --------------------------------------------------------------------------------------------------
* Factset: This file build the ultimate-parent aggregation data from Factset, which is
* used as input in the CMNS aggregation procedure.
* --------------------------------------------------------------------------------------------------
cap log close
log using $cmns1/logs/Build_Factset, replace
global temp $cmns1/temp 

* ---------------------------------------------------------------------------------------------------
* Improved version of sym_cusip
* ---------------------------------------------------------------------------------------------------

use $raw/Factset/fds_stata/sym_isin, clear
mmerge isin using $temp/cgs/isin_to_cusip, unmatched(m) update
rename cusip9 cusip
drop if missing(cusip)
drop isin
drop cgs_currency _merge
tempfile sym_isin_with_cusip
save `sym_isin_with_cusip', replace

use $raw/Factset/fds_stata/sym_cusip, clear
gen priority = 0
append using `sym_isin_with_cusip'
replace priority = 1 if missing(priority)
sort cusip priority
by cusip: keep if _n == 1
rename priority isin_matched
save $temp/factset/sym_cusip_improved, replace

* ---------------------------------------------------------------------------------------------------
* Compact version of sym_coverage
* ---------------------------------------------------------------------------------------------------

use $raw/Factset/fds_stata/sym_coverage, clear
keep fsym_id currency fsym_primary_equity_id fsym_primary_listing_id active_flag fref_security_type universe_type
save $temp/factset/sym_coverage_compact, replace

* ---------------------------------------------------------------------------------------------------
* Construct entity to primary CUSIP6 mapping
* ---------------------------------------------------------------------------------------------------

* Map each entity to a primary security: primary equities
use $raw/Factset/fds_stata/sym_sec_entity, clear
mmerge fsym_id using $raw/Factset/fds_stata/sym_cusip, unmatched(m)
drop if missing(cusip)
mmerge fsym_id using $temp/factset/sym_coverage_compact, unmatched(b)
drop _merge
gen is_primary_equity = 0
replace is_primary_equity = 1 if fsym_id == fsym_primary_equity_id
bysort factset_entity_id: egen has_primary_equity = max(is_primary_equity)
drop if has_primary_equity == 1 & is_primary_equity == 0
tempfile factset_map_step1
save `factset_map_step1', replace

* Collapse to entity-CUSIP6 links
use `factset_map_step1', clear
mmerge fsym_id using $temp/factset/sym_cusip_improved, unmatched(m)
drop _merge
drop fsym_primary_equity_id fsym_primary_listing_id
drop if missing(cusip)
gen issuer_number = substr(cusip, 1, 6)
gen sec_rank = 1
replace sec_rank = 0 if universe_type == "EQ"
sort issuer_number sec_rank
bys issuer_number: egen min_sec_rank = min(sec_rank)
drop if sec_rank > min_sec_rank
collapse (firstnm) universe_type, by(factset_entity_id issuer_number)
tempfile factset_map_step2
save `factset_map_step2', replace

* Map each entity to a primary issuer number: prioritize according to CGS AI file
use `factset_map_step2', clear
mmerge issuer_number using $cmns1/aggregation_sources/cgs_ai_aggregation, unmatched(m) umatch(cusip6) ukeep(ai_cusip6)
gen is_ai_parent = 0
replace is_ai_parent = 1 if issuer_number == ai_cusip6
bys factset_entity_id: egen has_ai_parent = max(is_ai_parent)
drop if has_ai_parent == 1 & is_ai_parent == 0

* Map each entity to a primary issuer number: give priority to equity CUSIP6
cap drop *ai*
cap drop _merge
gen issuer_rank = 1
replace issuer_rank = 0 if universe_type == "EQ"
bys factset_entity_id: egen min_issuer_rank = min(issuer_rank)
drop if issuer_rank != min_issuer_rank

* Map each entity to a primary issuer number: for anything remaining, we simply pick at random
set seed 1
bys factset_entity_id: gen _rand = runiform()
bys factset_entity_id: egen max_rand = max(_rand)
drop if float(max_rand) != float(_rand)
drop *rank *rand
drop if missing(factset_entity_id)
save $temp/fid_to_primary_cusip6, replace

* ---------------------------------------------------------------------------------------------------
* Construct raw aggregation file: for actual use
* ---------------------------------------------------------------------------------------------------

* Get the full list of matches
use $raw/Factset/fds_stata/ent_entity_structure, clear
drop factset_parent_entity_id
mmerge factset_entity_id using $raw/Factset/fds_stata/sym_sec_entity, unmatched(m)
drop if _merge == 1
mmerge factset_up_entity_id using $temp/fid_to_primary_cusip6, unmatched(m) umatch(factset_entity_id) uname(up_)
drop if missing(fsym_id)
drop _merge
mmerge fsym_id using $temp/factset/sym_cusip_improved, unmatched(m)
drop if missing(cusip)
gen cusip6 = substr(cusip, 1, 6)
rename up_issuer_number up_cusip6
save $temp/factset_full_matches, replace

* Collapse to C6-C6 level
use $temp/factset_full_matches, clear
bys cusip6 up_cusip6: keep if _n == 1
keep factset_entity_id factset_up_entity_id cusip6 up_cusip6
save $temp/factset_mapping_dupes, replace


* Do a simple de-duplication by keeping the most frequent parents *within each CUSIP6*
use $temp/factset_full_matches, clear
mmerge factset_entity_id using $raw/Factset/fds_stata/ent_entity_coverage, unmatched(m) ukeep(entity_name)
mmerge factset_up_entity_id using $raw/Factset/fds_stata/ent_entity_coverage, unmatched(m) umatch(factset_entity_id) ukeep(entity_name) uname(up_)
bys cusip6 up_cusip6: gen count = 1 if _n == 1
bys cusip6: egen N = total(count)
keep if N > 1
drop N
bys cusip6 up_cusip6: gen link_supporting_securities = _N
bys cusip6: egen max_supporters = max(link_supporting_securities)
drop if link_supporting_securities < max_supporters
drop count
bys cusip6 up_cusip6: gen count = 1 if _n == 1
bys cusip6: egen N = total(count)
cap drop _merge

* For anything remaining, we prefer *less* aggregation to more (to avoid mistakes), so we prioritize by name distance
order cusip6 up_cusip6 entity_name up_entity_name
format %40s *name*
sort cusip6
replace entity_name = upper(entity_name)
replace up_entity_name = upper(up_entity_name)
preserve
keep if N == 1
bys cusip6 up_cusip6: keep if _n == 1
keep factset_entity_id factset_up_entity_id cusip6 up_cusip6
save $temp/factset_dedupe_v2_p1, replace
restore
keep if N > 1
jarowinkler entity_name up_entity_name
bys cusip6: egen max_name_score = max(jarowinkler)
drop if jarowinkler < max_name_score
drop count N
bys cusip6 up_cusip6: gen count = 1 if _n == 1
bys cusip6: egen N = total(count)
preserve
keep if N == 1
bys cusip6 up_cusip6: keep if _n == 1
keep factset_entity_id factset_up_entity_id cusip6 up_cusip6
save $temp/factset_dedupe_v2_p2, replace

* For anything left, we pick at random
restore
keep if N > 1
bys cusip6 up_cusip6: keep if _n == 1
gen _rand = runiform()
bys cusip6: egen max_rand = max(_rand)
drop if _rand < max_rand
keep factset_entity_id factset_up_entity_id cusip6 up_cusip6
save $temp/factset_dedupe_v2_p3, replace

* Append all 
use $temp/factset_dedupe_v2_p1, clear
append using $temp/factset_dedupe_v2_p2
append using $temp/factset_dedupe_v2_p3
save $temp/factset_dedupe_v2, replace

* Construct consolidated mapping
use $temp/factset_mapping_dupes, clear
bys cusip6: gen N = _N
drop if N > 1
append using $temp/factset_dedupe_v2
drop N
bys cusip6: gen N = _N
assert N == 1
drop N
sort cusip6
save $temp/factset_mapping_raw_no_ambig, replace

* ---------------------------------------------------------------------------------------------------
* Complete Factset aggregation with all our pre-treatments
* ---------------------------------------------------------------------------------------------------

* Merge in names and country
use $temp/factset_mapping_raw_no_ambig, clear
mmerge factset_entity_id using $raw/Factset/fds_stata/ent_entity_coverage, unmatched(m) ukeep(entity_name)
mmerge factset_up_entity_id using $raw/Factset/fds_stata/ent_entity_coverage, unmatched(m) umatch(factset_entity_id) ukeep(entity_name) uname(up_)
format %40s *name*
replace entity_name = upper(entity_name)
replace up_entity_name = upper(up_entity_name)
mmerge factset_up_entity_id using $raw/Factset/fds_stata/ent_entity_coverage, unmatched(m) umatch(factset_entity_id) uname(up_)
format %40s *name*
gen issuer_number = cusip6
mmerge factset_entity_id using $raw/Factset/fds_stata/ent_entity_coverage, unmatched(m) umatch(factset_entity_id) ukeep(entity_type)
mmerge up_iso_country using $raw/Macro/Concordances/iso2_iso3.dta, umatch(iso2)
drop if _merge == 2
drop _merge
drop up_iso_country
rename iso3 country_bg
tempfile fds_treatments_step1
save `fds_treatments_step1', replace

* Drop any XSN (Factset does not have concept)
use `fds_treatments_step1', clear
mmerge issuer_number using $cmns1/aggregation_sources/cgs_compact_complete.dta, unmatched(m) ukeep(domicile) uname(cgs_)
drop if cgs_domicile == "XSN"
drop if strpos(up_entity_name, "EUROPEAN UNION")
drop if strpos(up_entity_name, "INTERNATIONAL INVESTMENT BANK")
drop if strpos(up_entity_name, "WORLD BANK")
drop if strpos(up_entity_name, "EUROPEAN INVESTMENT BANK")
drop if strpos(up_entity_name, "AFRICA FINANCE CORP")
drop if strpos(up_entity_name, "ASIAN DEVELOPMENT BANK")
drop if strpos(up_entity_name, "CORPORACION ANDINA DE FOMENTO")
drop if strpos(up_entity_name, "CENTRAL AMERICAN BANK FOR ECONOMIC INTEGRATION")
drop if strpos(up_entity_name, "INTER AMERICAN DEVELOPMENT BANK")
drop if strpos(up_entity_name, "NORDIC INVESTMENT BANK")
drop if strpos(up_entity_name, "INTERNATIONAL FINANCE CORP")
drop if strpos(up_entity_name, "EUROPEAN COMPANY FOR FINANCING OF RAILROAD ROLLING STOCK")
drop if strpos(up_entity_name, "COUNCIL OF EUROPE DEVELOPMENT BANK")
drop if strpos(up_entity_name, "EUROPEAN BANK FOR RECONSTRUCTION & DEVELOPMENT")
drop if strpos(up_entity_name, "EASTERN & SOUTHERN AFRICAN TRADE & DEVELOPMENT BANK")
drop if strpos(up_entity_name, "NORTH AMERICAN DEVELOPMENT BANK")
drop if strpos(up_entity_name, "CARIBBEAN DEVELOPMENT BANK")
drop if strpos(up_entity_name, "ISLAMIC DEVELOPMENT BANK")
format %40s *name*
drop *proper_name*
tempfile fds_treatments_step2
save `fds_treatments_step2', replace
save $temp/fds_treatments_step2, replace

* Drop corporate-to-government assignments; muni-to-corporate assignments; cross-border munis (the latter seem to have mistakes)
use `fds_treatments_step2', clear
mmerge cusip6 using $temp/figi_cusip6_sectype, unmatched(m)
mmerge up_cusip6 using $temp/figi_cusip6_sectype, unmatched(m) uname(up_) umatch(cusip6)
drop if entity_type != "GOV" &  up_entity_type == "GOV"
drop if consolidated_sector == "Muni" & up_consolidated_sector != "Muni" & ~missing(up_consolidated_sector )
drop if (inlist(consolidated_sector, "Muni", "Govt") | inlist(up_consolidated_sector, "Muni", "Govt")) & cgs_domicile != country_bg & ~missing(cgs_domicile) & ~missing(country_bg)
* Also drop AUS, BHR, SWE government bonds (several appear to have mistaken CUSIPs)
drop if entity_type == "GOV" & inlist(cgs_domicile, "AUS", "BHR", "SWE")
tempfile fds_treatments_step3
save $temp/fds_treatments_step3, replace

* Prep Dealogic, SDC, and MS for cross-check against COR
use $cmns1/aggregation_sources/dealogic_aggregation, clear
keep up_cusip6 country_dlg
rename up_cusip6 cusip6_up_bg
duplicates drop
replace country_dlg = "UAE" if country_dlg == "ARE"
save $temp/dlg_country_for_factset_build, replace


use $cmns1/aggregation_sources/sdc_aggregation, clear
drop cusip6 use_cusip
duplicates drop
rename up_cusip6 cusip6
mmerge cusip6 using $cmns1/aggregation_sources/sdc_country, unmatched(m)
drop if _merge == 1
drop if use_up_cusip == 0
keep cusip6 country_sdc
rename cusip6 cusip6_up_bg
replace country_sdc = "UAE" if country_sdc == "ARE"
save $temp/sdc_country_for_factset_build, replace


use $cmns1/aggregation_sources/morningstar_country, clear
rename cusip6 cusip6_up_bg
rename iso_country country_ms
save $temp/ms_country_for_factset_build, replace

* Find cases in which we want to over-ride HQ with COR
* Check against other data sources (if available) to make sure COR is also listed elsewhere (otherwise we don't use it)
* For MS, non-IRL, we don't count this if MS lists many subsidiaries in cor_geo_iso3 (since in these cases the MS reports
* might be picking up local subsidiaries, and we don't want to induce too much artificial correlation in the country reports)
local local_issuing_sub_treshold = 10
use $temp/fds_treatments_step3, clear
rename up_cusip6 cusip6_up_bg
sort issuer_number
mmerge up_iso_country_cor_georev using $raw/Macro/Concordances/iso2_iso3.dta, umatch(iso2) uname(cor_geo_) unmatched(m)
drop up_iso_country_cor_georev
drop _merge
replace country_bg = "UAE" if country_bg == "ARE"
replace cor_geo_iso3 = "UAE" if cor_geo_iso3 == "ARE"
keep if country_bg != cor_geo_iso3 & ~missing(cor_geo_iso3)
bys cusip6_up_bg cgs_domicile: gen n_local_subsidiaries = _N
bys cusip6_up_bg cgs_domicile: keep if _n == 1
mmerge cusip6_up_bg using $temp/dlg_country_for_factset_build, unmatched(m)
mmerge cusip6_up_bg using $temp/sdc_country_for_factset_build, unmatched(m)
order country_bg cor_geo_iso3 up_entity_name country_dlg country_sdc
gen override_hq = 0
replace override_hq = 1 if cor_geo_iso3 == country_dlg | cor_geo_iso3 == country_sdc
bys cusip6_up_bg: egen min_override_hq = min(override_hq)
bys cusip6_up_bg: keep if _n == 1
keep if min_override_hq == 1
keep cusip6_up_bg cor_geo_iso3
save $temp/factset_hq_overrides, replace

* Ensure that we have unique country_bg assignments within parent CUSIP6
use $temp/fds_treatments_step3, clear
rename up_cusip6 cusip6_up_bg
mmerge cusip6_up_bg using $temp/factset_hq_overrides
replace country_bg = cor_geo_iso3 if ~missing(cor_geo_iso3)
drop if missing(cusip6_up_bg)
bys cusip6_up_bg country_bg: gen counter = 1 if _n == 1
bys cusip6_up_bg: egen totCountry = total(counter)
keep if totCountry > 1
sort cusip6_up_bg
order cgs_domicile country_bg
gen bg_is_th = 0
replace bg_is_th = 1 if inlist(country_bg, $tax_haven_1)
replace bg_is_th = 1 if inlist(country_bg, $tax_haven_2)
replace bg_is_th = 1 if inlist(country_bg, $tax_haven_3)
replace bg_is_th = 1 if inlist(country_bg, $tax_haven_4)
replace bg_is_th = 1 if inlist(country_bg, $tax_haven_5)
replace bg_is_th = 1 if inlist(country_bg, $tax_haven_6)
replace bg_is_th = 1 if inlist(country_bg, $tax_haven_7)
replace bg_is_th = 1 if inlist(country_bg, $tax_haven_8)
bys cusip6_up_bg: egen has_bg_nth = min(bg_is_th)
bys cusip6_up_bg: egen has_bg_th = max(bg_is_th)
replace has_bg_nth = 1 - has_bg_nth
drop if has_bg_nth == 1 & bg_is_th == 1 & has_bg_th == 1
drop counter totCountry
bys cusip6_up_bg country_bg: gen counter = 1 if _n == 1
bys cusip6_up_bg: egen totCountry = total(counter)
preserve
keep if totCountry == 1
keep cusip6_up_bg country_bg
duplicates drop
rename country_bg consolidated_country_bg
save $temp/fds_country_bg_corrections, replace
restore
bys cusip6_up_bg country_bg: gen N = _N
order cusip6_up_bg country_bg N
sort cusip6_up_bg
keep if totCountry > 1
bys cusip6_up_bg country_bg: keep if _n == 1
bys cusip6_up_bg: egen maxN = max(N)
gen second_N = N if N != maxN
bys cusip6_up_bg: egen second_maxN = max(second_N)
gen max_margin = maxN / second_maxN
preserve
keep if max_margin > 2 & N == maxN & N > 2 
keep cusip6_up_bg country_bg
duplicates drop
rename country_bg consolidated_country_bg
save $temp/fds_country_bg_corrections_part2, replace
restore
mmerge cusip6_up_bg using $temp/fds_country_bg_corrections_part2
drop if _merge == 3
keep cusip6_up_bg
duplicates drop
gen to_drop = 1
save $temp/fds_country_bg_corrections_part3, replace

* Produce final aggregation file
use $temp/fds_treatments_step3, clear
rename up_cusip6 cusip6_up_bg
mmerge cusip6_up_bg using $temp/factset_hq_overrides
replace country_bg = cor_geo_iso3 if ~missing(cor_geo_iso3)
mmerge cusip6_up_bg using $temp/fds_country_bg_corrections, unmatched(m)
replace country_bg = consolidated_country_bg if ~missing(consolidated_country_bg)
drop consolidated_country_bg
mmerge cusip6_up_bg using $temp/fds_country_bg_corrections_part2, unmatched(m)
replace country_bg = consolidated_country_bg if ~missing(consolidated_country_bg)
drop consolidated_country_bg
mmerge cusip6_up_bg using $temp/fds_country_bg_corrections_part3, unmatched(m)
drop if to_drop == 1
keep cusip6 cusip6_up_bg country_bg up_entity_name
rename cusip6 issuer_number
bys issuer_number cusip6_up_bg: keep if _n == 1
save $cmns1/aggregation_sources/factset_aggregation_full_resolved, replace
rename cusip6_up_bg cusip6_bg
keep issuer_number cusip6_bg country_bg
rename issuer_number cusip6
drop if missing(cusip6_bg)
save $cmns1/aggregation_sources/factset_aggregation_v2, replace

* Also save names data
use $temp/fds_treatments_step3, clear
keep cusip6 entity_name
replace entity_name = upper(entity_name)
tempfile names_part1
save `names_part1', replace
use $temp/fds_treatments_step3, clear
keep up_cusip6 up_entity_name
rename up_cusip6 cusip6
rename up_entity_name entity_name
append using `names_part1'
bysort cusip6: keep if _n == 1
rename entity_name issuer_name
save $cmns1/aggregation_sources/factset_names, replace

* ---------------------------------------------------------------------------------------------------
* CMNS with Factset only
* ---------------------------------------------------------------------------------------------------

use $cmns1/aggregation_sources/factset_aggregation_full_resolved, clear
drop if missing(cusip6_up_bg)
rename up_entity_name issuer_name_up
order issuer_number issuer_name
save $temp/aggregation_fds_only, replace

* --------------------------------------------------------------------------------------------------
* Factset: HKG and LUX Companies
* This jobs prepares the Factset screen of Hong Kong and Luxembourg companies for aggregation
* --------------------------------------------------------------------------------------------------

* Hong Kong
import excel using $raw/Factset/workstation/HKG_Companies.xlsx, clear cellrange(A5) firstrow
drop if missing(CUSIP)
keep if EntityCountryHQ == "Hong Kong"
keep if EntityCountryRisk == "Hong Kong"
gsort - MktVal
keep if inlist(EntityCountryParentHQ, "Hong Kong", "@NA")
keep if _n <= 50
gen cusip6 = substr(CUSIP, 1, 6)
save $cmns1/aggregation_sources/factset_hkg_companies, replace

* Luxembourg
import excel using $raw/Factset/workstation/LUX_Companies.xlsx, clear cellrange(A5) firstrow
drop if missing(CUSIP)
keep if EntityCountryHQ == "Luxembourg"
keep if EntityCountryRisk == "Luxembourg"
gsort - MktVal
keep if inlist(EntityCountryParentHQ, "Luxembourg", "@NA")
keep if _n <= 50
gen cusip6 = substr(CUSIP, 1, 6)
save $cmns1/aggregation_sources/factset_lux_companies, replace

* Ireland
import excel using $raw/Factset/workstation/IRL_Companies.xlsx, clear cellrange(A5) firstrow
drop if missing(CUSIP)
drop if CUSIP == "@NA"
keep if EntityCountryHQ == "Ireland"
keep if EntityCountryRisk == "Ireland"
gsort - MktVal
keep if inlist(EntityCountryParentHQName, "Ireland", "@NA")
keep if _n <= 100
gen cusip6 = substr(CUSIP, 1, 6)
save $cmns1/aggregation_sources/factset_irl_companies, replace

* Netherlands
import excel using $raw/Factset/workstation/NLD_Companies.xlsx, clear cellrange(A5) firstrow
drop if missing(CUSIP)
drop if CUSIP == "@NA"
keep if EntityCountryHQ == "Netherlands"
keep if EntityCountryRisk == "Netherlands"
gsort - MktVal
keep if inlist(EntityCountryParentHQName, "Netherlands", "@NA")
keep if _n <= 300
gen cusip6 = substr(CUSIP, 1, 6)
save $cmns1/aggregation_sources/factset_nld_companies, replace

log close
