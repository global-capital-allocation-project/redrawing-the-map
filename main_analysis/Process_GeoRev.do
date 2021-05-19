* ---------------------------------------------------------------------------------------------------
* Process_GeoRev: Imports and processes Factset GeoRev data on geography of sales
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Process_GeoRev, replace

global assetlist = "E B BC"
global treatment_list "baseline threshA threshB threshC threshD threshE"

* ---------------------------------------------------------------------------------------------------
* Import and reshape
* ---------------------------------------------------------------------------------------------------

* remove a few duplicated reports in the georev data
use $raw/Factset/fds_stata/gr_item, clear
collapse (lastnm) est_pct start_date end_date conf certainty_rank certainty_class, by(report_id iso_country)
mmerge iso_country using $raw/Macro/Concordances/iso2_iso3.dta, umatch(iso2)
save $sales/gr_merged_cusip_ent_agg_cusip6_iso3.dta, replace
replace iso3="RKS" if iso_co=="XK"
drop if iso_co=="AN" & _merge==1
replace iso3="RKS" if iso_co=="XK"
drop if iso_co=="AN" & _merge==1
replace iso3=iso_co if iso3==""
drop iso_co
rename iso3 iso_country
save $sales/gr_item_iso3.dta, replace

* reshaping the data
use $sales/gr_item_iso3.dta, clear
drop _merge start_date end_date
reshape wide est_pct conf certainty_rank certainty_class, i(report_id) j(iso_co) str
quietly {
    foreach x of varlist est* conf* certainty_rank*{
        replace `x'=0 if `x'==.
    }
}
foreach x of varlist certainty_class*{
    cap tostring `x', replace
    replace `x'="E" if missing(`x')
}
egen total=rowtotal(est*)
order total

* a few observations in the georev data do not add up: we drop these
drop if total < 99.95 | total > 110
save $sales/gr_item_iso3_wide.dta, replace

* ---------------------------------------------------------------------------------------------------
* Names files
* ---------------------------------------------------------------------------------------------------

* entity names from factset
use $raw/Factset/fds_stata/ent_entity_names.dta, clear
gsort factset_entity_id
by factset_entity_id: egen count=count(factset_entity_id)
gen short_dummy=0
replace short_dummy=1 if entity_name_type=="SHORT_NAME"
by factset_entity_id: egen f_short_dummy=max(short_dummy)
drop if entity_name_type~="SHORT_NAME" & f_short_dummy==1
keep if entity_name_type=="SHORT_NAME"
keep factset_entity_id entity_name_value
rename entity_name_value factset_short_name
save $sales/factset_short_name.dta, replace

* ---------------------------------------------------------------------------------------------------
* Merge all parts of GeoRev
* ---------------------------------------------------------------------------------------------------

* report and item: merge georev data to factset entity id's
use $raw/Factset/fds_stata/gr_report, clear
mmerge report_id using $sales/gr_item_iso3_wide.dta
keep if _merge==3
save $sales/gr_merged.dta, replace

* map entity ids to primary cusip6 code
use $sales/gr_merged.dta, clear
mmerge factset_entity_id using $cmns1/temp/factset_entity_id_to_primary_cusip6.dta
drop if _merge==2
save $sales/gr_merged_cusip.dta, replace

* map entity_id to factset ultimate parent entity id and ultimate parent cusip6 code
use $sales/gr_merged_cusip.dta, clear
mmerge factset_entity_id using $raw/Factset/fds_stata/ent_entity_structure.dta, ukeep(factset_ultimate_parent_entity_i)
drop if _merge==2
mmerge factset_ultimate_parent_entity_i using $cmns1/temp/factset_entity_id_to_primary_cusip6.dta, umatch(factset_entity_id) uname(factset_up_)
drop if _merge==2
save $sales/gr_merged_cusip_ent.dta, replace

* merging in the cmns aggregation file for residency and nationality
use $sales/gr_merged_cusip_ent.dta, clear
mmerge issuer_number using $cmns1/country_master/cmns_aggregation, ukeep(cgs_domicile cusip6_up_bg issuer_name issuer_name_up country_bg)
drop if _merge==2
save $sales/gr_merged_cusip_ent_agg.dta, replace

* construct unique cusip6 to entity_id map
use $cmns1/temp/factset_entity_id_to_primary_cusip6.dta, clear
duplicates drop issuer_number, force 
save $cmns1/temp/cusip6_to_factset_entity_id_unique.dta, replace

* map cusip6_up_bg to factset_entity_id: this is to ensure consistency with issuer treatment in aggregation data
use $sales/gr_merged_cusip_ent_agg.dta, clear
mmerge cusip6_up_bg using $cmns1/temp/cusip6_to_factset_entity_id_unique.dta, umatch(issuer_number) ukeep(factset_entity_id) uname(up_)
drop if _merge==2
save $sales/gr_merged_cusip_ent_agg_cusip6.dta, replace

* merge in entity names
use $sales/gr_merged_cusip_ent_agg_cusip6.dta, clear
mmerge factset_entity_id using $sales/factset_short_name.dta
drop if _merge==2
save $sales/gr_merged_cusip_ent_agg_cusip6_names.dta, replace

* ---------------------------------------------------------------------------------------------------
* Create a dataset with unique entity IDs
* ---------------------------------------------------------------------------------------------------

* now we want to retain just one report per firm per quarter
use $sales/gr_merged_cusip_ent_agg_cusip6_names.dta, clear
gen period_end_date_d=date(period_end_date, "YMD")
format period_end_date_d %td
gen period_end_date_q=qofd(period_end_date_d)
format period_end_date_q %tq
gen period_end_date_y=yofd(period_end_date_d)
format period_end_date_y %ty
order period_end_date_d period_end_date_q period_end_date_y
drop start_date end_date
duplicates drop

* drop any duplicates on every meaningful entry
duplicates drop period_end_date_q factset_entity_id total est*, force 
gen diff_100=abs(100-total)
bysort factset_entity_id period_end_date_q: egen closest=min(diff_100)
keep if closest==diff_100

* keep the report that has the least allocation to unspecified destination
egen problem_iso=rowtotal(est_pctXP est_pctXR est_pctXS est_pctXT est_pctXW est_pctXY est_pctXZ)
bysort factset_entity_id period_end_date_q: egen least_problem=min(problem_iso)
keep if problem_iso==least_problem
drop problem_iso least_problem

* alternatively, keep the report closer to the end of quarter
bysort factset_entity_id period_end_date_q: egen last_day=max(period_end_date_d)
keep if last_day==period_end_date_d
drop last_day

* alternatively, keep the later report_id
format report_id %12.0f
bysort factset_entity_id period_end_date_q: egen long last_report=max(report_id)
format last_report %12.0f
bysort factset_entity_id period_end_date_q: egen count=count(factset_entity_id)
drop if last_report~=report_id & count>1
drop count
bysort factset_entity_id period_end_date_q: egen count=count(factset_entity_id)

* adjustment for rounding error: we round estimates to sum exactly to 100%
quietly {
foreach x of varlist est_* {
    replace `x'=100*`x'/total
}
}
egen total2=rowtotal(est*)
drop total*
save $sales/gr_unique_factset_entity_id.dta, replace

* collapse the dataset to have unique observations at the cusip6-quarter level
use $sales/gr_unique_factset_entity_id.dta, clear
keep if issuer_num~=""
cap drop closest
bysort issuer_num period_end_date_q: egen closest=min(diff_100)
keep if closest==diff_100

* keep the report that has the least allocation to unspecified destination
egen problem_iso=rowtotal(est_pctXP est_pctXR est_pctXS est_pctXT est_pctXW est_pctXY est_pctXZ)
bysort issuer_num period_end_date_q: egen least_problem=min(problem_iso)
keep if problem_iso==least_problem
drop problem_iso least_problem

* alternatively, keep the report closer to the end of quarter
bysort issuer_num period_end_date_q: egen last_day=max(period_end_date_d)
keep if last_day==period_end_date_d
drop last_day

* alternatively, keep the later report_id
format report_id %12.0f
cap drop last_report 
cap drop count
bysort issuer_num period_end_date_q: egen long last_report=max(report_id)
format last_report %12.0f
bysort issuer_num period_end_date_q: egen count=count(issuer_num)
drop if last_report~=report_id & count==2
drop count
bysort issuer_num period_end_date_q: egen count=count(issuer_num)
save $sales/gr_unique_cusip6.dta, replace

* collapse to annual frequency
use $sales/gr_unique_cusip6.dta, clear
bysort issuer_num period_end_date_y: egen last_q=max(period_end_date_q)
keep if period_end_date_q==last_q
save $sales/gr_unique_cusip6_y.dta, replace

* if a particular issuer has missing years, we carry forward data from the latest available year
use $sales/gr_unique_cusip6_y.dta, clear
egen cusip6_id=group(issuer_num)
duplicates drop cusip6_id period_end_date_y, force
tsset cusip6_id period_end_date_y
tsfill
gen original=1 if report_id~=.
replace original=0 if report_id==.
drop period_end_date_d period_end_date_q report_id period_start_date period_end_date last_report count closest last_q diff_100 _merge
by cusip6_id: carryforward factset_entity_id issuer_num est* conf* certainty_rank* certainty_class* universe_type factset_ultimate_parent_entity_i factset_up_issuer_number factset_up_universe_type issuer_name cgs_domicile cusip6_up_bg country_bg issuer_name_up up_factset_entity_id factset_short_name, replace
rename period_end_date_y year 
save $sales/gr_unique_cusip6_y_filled.dta, replace
save $sales/gr_unique_cusip6_y_filled_baseline.dta, replace

* versions with confidence thresholds, assigning unattributed sales to residency
local certainlist =""
foreach class in A B C D E {
    use $sales/gr_unique_cusip6_y_filled.dta, clear
        if "`class'"=="A" {
        local certainlist `""A" "'
        }
        if "`class'"=="B" {
        local certainlist `""A","B" "'
        }
        if "`class'"=="C" {
        local certainlist `""A","B","C" "'
        }
        if "`class'"=="D" {
        local certainlist `""A","B","C","D" "'
        }    
        if "`class'"=="E" {
        local certainlist `""A","B","C","D","E" "'
        } 
        
    gen tores=0
    quietly {
    foreach x of varlist est_pct* {
        local iso=subinstr("`x'","est_pct","",.)
        replace tores=tores+`x' if inlist(certainty_class`iso',`certainlist')==0
        replace `x'=0 if inlist(certainty_class`iso',`certainlist')==0
    }
    levelsof cgs_dom, local(iso)
    foreach x of local iso {
        cap gen est_pct`x'=0
        display "`x'"
        replace est_pct`x'=est_pct`x'+tores if cgs_dom=="`x'"
    }
    }
    save $sales/gr_unique_cusip6_y_filled_thresh`class'.dta, replace
}

* ---------------------------------------------------------------------------------------------------
* List of all CUSIPs in the data
* ---------------------------------------------------------------------------------------------------

* in the following we fully enumerate all mappings from individual cusip6 codes to
* ultimate parent cusip6 codes as provided by the factset data, in order to ensure
* that we do not miss any potential merges when merging georev with holdings data

* list of all cusips in factset data, mapped to entity codes
use $raw/Factset/fds_stata/sym_cusip.dta,clear
mmerge fsym_id using $raw/Factset/fds_stata/sym_sec_entity.dta
keep if _merge==3
drop _merge
gen cusip6=substr(cusip,1,6)
drop fsym_id cusip
duplicates drop cusip6 factset_entity_id, force
save $sales/cusip6_factset_entity_id.dta, replace
mmerge factset_entity_id using $raw/Factset/fds_stata/ent_entity_structure.dta, ukeep(factset_parent_entity_id)
drop if _merge==2
mmerge factset_parent_entity_id using $cmns1/temp/factset_entity_id_to_primary_cusip6.dta, umatch(factset_entity_id) uname(factset_parent_)
drop if _merge==2
mmerge factset_entity_id using $raw/Factset/fds_stata/ent_entity_structure.dta, ukeep(factset_ultimate_parent_entity_i)
drop if _merge==2
mmerge factset_ultimate_parent_entity_i using $cmns1/temp/factset_entity_id_to_primary_cusip6.dta, umatch(factset_entity_id) uname(factset_up_)
drop if _merge==2
save $sales/full_cusip_list.dta, replace

* list of all cusips in georev data
use $sales/gr_unique_cusip6_y_filled.dta, clear
keep factset_entity_id issuer_number
duplicates drop
save $sales/ids_in_georev.dta, replace
use $sales/ids_in_georev.dta, clear
duplicates drop issuer_number, force
save $sales/cusips_in_georev.dta, replace

* keep every single unique cusip6 to ultimate issuer map for issuer numbers that appear in georev data
use $sales/full_cusip_list.dta, clear
drop _merge
duplicates drop cusip6 factset_up_issuer_number, force
mmerge factset_up_issuer_number using $sales/cusips_in_georev.dta, umatch(issuer_number) ukeep(issuer_number)
keep if _merge==3
drop _merge
drop if factset_up_issuer_number==""
keep cusip6 factset_up_issuer_number
bysort cusip6: gen n=_n
reshape wide factset_up_issuer_num, i(cusip6) j(n)
save $sales/cusip6_structure_wide.dta, replace

* ---------------------------------------------------------------------------------------------------
* Holdings summary
* ---------------------------------------------------------------------------------------------------

* create "all bonds" category from holdings summary
use $cmns1/holdings_master/mns_issuer_summary, clear
replace asset_class="B" if regexm(asset_class,"Bonds")==1
keep if asset_class=="B"
save $sales/mns_issuer_summary_B, replace

* append "all bonds" category to rest of holdings data
use $cmns1/holdings_master/mns_issuer_summary, clear
replace asset_class="E" if asset_class=="Equity"
replace asset_class="BC" if regexm(asset_class,"Corporate")==1
keep if asset_class=="E" | asset_class=="BC"
append using $sales/mns_issuer_summary_B
save $sales/mns_issuer_summary_for_merge, replace

* ---------------------------------------------------------------------------------------------------
* Merge holdings with GeoRev
* ---------------------------------------------------------------------------------------------------

* iterate over treatments
foreach treatment of global treatment_list {

    di "Merging holdings and GeoRev: `treatment'"

    * merge holdings with enumerated mappings to ultimate parent cusip6 via factset data
    use $sales/mns_issuer_summary_for_merge, clear
    mmerge issuer_number using $sales/cusip6_structure_wide.dta, umatch(cusip6) ukeep(factset_up_issuer_number1 factset_up_issuer_number2)
    drop if _merge==2

    * data in usd billions
    replace market=market/(10^9)

    * merge in georev reports (which we have made unique at the issuer-quarter level)
    mmerge issuer_num year using $sales/gr_unique_cusip6_y_filled_`treatment'.dta
    drop if _merge==2
    gen name_short=factset_short_name
    replace name_short=substr(issuer_name,1,20) if name_short==""
    replace name_short=substr(issuer_name_up,1,20) if name_short==""
    rename _merge merge_composite1 

    * loop over mappings to ultimate parent cusip6: we always prioritize immediate mappings from issuer number
    * to factset entity id, and if those are not available we use these further mappings to parent entity in
    * order to link holdings to georev
    local i=2
    foreach identifier in cusip6_up_bg factset_up_issuer_number1 factset_up_issuer_number2 {

        * merge in georev data with given parent identifier mapping
        local j=`i'-1
        rename `identifier' `identifier'_temp
        mmerge  `identifier'_temp year using $sales/gr_unique_cusip6_y_filled_`treatment'.dta, umatch(issuer_num year) uname("georevup_")
        drop if _merge==2

        * only keep the match with highest priority
        foreach x of varlist est_pct* factset_short_name {
           quietly replace `x'=georevup_`x' if _merge==3 & merge_composite`j'==1
        }
        drop georevup_*
        rename _merge merge`i'
        gen merge_composite`i'=max(merge`i',merge_composite`j')
        local i=`i'+1

    }

    * sanity check: we make sure we don't count something as a successful merge if we can't
    * account for at least 80% of a firm's revenue via georev
    order est*, last
    egen total=rowtotal(est*)
    foreach x of varlist est_pct* {
        quietly replace `x'=. if total<80
    }
    drop total
    egen total=rowtotal(est_pct*)

    * we keep all revenues in country of residency if we don't have a successful match from georev
    gen flag_impute=0
    foreach x of varlist est_pct* {
        local temp=subinstr("`x'","est_pct","",.)
        replace flag_impute=1 if cgs_dom=="`temp'" & total==0
        replace `x'=100 if cgs_dom=="`temp'" & total==0
        replace `x'=0 if `x'==.
    }
    drop total

    * check how much of sales is unallocated for each firm: if there's any, 
    * we assign these to place of residency
    gen unallocated_total=0
    foreach x of varlist est_pct* {
        local temp=subinstr("`x'","est_pct","",.)
        if length("`temp'")==2 {  
            replace unallocated_total=unallocated_total+`x'
            drop `x'
        }  
    }
    foreach x of varlist est_pct* {
         local temp=subinstr("`x'","est_pct","",.)
         replace `x'=`x'+unallocated_total if cgs_dom=="`temp'"
    }

    * scale data so that we use shares that all add up to 1
    egen total=rowtotal(est_pct*)
    foreach x of varlist est_pct* {
        replace `x'=`x'/total
    }

    * create market values weighted by the georev shares
    foreach x of varlist est_pct* {
        local temp=subinstr("`x'","pct_","",.)
        gen mv_`temp'=marketvalue_usd*`x'
    }
    save $sales/full_merge_`treatment'.dta, replace

}

* ---------------------------------------------------------------------------------------------------
* List of countries for sales matrices
* ---------------------------------------------------------------------------------------------------

* list of countries that are in matched bonds holdings
use $sales/full_merge_baseline.dta if asset=="B", clear
keep cgs_dom
duplicates drop
save $sales/country_list_B.dta, replace

* list of countries that are in matched equity holdings
use $sales/full_merge_baseline.dta if asset=="E", clear
keep cgs_dom
duplicates drop
save $sales/country_list_E.dta, replace

* append these country lists, which we use to generate sales-weighted matrices
use $sales/country_list_B.dta, clear
append using $sales/country_list_E.dta
duplicates drop
gen temp=1
save $sales/clist1.dta, replace
use $sales/clist1.dta, clear
mmerge temp using $sales/clist1.dta, uname(x_)
drop temp _merge
rename x_c iso_country_code
save $sales/country_list_merge.dta, replace

* ---------------------------------------------------------------------------------------------------
* Construct sales matrices
* ---------------------------------------------------------------------------------------------------

* contruct the sales matrices that we use for reallocations: one per each year, investor country, asset class
foreach treatment of global treatment_list {
forvalues y=2007/2017 {
foreach asset of global assetlist {

if "`treatment'" == "baseline" {
    global inv_list "AUS CAN CHE DNK EMU GBR NOR SWE USA"
}
else {
    global inv_list "USA EMU"
}

foreach dom of global inv_list {
    
    display "`treatment': `asset'_`dom'_`y'"
    
    qui {
        
        * load in the appropriate merged holdings-georev data
        use mv* cgs_dom asset Dom year using $sales/full_merge_`treatment'.dta if asset=="`asset'" & Dom=="`dom'" & year==`y', clear
        
        * shape this to be a long matrix (in market values) with residency on the rows and sales destination on the columns
        collapse (sum) mv*, by(cgs_dom)
        rename mv_est_pct* mv_*    
        reshape long mv_, i(cgs_dom) j(iso_country_code) str
        
        * merge in full list of countries to ensure homogeneous shape of matrices, with zero values for missings
        qui mmerge cgs_dom iso_country_code using $sales/country_list_merge.dta
        replace mv=0 if mv==.
        drop _merge
        save $sales_matrices/`dom'_`asset'_`y'_sales_long_`treatment'.dta, replace
        
        * go to wide matrix form
        qui reshape wide mv, i(cgs_dom) j(iso_co) str
        
        * convert market values to shares
        egen total=rowtotal(mv*)
        foreach x of varlist mv* {
            local temp=subinstr("`x'","mv_","",.)
            replace `x'=1 if cgs_dom=="`temp'" & total==0
        }
        egen total2=rowtotal(mv*)
        foreach x of varlist mv* {
            local temp=subinstr("`x'","mv_","",.)
            gen pct_`temp'=100*`x'/total2
            replace pct_`temp'=0 if pct_`temp'==.
        }

        * we are done: save the matrix
        drop mv* total*
        renpfix pct_
        save $sales_matrices/`dom'_`asset'_`y'_sales_`treatment'.dta, replace
        
    }
}
}
}
}

log close
