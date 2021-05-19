* ---------------------------------------------------------------------------------------------------
* Issuance_Distribution_Matrices: Produces issuance distribution matrices
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Issuance_Distribution_Matrices, replace

* ---------------------------------------------------------------------------------------------------
* Issuance distribution matrices for corporate bonds
* ---------------------------------------------------------------------------------------------------

cap restore 
use $cmns1/issuance_master/dealogic_factset_issuance_timeseries.dta, clear
keep if is_corp == 1
keep if active == 1

sort dcmdealdealid trancheid cusip year
bys dcmdealdealid trancheid	cusip year: gen aux_n = _n
keep if aux_n == 1

collapse (sum) value_cur_adj, by(residency nationality year)
sort year residency nationality
fillin year residency nationality
drop if residency == "" | nationality == ""
replace value_cur_adj = 0 if value_cur_adj == .
drop _fillin

bys year residency: egen total_residency = total(value_cur_adj)
gen ratio = value_cur_adj/total_residency
drop value_cur_adj total_residency
replace ratio = 0 if ratio == .
drop if year < 2005

qui mmerge residency using $raw/Macro/Concordances/countries_universe, umatch(iso3)
replace nationality = residency if nationality == ""
qui mmerge nationality using $raw/Macro/Concordances/countries_universe, umatch(iso3)
drop _merge
fillin year residency nationality
drop if year == .
drop if residency == ""
drop if nationality == ""
drop _fillin
replace ratio = 0 if ratio == . & residency != nationality 
replace ratio = 1 if ratio == . & residency == nationality

bys year residency: egen _tot_ratio = total(ratio)
replace ratio = 0 if _tot_ratio > 1.5 & nationality == residency 
drop _tot_ratio
bys year residency: egen _tot_ratio = total(ratio)
replace ratio = 1 if _tot_ratio == 0 & residency == nationality
drop _tot_ratio

save $cmns1/issuance_based_matrices/issuance_corporate_bonds_long.dta, replace

foreach i of num 2007/2017{
	preserve
	keep if year == `i'
	drop year
	qui reshape wide ratio, i(residency) j(nationality, string)
	egen total = rowtotal(ratio*)
	replace total=round(total,1)
	assert total ==1
	drop total
	rename ratio* *
	save $cmns1/issuance_based_matrices/Corporate_Bonds_dta/Issuance_Distribution_Matrix_Corporate_Bonds_`i'.dta, replace
    export excel using $cmns1/issuance_based_matrices/Corporate_Bonds_xls/Issuance_Distribution_Matrix_Corporate_Bonds_`i'.xls, firstrow(variables) replace
	restore
}

* ---------------------------------------------------------------------------------------------------
* Issuance distribution matrices for all bonds
* ---------------------------------------------------------------------------------------------------

cap restore 
use $cmns1/issuance_master/dealogic_factset_issuance_timeseries.dta, clear
keep if is_all_bonds == 1
keep if active == 1

sort dcmdealdealid trancheid cusip year
bys dcmdealdealid trancheid	cusip year: gen aux_n = _n
keep if aux_n == 1

collapse (sum) value_cur_adj, by(residency nationality year)
sort year residency nationality
fillin year residency nationality
drop if residency == "" | nationality == ""
replace value_cur_adj = 0 if value_cur_adj == .
by year residency: egen total_residency = total(value_cur_adj)
gen ratio = value_cur_adj/total_residency
drop value_cur_adj total_residency
drop _fillin
replace ratio = 0 if ratio == .
drop if year < 2007

qui mmerge residency using $raw/Macro/Concordances/countries_universe, umatch(iso3)
replace nationality = residency if nationality == ""
qui mmerge nationality using $raw/Macro/Concordances/countries_universe, umatch(iso3)

drop _merge
fillin year residency nationality
drop if year == .
drop if residency == ""
drop if nationality == ""

drop _fillin
replace ratio = 0 if ratio == . & residency != nationality 
replace ratio = 1 if ratio == . & residency == nationality

bys year residency: egen _tot_ratio = total(ratio)
replace ratio = 0 if _tot_ratio > 1.5 & nationality == residency 
drop _tot_ratio
bys year residency: egen _tot_ratio = total(ratio)
replace ratio = 1 if _tot_ratio == 0 & residency == nationality
drop _tot_ratio

save $cmns1/issuance_based_matrices/issuance_all_bonds_long.dta, replace

foreach i of num 2007/2019{
	preserve
	keep if year == `i'
	drop year
	qui reshape wide ratio, i(residency) j(nationality, string)
	egen total = rowtotal(ratio*)
	replace total=round(total,1)
	assert total ==1
	drop total
	rename ratio* *
	save $cmns1/issuance_based_matrices/All_Bonds_dta/Issuance_Distribution_Matrix_All_Bonds_`i'.dta, replace
    export excel using $cmns1/issuance_based_matrices/All_Bonds_xls/Issuance_Distribution_Matrix_All_Bonds_`i'.xls, firstrow(variables) replace
	restore
}

* ---------------------------------------------------------------------------------------------------
* Issuance distribution matrices for equities
* ---------------------------------------------------------------------------------------------------

cap restore
use $cmns1/equity_issuance_master/equity_issuance_master.dta, clear

collapse (sum) marketcap_usd, by(cgs_domicile country_bg year)
drop if cgs_domicile == "" | country_bg == ""
fillin cgs_domicile country_bg year
drop _fillin
replace marketcap_usd = 0 if marketcap_usd == .

mmerge cgs_domicile using $raw/Macro/Concordances/countries_universe, umatch(iso3)
replace country_bg = cgs_domicile if country_bg == ""
mmerge country_bg using $raw/Macro/Concordances/countries_universe, umatch(iso3)

drop _merge
fillin year	cgs_domicile country_bg
drop if year == .
drop if cgs_domicile == ""
drop if country_bg == ""
replace marketcap_usd = 0 if marketcap_usd == .

drop _fillin
bys year cgs_domicile: egen total_residency = total(marketcap_usd)

gen ratio = (marketcap_usd/total_residency)
replace ratio = 1 if total_residency == 0 & cgs_domicile == country_bg
drop marketcap_usd total_residency
replace ratio = 0 if ratio == .
rename country_bg nationality
rename cgs_domicile residency
save $cmns1/issuance_based_matrices/issuance_equity_long.dta, replace

foreach n of numlist 2007/2018 {
    display "`n'"
    preserve
    keep if year == `n'
    drop year
    qui reshape wide ratio, i(residency) j(nationality, string)
    rename ratio* *
    egen aux = rowtotal(ABW-ZWE)
    gen aux2 = round(aux)
    assert aux2 == 1
    drop aux aux2
    save $cmns1/issuance_based_matrices/Equity_dta/Issuance_Distribution_Matrix_Equities_`n'.dta, replace
    export excel $cmns1/issuance_based_matrices/Equity_xls/Issuance_Distribution_Matrix_Equities_`n'.xls, firstrow(variables) replace
    restore
}

log close
