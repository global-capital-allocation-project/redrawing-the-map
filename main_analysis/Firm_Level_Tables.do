* ---------------------------------------------------------------------------------------------------
* Firm_Level_Tables: This job produces a list of the largest tax haven bond financing subsidiaries in
* the BRICS countries (Table 6) and of the largest Chinese firms in equity holdings (Table 7) 
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Firm_Level_Tables, replace

* ---------------------------------------------------------------------------------------------------
* Largest tax haven bond bond financing subsidiaries in the BRICS countries
* ---------------------------------------------------------------------------------------------------

* Get bond issuance data
use $cmns1/issuance_master/dealogic_factset_issuance_timeseries.dta, clear
keep if year==2017
duplicates drop cusip, force 
qui mmerge cusip6 using $cmns1/country_master/cmns_aggregation, umatch(issuer_number) ukeep(issuer_name cgs_dom cusip6_up_bg country_bg issuer_name_up)
keep if _merge==3
save $cmns1/temp/issuance_aggregation_bonds.dta, replace

* Add in asset classes
use cusip class_code2 using $cmns1/security_master/gcap_security_master.dta, clear
qui mmerge cusip using $cmns1/temp/issuance_aggregation_bonds.dta
keep if _merge==3
save $cmns1/temp/issuance_aggregation_ac.dta, replace

* Generate BRICS bonds table
use $cmns1/temp/issuance_aggregation_ac.dta, clear
keep if class=="BC"
keep if country_bg=="BRA" | country_bg=="RUS" | country_bg=="IND" | country_bg=="CHN" | country_bg=="ZAF"
keep if inlist(cgs_dom,$tax_haven_1) | inlist(cgs_dom,$tax_haven_2) |  inlist(cgs_dom,$tax_haven_3) |  inlist(cgs_dom,$tax_haven_4) |  inlist(cgs_dom,$tax_haven_5) |  inlist(cgs_dom,$tax_haven_6) |  inlist(cgs_dom,$tax_haven_7) | inlist(cgs_dom, $tax_haven_8)
collapse (sum) value_cur_adj, by(cgs_dom cusip6 cusip6_up_bg country_bg issuer_name issuer_name_up)
replace value=value/1000
gsort -value_cur
rename value source_dest_amt
gen negval=-source_dest_amt
bysort country_bg: egen rank=rank(negval), unique
sort country_bg rank
order country_bg rank cusip6 issuer_name cgs_dom source_dest
keep country_bg rank cusip6 issuer_name cgs_dom source_dest 
keep if rank<=5
replace issuer_name=proper(issuer_name)
format %50s issuer_name
save $cmns1/tables/firms_brics_debt, replace

* ---------------------------------------------------------------------------------------------------
* Largest Chinese firms in equity holdings
* ---------------------------------------------------------------------------------------------------

* Industry classification data from Factset API
import excel $raw/Factset/workstation/china_industries_factset.xlsx, sheet("Sheet1") firstrow clear
save $cmns1/temp/china_industries_factset.dta, replace

* Get issuance data
use $cmns1/equity_issuance_master/equity_issuance_master.dta, clear
keep if year==2017 
keep if country_bg=="CHN"
gsort -marketcap
replace marketcap=marketcap*1000
drop if missing(cusip)
duplicates drop cusip, force
save $cmns1/temp/china_equity_issuance.dta, replace

* Get holdings data
use $cmns1/holdings_master/mns_security_summary.dta, clear
keep if asset_class=="Equity"
keep if country_bg=="CHN"
collapse (sum) marketvalue_usd (firstnm) issuer_name cgs_domicile  issuer_name_up tax_haven, by(year cusip country_bg)
replace market=market/(10^9)
gsort -market
save $cmns1/temp/china_equity_holdings, replace

* Generate China table
use $cmns1/temp/china_equity_holdings, clear
keep if year == 2017
collapse (sum) marketvalue_usd (firstnm) issuer_name_up, by(cusip cgs_domicile country_bg)
qui mmerge cusip using $cmns1/temp/china_equity_issuance.dta, ukeep(marketcap_usd)
keep if _merge==3
replace issuer_name_up=proper(issuer_name_up)
keep if inlist(cgs_dom, $tax_haven_1) | inlist(cgs_dom, $tax_haven_2) |  inlist(cgs_dom, $tax_haven_3) |  inlist(cgs_dom, $tax_haven_4) |  inlist(cgs_dom, $tax_haven_5) |  inlist(cgs_dom, $tax_haven_6) |  inlist(cgs_dom, $tax_haven_7) | inlist(cgs_dom,  $tax_haven_8) | cgs_dom == "CHN"
replace cusip="56752108" if regexm(issuer_name_up,"Baidu")==1
qui mmerge cusip using $cmns1/temp/china_industries_factset, ukeep(industry)
keep if _merge==3
gsort -marketvalue_usd
gen holdings_rank=_n
order holdings_rank issuer_name_up cgs_dom marketcap_usd industry 
gsort -marketvalue_usd
keep holdings_rank issuer_name_up cgs_dom marketcap_usd industry 
keep if holdings_rank<=25
save $cmns1/tables/firms_china_equity, replace

log close
