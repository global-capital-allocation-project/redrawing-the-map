* ---------------------------------------------------------------------------------------------------
* SDC: This file build the ultimate-parent aggregation data from SDC Platinum, which is
* used as input in the CMNS aggregation procedure.
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Build_SDC, replace

* --------------------------------------------------------------------------------------------------
* Read in the raw SDC data, downloaded in Excel format
* --------------------------------------------------------------------------------------------------

* Program to clean nation field
cap program drop clean_nation_field
program define clean_nation_field
	replace iso="ANT" if Nation=="Neth Antilles"
	replace iso="BOL" if Nation=="Bolivia"
	replace iso="BIH" if Nation=="Bosnia"
	replace iso="VGB" if Nation=="British Virgin"
	replace iso="CZE" if Nation=="Czechoslovakia"
	replace iso="DOM" if Nation=="Dominican Rep"
	replace iso="GNQ" if Nation=="Equator Guinea"
	replace iso="PYF" if Nation=="Fr Polynesia"
	replace iso="IRN" if Nation=="Iran"
	replace iso="IRL" if Nation=="Ireland-Rep"
	replace iso="CIV" if Nation=="Ivory Coast"
	replace iso="LAO" if Nation=="Laos"
	replace iso="MAC" if Nation=="Macau"
	replace iso="MKD" if regexm(Nation,"Macedonia")==1
	replace iso="MHL" if regexm(Nation,"Marshall I")==1
	replace iso="FSM" if regexm(Nation,"Micronesia")==1
	replace iso="MDA" if regexm(Nation,"Moldova")==1
	replace iso="XSN" if regexm(Nation,"Multi-National")==1
	replace iso="PNG" if regexm(Nation,"Papua N")==1
	replace iso="RUS" if regexm(Nation,"Russia")==1
	replace iso="SVK" if regexm(Nation,"Slovak")==1
	replace iso="KOR" if regexm(Nation,"South Korea")==1
	replace iso="LCA" if regexm(Nation,"St Lucia")==1
	replace iso="SUR" if regexm(Nation,"Surinam")==1
	replace iso="SYR" if regexm(Nation,"Syria")==1
	replace iso="TWN" if regexm(Nation,"Taiwan")==1
	replace iso="TTO" if regexm(Nation,"Trinidad")==1
	replace iso="TCA" if regexm(Nation,"Turks/Caicos")==1
	replace iso="UAE" if regexm(Nation,"Utd Arab")==1
	replace iso="VEN" if regexm(Nation,"Venezuela")==1
	replace iso="VNM" if regexm(Nation,"Vietnam")==1
end

* Import the raw SDC XLSX files; bonds issues
fs "$sdc_bonds/*.xlsx"
foreach file in `r(files)' {
	display "`file'"
	import excel $sdc_bonds/`file', sheet("Request 2") allstring firstrow clear
	local temp=subinstr("`file'",".xlsx","",.)
	gen file="`temp'"
	display "`temp'"
	save $sdc_dta/`temp'.dta, replace
}

* Import the raw SDC XLS files; bonds issues
fs "$sdc_bonds/*.xls"
foreach file in `r(files)' {
	display "`file'"
	import excel $sdc_bonds/`file', sheet("Request 2") allstring firstrow clear
	local temp=subinstr("`file'",".xls","",.)
	gen file="`temp'"
	display "`temp'"
	save $sdc_dta/`temp'.dta, replace
}

* Country names
import excel $raw/Macro/Concordances/country_and_currency_codes.xlsx, sheet("Data") firstrow clear
keep country_name iso_country_code
duplicates drop
save $cmns1/temp/country_names, replace

* Append the SDC files
clear
fs "$sdc_dta/*.dta"
foreach file in `r(files)' {
	append using $sdc_dta/`file', force
}

* Update CUSIP9 using CUSIP master file
mmerge ISIN using $temp/cgs/isin_to_cusip, umatch(isin)
rename cusip9 cusip9_cgs
drop if _merge==2
rename Digit cusip9_sdc
gen cusip6_cgs = substr(cusip9_cgs,1,6)
gen cusip6_sdc = substr(cusip9_sdc,1,6)
order CUSIP UltimateParentCUSIP cusip6*
gen data_date=subinstr(file,"_gv","",.)
split data_date,p("Q")
replace data_date2="Q"+data_date2 if data_date1==""
replace data_date3="Q"+data_date3 if data_date1==""
replace data_date1=subinstr(data_date1,"s","",.)
replace data_date2="Q1"+data_date1 if data_date2==""
replace data_date3="Q4"+substr(data_date1,1,3)+"9" if data_date3==""
gen start_date_str=substr(data_date2,3,6)+substr(data_date2,1,2)
gen end_date_str=substr(data_date3,3,6)+substr(data_date3,1,2)
drop data_date*
gen end_date=quarterly(end_date_str,"YQ",2017)
format end_date %tq
gen start_date=quarterly(start_date_str,"YQ",2017)
format start_date %tq
gen issue_date=date(IssueDate,"DMY")
gen maturity_date=date(FinalMat,"DMY")
format issue_date %td
format maturity_date %td
drop E U AC AG

* Prepare SDC currency mapping file
preserve
tempfile curr
drop if cgs_cu=="" | Cur==""
gen n=_n
collapse (count) n, by(cgs Currency)
drop if cgs==""
drop if Curr==""
bysort Curr: egen max=max(n)
keep if n==max
gen counter = 1 if !missing(Curr)
bysort Curr: egen curr_count=sum(counter)
drop counter
drop if Curr=="ESC" & cgs~="PTE"
drop if Curr=="UP" & cgs=="USD"
drop curr_count
keep Curr cgs_curr
save $sdc_additional/sdc_cgs_currmapping.dta, replace

* Merge in the currency mapping data
restore
mmerge Curr using $sdc_additional/sdc_cgs_currmapping.dta, uname(cgs_)
replace Curr=cgs_cgs_curr if cgs_cgs_curr~=""
drop cgs_cgs_curr

* Clean nation field
preserve
tempfile nation
keep Nation
duplicates drop
drop if Nation==""
sort Na
mmerge Nation using $cmns1/temp/country_names.dta, umatch(country_n)
clean_nation_field
drop if iso==""
drop _merge
save "`nation'"
restore
mmerge Nation using "`nation'"
drop if _merge==2
tab Nation if iso==""
save $sdc_datasets/sdc_bonds_appended, replace

* Import XLSX files for SDC equities
fs "$sdc_equities/Equities*.xlsx"
foreach file in `r(files)' {
	display "`file'"
	import excel $sdc_equities/`file', sheet("Request 2") allstring firstrow clear
	local temp=subinstr("`file'",".xlsx","",.)
	gen file="`temp'"
	display "`temp'"
	save $sdc_eqdta/`temp'.dta, replace
}	

* Append the files
clear
fs "$sdc_eqdta/Equities*.dta"
foreach file in `r(files)' {
	append using $sdc_eqdta/`file', force
}

* Update CUSIP9 using CUSIP master file
mmerge ISIN using $temp/cgs/isin_to_cusip, umatch(isin) ukeep(cusip9)
rename cusip9 cusip9_cgs
drop if _merge==2
rename Digit cusip9_sdc
gen cusip6_cgs = substr(cusip9_cgs,1,6)
gen cusip6_sdc = substr(cusip9_sdc,1,6)
order CUSIP UltimateParentCUSIP cusip6*
gen data_date=subinstr(file,"Equities_","",.)
replace data_date="1970" if regexm(data_date,"1970")==1
replace data_date="2000" if regexm(data_date,"2000")==1
replace data_date="2008" if regexm(data_date,"2008")==1
replace data_date="2013" if regexm(data_date,"2013")==1
destring data_date, replace
gen start_date=data_date
replace start_date=qofd(dofy(start_date))
format start_date %tq

* Clean nation field
preserve
tempfile nation
keep Nation
duplicates drop
drop if Nation==""
sort Na
mmerge Nation using $cmns1/temp/country_names.dta, umatch(country_n)
clean_nation_field
drop if iso==""
drop _merge
save "`nation'"
restore
mmerge Nation using "`nation'"
drop if _merge==2 | (_merge==-1 & Issuer=="")
tab Nation if iso==""
save $sdc_datasets/sdc_eq_appended, replace

* Import raw XLSX data from SDC loans
fs "$sdc_loans/*.xlsx"
foreach file in `r(files)' {
	display "`file'"
	import excel $sdc_loans/`file', sheet("Request 2") allstring firstrow clear
	local temp=subinstr("`file'",".xlsx","",.)
	gen file="`temp'"
	display "`temp'"
	save $sdc_loandta/`temp'.dta, replace
}	

* Append the files
clear
fs "$sdc_loandta/*.dta"
foreach file in `r(files)' {
	append using $sdc_loandta/`file', force
}

* Merge in currency tags
mmerge Curr using $sdc_additional/sdc_cgs_currmapping.dta, uname(cgs_)
replace Curr=cgs_cgs_curr if cgs_cgs_curr~=""
drop cgs_cgs_curr

* Merge in country tags and save output
preserve
tempfile nation
keep Nation
duplicates drop
drop if Nation==""
sort Na
mmerge Nation using $cmns1/temp/country_names.dta, umatch(country_n)
clean_nation_field
drop if iso==""
drop _merge
save "`nation'"
restore
mmerge Nation using "`nation'"
drop if _merge==2
tab Nation if iso==""
save $sdc_datasets/sdc_loans_appended, replace

* --------------------------------------------------------------------------------------------------
* Append all the raw SDC data, downloaded in Excel format
* --------------------------------------------------------------------------------------------------

* Append all the raw SDC data
use $sdc_datasets/sdc_bonds_appended, clear
gen source_type = "bond"
append using $sdc_datasets/sdc_eq_appended
replace source_type = "equity" if missing(source_type)
append using $sdc_datasets/sdc_loans_appended
replace source_type = "loan" if missing(source_type)
drop if missing(CUSIP)

* Rearrange the data 
replace cusip9_sdc = LoanCusip if missing(cusip9_sdc) & ~missing(LoanCusip)
drop LoanCusip
replace cusip6_sdc = substr(cusip9_sdc, 1, 6) if missing(cusip6_sdc) & ~missing(cusip9_sdc)
order cusip9_sdc ISIN cusip9_cgs cusip6_cgs cusip6_sdc CUSIP ImmediateParentCUSIP UltimateParentCUSIP  source

* Drop any cases in which cusip6_sdc != cusip6_cgs
drop if cusip6_sdc != cusip6_cgs & ~missing(cusip6_sdc) & ~missing(cusip6_cgs)

* Do not use any CUSIP links for which the CUSIP identified does not appear in the CGS master file
mmerge CUSIP using $cmns1/aggregation_sources/cgs_compact_complete, umatch(issuer_number) unmatched(m) uname(cgs_)
gen use_CUSIP = 1
replace use_CUSIP = 0 if _merge == 1
rename cgs_issuer_name CUSIP_issuer_name 
rename cgs_domicile CUSIP_domicile

* Do not use any cusip6_sdc links for which the CUSIP identified does not appear in the CGS master file
mmerge cusip6_sdc using $cmns1/aggregation_sources/cgs_compact_complete, umatch(issuer_number) unmatched(m) uname(cgs_)
gen use_cusip6_sdc = 1
replace use_cusip6_sdc = 0 if _merge == 1
rename cgs_issuer_name cusip6_sdc_issuer_name 
rename cgs_domicile cusip6_sdc_domicile

* Do not use any ImmediateParentCUSIP links for which the CUSIP identified does not appear in the CGS master file
mmerge ImmediateParentCUSIP using $cmns1/aggregation_sources/cgs_compact_complete, umatch(issuer_number) unmatched(m) uname(cgs_)
gen use_ImmediateParentCUSIP = 1
replace use_ImmediateParentCUSIP = 0 if _merge == 1
rename cgs_issuer_name ImmediateParentCUSIP_issuer_name 
rename cgs_domicile ImmediateParentCUSIP_domicile

* Do not use any UltimateParentCUSIP links for which the CUSIP identified does not
* appear in the CGS master file
mmerge UltimateParentCUSIP using $cmns1/aggregation_sources/cgs_compact_complete, umatch(issuer_number) unmatched(m) uname(cgs_)
gen use_UltimateParentCUSIP = 1
replace use_UltimateParentCUSIP = 0 if _merge == 1
rename cgs_issuer_name UltimateParentCUSIP_issuer_name 
rename cgs_domicile UltimateParentCUSIP_domicile

* Finally also do a name merge for cusip6_cgs
mmerge cusip6_cgs using $cmns1/aggregation_sources/cgs_compact_complete, umatch(issuer_number) unmatched(m) uname(cgs_)
gen use_cusip6_cgs = 1
replace use_cusip6_cgs = 0 if _merge == 1
rename cgs_issuer_name cusip6_cgs_issuer_name 
rename cgs_domicile cusip6_cgs_domicile

* Intermediate step save
save $temp/sdc_parent_match_tmp1, replace

* Create names dataset
use $temp/sdc_parent_match_tmp1, clear
keep CUSIP source_type Issuer
drop if missing(Issuer)
drop if missing(CUSIP)
sort source_type CUSIP
by source_type CUSIP: keep if _n == 1
save $temp/sdc_names_internal, replace
keep CUSIP Issuer
rename CUSIP cusip6
rename Issuer issuer_name
save $cmns1/aggregation_sources/sdc_names, replace

* Now do fuzzy name comparisons
use $temp/sdc_parent_match_tmp1, clear
drop NumberofEmployeesDate IssueDate FinalMaturity Maturity MainSICCode TickerSymbol PrincipalAmountmil TypeofSecurity Currency Marketplace Coupon OfferYieldtoMaturity MoodyRating StandardPoorsRating IssuerBorrowerSEDOL IssueType CountryofIncorporation GoverningLaw BondTypeCode BondType MarketArea MasterDealType DealNumber TotalAssetsBeforethe PublicStatus TotalDebttoEquityR NumberofEmployees DateFounded UltimateParentSupranationalFl IssuerBorrowerParentsSedol UltimateParentsPublicStatus UltimateParentsPrimarySICCo Nation LaunchDate PrincipalAmount DCNNumber cgs_currency start_date_str end_date_str end_date start_date issue_date maturity_date iso_country_code PrimaryExchangeWhereIssuers PrincipalAmtsumofallMkt SharesFiledinthisMkt AmtFiledinthisMktmil UltimateParentsNation data_date AnnouncementDate FinancialCloseDate FilingDate Borrower G BusinessDescription PrimarySICCode State NationHeadquarters PrimaryExchangeWhereBorrower Industry TrancheAmountmil OfferPrice Description PrimaryExchangeWhereIssueWil TotalDollarAmountFiled SharesOfferedinthisMkt SharesOfferedsumofallM ShortPricingDescr LeadAgents SyndicationAgentCode DocumentationAgentCode Status TargetMarketDescription UltimateParentsTicker IssuerBorrowerUltimateParent AN Name AT AX AZ A CUSIP_domicile
gen sdc_issuer_name = upper(Issuer)

* Thresholds (UP field appears of higher quality, so has a lower threshold)
local name_match_threshold = 0.85
local name_match_threshold_up = 0.7
local name_match_threshold_th = 0.2
local name_match_threshold_gov_munis = 0.99

* Add parent names
mmerge UltimateParentCUSIP using $temp/sdc_names_internal, umatch(CUSIP) unmatched(m) uname(up_)
mmerge ImmediateParentCUSIP using $temp/sdc_names_internal, umatch(CUSIP) unmatched(m) uname(ip_)
rename up_Issuer sdc_up_name
replace sdc_up_name = upper(sdc_up_name)
rename ip_Issuer sdc_ip_name
replace sdc_ip_name = upper(sdc_ip_name)
replace sdc_up_name = UltimateParent if missing(sdc_up_name)
replace sdc_up_name = upper(sdc_up_name)

* Do some cleaning of names
foreach name_field in "CUSIP_issuer_name" "cusip6_sdc_issuer_name" "cusip6_cgs_issuer_name" "UltimateParentCUSIP_issuer_name" "ImmediateParentCUSIP_issuer_name" {
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

* Compute name distance metrics
jarowinkler sdc_issuer_name CUSIP_issuer_name, gen(namedist_CUSIP)
jarowinkler sdc_issuer_name cusip6_sdc_issuer_name, gen(namedist_cusip6_sdc)
jarowinkler sdc_issuer_name cusip6_cgs_issuer_name, gen(namedist_cusip6_cgs)
jarowinkler sdc_up_name UltimateParentCUSIP_issuer_name, gen(namedist_UltimateParentCUSIP)
jarowinkler sdc_ip_name ImmediateParentCUSIP_issuer_name, gen(namedist_ImmediateParentCUSIP)
format %55s *_issuer_name

* Intermediate step save
save $temp/sdc_parent_match_tmp2, replace

* Prepare the FIGI file: Security level
* To assign marketsector: we are conservative and assign issuers to the market sector
* for which we want to have the strictest SDC filters
use $raw/figi/figi_master_compact, clear
keep cusip marketsector
drop if missing(cusip)
gen consolidated_sector = ""
replace consolidated_sector = "Other" if inlist(marketsector, "Index", "Comdty", "Curncy", "")
replace consolidated_sector = "Muni" if marketsector == "Muni"
replace consolidated_sector = "Govt" if marketsector == "Govt"
replace consolidated_sector = "Corp_Equity" if inlist(marketsector, "Equity", "Pfd")
replace consolidated_sector = "Corp_Bond" if marketsector == "Corp"
replace consolidated_sector = "Agency_Structured" if marketsector == "Mtge"
collapse (firstnm) consolidated_sector, by(cusip)
save $temp/figi_cusip9_sectype, replace

* Load checkpoint
use $temp/sdc_parent_match_tmp2, clear
cap drop _merge

* Merge with FIGI data to figure out what records correspond to sovereign and muni bonds 
mmerge cusip9_sdc using $temp/figi_cusip9_sectype, unmatched(m) umatch(cusip)
rename consolidated_sector cusip9_sdc_sector
mmerge cusip9_cgs using $temp/figi_cusip9_sectype, unmatched(m) umatch(cusip)
rename consolidated_sector cusip9_cgs_sector
foreach _field in "CUSIP" "ImmediateParentCUSIP" "UltimateParentCUSIP" "cusip6_sdc" "cusip6_cgs" {
	mmerge `_field' using "$temp/figi_cusip6_sectype", unmatched(m) umatch(cusip6)
	rename consolidated_sector `_field'_sector
}
replace cusip6_cgs_sector = cusip9_cgs_sector if ~missing(cusip9_cgs_sector)
replace cusip6_sdc_sector = cusip9_sdc_sector if ~missing(cusip9_sdc_sector)

* Merge in the CGS domiciles
foreach _field in "CUSIP" "cusip6_sdc" "ImmediateParentCUSIP" "UltimateParentCUSIP" "cusip6_cgs" {
	di "Processing `_field'"
	mmerge `_field' using "$cmns1/aggregation_sources/cgs_compact_complete", umatch(issuer_number) unmatched(m) uname(cgs_) ukeep(domicile)
	gen th_`_field' = 0
	forvalues j=1(1)10 {
			cap replace th_`_field' = 1 if (inlist(cgs_domicile,${tax_haven_`j'}))
	}
	drop cgs_domicile
}

* Set use flags to zero if names are missing
foreach _field in "CUSIP" "cusip6_sdc" "cusip6_cgs" "ImmediateParentCUSIP" {
	replace use_`_field' = 0 if missing(sdc_issuer_name ) | missing(`_field'_issuer_name)
}

* Select matches
foreach _field in "CUSIP" "cusip6_sdc" "cusip6_cgs" "ImmediateParentCUSIP" {
	order namedist_`_field' sdc_issuer_name `_field'_issuer_name
	gsort -namedist_`_field'
	replace `_field' = "" if namedist_`_field' < `name_match_threshold' & ~missing(sdc_issuer_name) & ~missing(`_field'_issuer_name) & th_`_field' == 0 & ~inlist(`_field'_sector, "Govt", "Muni", "Agency_Structured", "")
	replace `_field' = "" if namedist_`_field' < `name_match_threshold_gov_munis' & ~missing(sdc_issuer_name) & ~missing(`_field'_issuer_name) & th_`_field' == 0 & inlist(`_field'_sector, "Govt", "Muni", "Agency_Structured", "")
	replace `_field' = "" if namedist_`_field' < `name_match_threshold_th' & ~missing(sdc_issuer_name) & ~missing(`_field'_issuer_name) & th_`_field' == 1
}

* Select matches
foreach _field in "ImmediateParentCUSIP" {
	order namedist_`_field' sdc_issuer_name `_field'_issuer_name
	gsort -namedist_`_field'
	replace `_field' = "" if namedist_`_field' < `name_match_threshold' & ~missing(sdc_ip_name) & ~missing(`_field'_issuer_name) & th_`_field' == 0 & ~inlist(`_field'_sector, "Govt", "Muni", "Agency_Structured", "")
	replace `_field' = "" if namedist_`_field' < `name_match_threshold_gov_munis' & ~missing(sdc_ip_name) & ~missing(`_field'_issuer_name) & th_`_field' == 0 & inlist(`_field'_sector, "Govt", "Muni", "Agency_Structured", "")
	replace `_field' = "" if namedist_`_field' < `name_match_threshold_th' & ~missing(sdc_ip_name) & ~missing(`_field'_issuer_name) & th_`_field' == 1
}

* Intermediate step save
save $temp/sdc_parent_match_tmp3, replace

* Create names dataset from ultimate parent field
use $temp/sdc_parent_match_tmp3, clear	
keep sdc_up_name UltimateParentCUSIP
rename sdc_up_name issuer_name
rename UltimateParentCUSIP cusip6
drop if missing(issuer_name) | missing(cusip6)
duplicates drop
save $cmns1/aggregation_sources/sdc_names, replace

* Flatten the files
foreach _field in "CUSIP" "cusip6_sdc" "cusip6_cgs" "ImmediateParentCUSIP" {

	* Load and process field
	di "Processing `_field'"
	use $temp/sdc_parent_match_tmp3, clear	
	keep `_field' UltimateParentCUSIP use_`_field' use_UltimateParentCUSIP th_`_field' sdc_up_name UltimateParentCUSIP_issuer_name th_UltimateParentCUSIP namedist_UltimateParentCUSIP UltimateParentCUSIP_sector
	rename `_field' cusip
	rename use_`_field' use_cusip
	rename UltimateParentCUSIP up_cusip
	rename use_UltimateParentCUSIP use_up_cusip
	replace use_cusip = 1 if th_`_field' == 1
	replace use_up_cusip = 1 if th_`_field' == 1

	* Also apply name filter to UltimateParentCUSIP
	replace up_cusip = "" if namedist_UltimateParentCUSIP < `name_match_threshold_up' & ~missing(sdc_up_name) & ~missing(UltimateParentCUSIP_issuer_name) & th_UltimateParentCUSIP == 0 & th_`_field' == 0 & ~inlist(UltimateParentCUSIP_sector, "Govt", "Muni", "Agency_Structured", "")
	replace up_cusip = "" if namedist_UltimateParentCUSIP < `name_match_threshold_gov_munis' & ~missing(sdc_up_name) & ~missing(UltimateParentCUSIP_issuer_name) & th_UltimateParentCUSIP == 0 & th_`_field' == 0 & inlist(UltimateParentCUSIP_sector, "Govt", "Muni", "Agency_Structured", "")
	replace up_cusip = "" if namedist_UltimateParentCUSIP < `name_match_threshold_th' & ~missing(sdc_up_name) & ~missing(UltimateParentCUSIP_issuer_name) & (th_UltimateParentCUSIP == 1 | th_`_field' == 1)

	* Collapse and save
	collapse (max) use_cusip use_up_cusip, by(cusip up_cusip)
	save $temp/sdc_flattened_`_field', replace

}

* Append
clear 
foreach _field in "CUSIP" "cusip6_sdc" "cusip6_cgs" "ImmediateParentCUSIP" {
	append using $temp/sdc_flattened_`_field'
}
collapse (max) use_cusip use_up_cusip, by(cusip up_cusip)
drop if missing(cusip) | missing(up_cusip)

* Deal with cases where there are multiple records per cusip
bysort cusip: gen n_rows = _N
drop if cusip == up_cusip & n_rows > 1
drop n_rows
bysort cusip: gen n_rows = _N
bysort cusip: egen max_use_cusip = max(use_cusip)
bysort cusip: egen max_use_up_cusip = max(use_up_cusip)
drop if n_rows > 1 & use_cusip < max_use_cusip & use_up_cusip < max_use_up_cusip
drop n_rows
bysort cusip: gen n_rows = _N
drop if n_rows >= 2
drop n_rows max_*

* Output dataset
rename cusip cusip6
rename up_cusip up_cusip6
keep cusip6 up_cusip6 use_cusip use_up_cusip
save $cmns1/aggregation_sources/sdc_aggregation, replace

* --------------------------------------------------------------------------------------------------
* Build SDC country information file
* --------------------------------------------------------------------------------------------------

* Read in country information
use cusip6* CUSIP iso start_date using $sdc_datasets/sdc_eq_appended, clear
append using $sdc_datasets/sdc_bonds_appended.dta, keep(cusip6* CUSIP iso start_date)
keep cusip6* CUSIP iso start_date
drop if iso==""
drop if cusip6_cgs=="" & cusip6_sdc=="" & CUSIP==""
duplicates drop
tempfile cgs sdc cusip 
preserve
keep cusip6_cgs iso start
drop if cusip6_cgs==""
duplicates drop
rename cusip6 cusip6
save "`cgs'", replace
restore
preserve
keep cusip6_sdc iso start 
drop if cusip6_sdc==""
duplicates drop
rename cusip6 cusip6 
save "`sdc'", replace

* Append all the information
restore
keep CUSIP iso start
drop if CUSIP==""
duplicates drop
rename CUSIP cusip6 
save "`cusip'", replace
use "`cusip'", clear
append using "`cgs'"
append using "`sdc'"
sort cusip6 start
collapse (lastnm) start, by(cusip6 iso)
duplicates drop 

* Flag tax havens
gen tax_haven=0
replace tax_haven=1 if inlist(iso,$tax_haven_1)==1 | inlist(iso,$tax_haven_2)==1 | inlist(iso,$tax_haven_3)==1 ///
	| inlist(iso,$tax_haven_4)==1 | inlist(iso,$tax_haven_5)==1 | inlist(iso,$tax_haven_6)==1 ///
	| inlist(iso,$tax_haven_7)==1 | inlist(iso,$tax_haven_8)==1 
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
bysort cusip6: egen nontaxhaven_cusip6=min(tax_haven)
drop if count>1 & tax_haven==1 & nontaxhaven_cusip6==0
drop count

* Select a unique assignment, penalizing tax havens
bysort cusip6: egen latest_start=max(start)
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
format latest %tq
drop if count>1 & start~=lat
drop count
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
gen multiple=0
replace multiple=1 if count>1
bysort cusip6: gen n=_n
keep if n==1
drop latest count n start non
keep cusip6 iso
rename iso country_sdc
save $cmns1/aggregation_sources/sdc_country.dta, replace

log close
