* ---------------------------------------------------------------------------------------------------
* Build_Dealogic_Master: This job produces a bond issuance masterfile from the Dealogic data, which
* is an input to the Build_Bond_Issuance job
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Build_Dealogic_Master, replace

* ---------------------------------------------------------------------------------------------------
* SIC codes
* ---------------------------------------------------------------------------------------------------

use $raw/dealogic/stata/CompanySICCodes.dta, clear
drop sortnumber
gsort companyid -isprimary
by companyid: gen sortnum = _n
keep if sortnum == 1
drop isprimary sortnum
save $cmns1/temp/dealogic/temp_sic_codes.dta, replace

* ---------------------------------------------------------------------------------------------------
* NAICS codes
* ---------------------------------------------------------------------------------------------------

use $raw/dealogic/stata/CompanyNAICSCodes.dta, clear
drop sortnumber
gsort companyid -isprimary
by companyid: gen sortnum = _n
keep if sortnum == 1
drop isprimary sortnum
save $cmns1/temp/dealogic/temp_naics_codes.dta, replace

* ---------------------------------------------------------------------------------------------------
* Company master file
* ---------------------------------------------------------------------------------------------------

use $raw/dealogic/stata/Company.dta, clear
mmerge id using $cmns1/temp/dealogic/temp_naics_codes.dta, umatch(companyid)
drop if _merge == 2
rename code NAICS
mmerge id using $cmns1/temp/dealogic/temp_sic_codes.dta, umatch(companyid)
drop if _merge == 2
rename code SIC
drop _merge
keep id companyparentid immediateparentid name nationalityofbusinessisocode nationalityofincorporationisocod organisationtypeid publicstatusid sigid NAICS SIC

gen code2 = string(NAICS)
gen _code2 = substr(code2,1,2)
drop code2
gen code2 = real(_code2)
drop _code2
rename code2 NAICS2

gen code2 = string(SIC)
gen _code2 = substr(code2,1,1)
drop code2
gen code2 = real(_code2)
drop _code2
rename code2 SIC2

gen is_gov_org_type = .
replace is_gov_org_type = 1 if inlist(organisationtypeid,1,2,3)
replace is_gov_org_type = 0 if ~inlist(organisationtypeid,1,2,3) & organisationtypeid!=.

gen is_gov_NAICS = . 
replace is_gov_NAICS = 1 if NAICS2 == 92
replace is_gov_NAICS = 0 if NAICS2 != 92 & NAICS2 != .

gen is_gov_SIC = . 
replace is_gov_SIC = 1 if SIC2 == 9
replace is_gov_SIC = 0 if SIC2 != 9 & SIC2 != .
save $cmns1/temp/company_complete.dta, replace

* ---------------------------------------------------------------------------------------------------
* DCM deal tranches: proceeds and value
* ---------------------------------------------------------------------------------------------------

cap restore
use $raw/dealogic/stata/DCMDealTranchesProceeds.dta, clear
rename value tranche_proceeds_value
qui mmerge dcmdealtranchedealid	dcmdealtranchetrancheid	currencyisocode	using $raw/dealogic/stata/DCMDealTranchesValue.dta, uname(tranche_value_)
drop _merge
bys dcmdealtranchedealid dcmdealtranchetrancheid: gen N = _N

mmerge dcmdealtranchedealid	dcmdealtranchetrancheid using $raw/dealogic/stata/DCMDealTranches.dta, umatch(dcmdealdealid	trancheid)
keep if _merge == 3

replace tranche_value_value = tranche_proceeds_value if tranche_value_value == . & tranche_proceeds_value !=.
gen issuance_in_local_currency = tranche_value_value if currency_issued == currencyisocode
bysort dcmdealtranchedealid	dcmdealtranchetrancheid (issuance_in_local_currency): replace issuance_in_local_currency = issuance_in_local_currency[1]
gen conversion_rate_at_issuance = issuance_in_local_currency/tranche_value_value

drop _merge
keep if currencyisocode == "USD"

* check for duplicates when use latest data
duplicates tag dcmdealtranchedealid	dcmdealtranchetrancheid, gen(_dup)
drop _dup

* ---------------------------------------------------------------------------------------------------
* DCM deal tranches: ISINs
* ---------------------------------------------------------------------------------------------------

mmerge dcmdealtranchedealid	dcmdealtranchetrancheid using $raw/dealogic/stata/DCMDealTranchesISINs.dta
keep if _merge != 2
bys dcmdealtranchedealid dcmdealtranchetrancheid: gen _dup = _N
drop _merge sortnumber N

* ---------------------------------------------------------------------------------------------------
* DCM deal tranches: issue characteristics
* ---------------------------------------------------------------------------------------------------

mmerge dcmdealtranchedealid	dcmdealtranchetrancheid using $raw/dealogic/stata/DCMDealTranchesIssueCharacteristics.dta
keep if _merge != 2
drop _merge _dup sortnumber
save $cmns1/temp/dealogic/aux_file.dta, replace

* ---------------------------------------------------------------------------------------------------
* DCM deal: master file
* ---------------------------------------------------------------------------------------------------

use $raw/dealogic/stata/DCMDeal.dta, clear

* remove variable if all obs are missing
foreach var of varlist _all {
     capture assert mi(`var')
     if !_rc {
        drop `var'
     }
}
mmerge issuerid using $cmns1/temp/company_complete.dta, umatch(id)
keep if _merge != 2
drop _merge
save $cmns1/temp/dcmdealnationality.dta, replace

* ---------------------------------------------------------------------------------------------------
* DCM deal tranches: master file
* ---------------------------------------------------------------------------------------------------

use $raw/dealogic/stata/DCMDealTranches.dta, clear
drop isin currencyisocode
mmerge dcmdealdealid trancheid using $cmns1/temp/dealogic/aux_file.dta, umatch(dcmdealtranchedealid dcmdealtranchetrancheid) 
mmerge dcmdealdealid using $cmns1/temp/dcmdealnationality.dta, umatch(dealid)
keep if _merge == 3

replace pricingdate = substr(pricingdate,1,10)
replace announcementdate = substr(announcementdate,1,10)
replace settlementdate = substr(settlementdate,1,10)
replace maturitydate = substr(maturitydate,1,10)

gen _pricingdate = date(pricingdate,"YMD")
gen _announcementdate = date(announcementdate,"YMD")
gen _settlementdate = date(settlementdate,"YMD")
gen _maturitydate = date(maturitydate,"YMD")

gen mdate = mofd(_pricingdate)
format mdate %tm 
drop pricingdate
rename mdate pricingdate

gen mdate = mofd(_announcementdate)
format mdate %tm 
drop announcementdate 
rename mdate announcementdate

gen mdate = mofd(_settlementdate)
format mdate %tm 
drop settlementdate
rename mdate settlementdate

gen mdate = mofd(_maturitydate)
format mdate %tm 
drop maturitydate
rename mdate maturitydate
format %td _announcementdate _pricingdate _settlementdate _maturitydate

bys dcmdealdealid trancheid: gen n = _n
gen value = tranche_value_value/1000000 if n == 1
replace value = 0 if n > 1
duplicates drop
save $cmns1/temp/tranches_complete.dta, replace

* ---------------------------------------------------------------------------------------------------
* Preparing Dealogic DCS file
* ---------------------------------------------------------------------------------------------------

cap restore
use $cmns1/temp/tranches_complete.dta, clear
bys dcmdealdealid trancheid: gen nvalues = _n

preserve
keep dcmdealdealid trancheid isin cusip
duplicates drop
mmerge isin using $cmns1/security_master/gcap_security_master_isin.dta, umatch(isin) uname(master_)
drop if _merge == 2

duplicates drop
replace cusip = master_cusip if cusip == "" & master_cusip != ""
drop _merge master*
duplicates drop

gsort dcmdealdealid trancheid -cusip
by dcmdealdealid trancheid: gen nvalues = _n
by dcmdealdealid trancheid: gen Nvalues = _N
by dcmdealdealid trancheid: replace cusip = cusip[1] if cusip == ""
drop if nvalues > 1 & cusip == ""
drop nvalues Nvalues

tempfile cusip
save `cusip'
restore

preserve
keep dcmdealdealid trancheid isin cusip
duplicates drop
qui mmerge cusip using $cmns1/security_master/gcap_security_master_cusip.dta, umatch(cusip) uname(master_)
drop if _merge == 2

duplicates drop
replace isin = master_isin if isin == "" & master_isin != ""
drop _merge master*
duplicates drop
gsort dcmdealdealid trancheid -isin
by dcmdealdealid trancheid: gen nvalues = _n
by dcmdealdealid trancheid: gen Nvalues = _N
drop nvalues Nvalues
tempfile isin
save `isin'
restore

mmerge dcmdealdealid trancheid using `cusip', umatch(dcmdealdealid trancheid) uname(master_) ukeep(cusip)
replace cusip = master_cusip if cusip == "" & master_cusip != ""
drop master*

mmerge dcmdealdealid trancheid using `isin', umatch(dcmdealdealid trancheid) uname(master_) ukeep(isin)
replace isin = master_isin if isin == "" & master_isin != ""
drop n value nvalues _merge	master_isin
duplicates drop
order cusip isin, after(trancheid)
gen cusip6 = substr(cusip,1,6)
duplicates drop dcmdealdealid trancheid cusip isin, force
order cusip6, after(cusip)

gsort dcmdealdealid trancheid -cusip6
by dcmdealdealid trancheid: gen nvalues = _n
by dcmdealdealid trancheid: gen Nvalues = _N

mmerge cusip6 using $cmns1/country_master/cmns_aggregation, umatch(issuer_number) ukeep(cgs_domicile country_bg)
drop if _merge ==2
gen _merge_cusip = _merge

gen residency = cgs_domicile
replace residency = nationalityofincorporationisocod if residency == ""
replace residency = nationalityisocode if residency == "" & nationalityofincorporationisocod == ""

gen nationality = country_bg
replace nationality = nationalityofbusinessisocode if nationality == ""
replace nationality = nationalityisocode if nationality == "" & nationalityofbusinessisocode == ""

drop nationalityofincorporationisocod nationalityofbusinessisocode cgs_domicile country_bg nationalityisocode

rename tranche_value_value value
replace value = value /1000000

order residency nationality, after(isin)
order value, after(nationality)
drop nvalues Nvalues _merge 

gsort dcmdealdealid trancheid
save $cmns1/issuance_master/dealogic_dcm_issuance_complete.dta, replace

log close
