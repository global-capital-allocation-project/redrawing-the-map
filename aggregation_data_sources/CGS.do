* --------------------------------------------------------------------------------------------------
* CGS_Build: This job imports the raw master security- and issuer-level data from CUSIP Global Services 
* (CGS). The accompanying guide (CMNS_Data_Guide.pdf) provides more details on the CGS data; the primary
* output is a series of files containing essential informations about global CUSIP-bearing securities
* and their issuers.
* --------------------------------------------------------------------------------------------------
cap log close
log using $cmns1/logs/CGS_Build, replace

* --------------------------------------------------------------------------------------------------
* INCMSTR file
* --------------------------------------------------------------------------------------------------

* INCMSTR, 2018 version from CGS
import delimited "$cmns1/temp/cgs_uncompress/INCMSTR.PIP", delimiter("|") encoding(ISO-8859-1) clear
rename v1 isin
rename v2 issuer_num
rename v3 issue_num
rename v4 chk_digit
rename v5 issuer_name
rename v6 issue_desc
rename v7 cfi_code
rename v8 iso_domicile
label var iso_domicile "ISO 2 Country code"
rename v9 iso_currency
rename v10 rate
rename v11 maturity_date
rename v12 last_modify_dt
rename v13 status
tostring chk_digit, replace
replace chk_digit="" if chk_digit=="."
gen cusip=issuer_num+issue_num+chk_digit
drop if cusip==""
drop last_modify_dt
duplicates drop
save "$cmns1/temp/cgs/incmstr.v18.dta", replace

* INCMSTR, we append the previous version (2016) for any missing observations
import delimited "$cmns1/temp/cgs_uncompress/delivery_2016/INCMSTR.PIP", delimiter("|") encoding(ISO-8859-1) clear
rename v1 isin
rename v2 issuer_num
rename v3 issue_num
rename v4 chk_digit
rename v5 issuer_name
rename v6 issue_desc
rename v7 cfi_code
rename v8 iso_domicile
label var iso_domicile "ISO 2 Country code"
rename v9 iso_currency
rename v10 rate
rename v11 maturity_date
rename v12 last_modify_dt
rename v13 status
tostring chk_digit, replace
replace chk_digit="" if chk_digit=="."
gen cusip=issuer_num+issue_num+chk_digit
drop if cusip==""
drop last_modify_dt
duplicates drop
save "$cmns1/temp/cgs/incmstr.v16.dta", replace

* Consolidate versions; we keep unique ISINs and in case of CUSIP conflicts we use the latest info
use "$cmns1/temp/cgs/incmstr.v18.dta", replace
gen _file = "new"
append using "$cmns1/temp/cgs/incmstr.v16.dta"
replace _file = "old" if missing(_file)
duplicates drop isin, force
bysort isin : egen numCusip = nvals(cusip)
drop if numCusip > 1 & _file == "old"
bysort cusip : keep if _n == 1
drop numCusip _file
save "$cmns1/temp/cgs/incmstr.dta", replace

* --------------------------------------------------------------------------------------------------
* 144a file
* --------------------------------------------------------------------------------------------------

* 144a, 2018 version
import delimited "$cmns1/temp/cgs_uncompress/FFAPlusMASTER.PIP", delimiter("|") encoding(ISO-8859-1) clear
rename v1 issuer_num
rename v2 issuer_name
rename v3 issuer_state_code
rename v4 issuer_desc
rename v5 maturity_date
rename v6 rate
rename v7 dated_date
rename v8 link_to_issue
rename v9 cusip_144a
rename v10 entry_date
rename v11 accredited_inv_cusip
rename v12 accredited_entry_date
rename v13 registered_cusip
rename v14 registered_entry_date
rename v15 reg_s
rename v16 reg_s_entry_date
rename v17 reg_s_isin
rename v18 reg_s_update_date
rename v19 issue_status
save "$cmns1/temp/cgs/ffaplusmaster.v18.dta", replace

* 144a, previous version
import delimited "$cmns1/temp/cgs_uncompress/delivery_2016/FFAPlusMASTER.PIP", delimiter("|") encoding(ISO-8859-1) clear
rename v1 issuer_num
rename v2 issuer_name
rename v3 issuer_state_code
rename v4 issuer_desc
rename v5 maturity_date
rename v6 rate
rename v7 dated_date
rename v8 link_to_issue
rename v9 cusip_144a
rename v10 entry_date
rename v11 accredited_inv_cusip
rename v12 accredited_entry_date
rename v13 registered_cusip
rename v14 registered_entry_date
rename v15 reg_s
rename v16 reg_s_entry_date
rename v17 reg_s_isin
rename v18 reg_s_update_date
rename v19 issue_status
save "$cmns1/temp/cgs/ffaplusmaster.v16.dta", replace

* Consolidate versions
use "$cmns1/temp/cgs/ffaplusmaster.v18.dta", replace
append using "$cmns1/temp/cgs/ffaplusmaster.v16.dta"
duplicates drop cusip_144a, force
save "$cmns1/temp/cgs/ffaplusmaster.dta", replace

* --------------------------------------------------------------------------------------------------
* Commercial paper files
* --------------------------------------------------------------------------------------------------

foreach x in "CPMASTER_ATTRIBUTE" "CPMASTER_ISSUE" "CPMASTER_ISSUER" {
	import delimited "$cmns1/temp/cgs_uncompress/`x'.PIP", delimiter("|") encoding(ISO-8859-1) clear
	save "$cmns1/temp/cgs/`x'.dta", replace
}

* --------------------------------------------------------------------------------------------------
* ALLMASTER_ISIN file
* --------------------------------------------------------------------------------------------------

* Common build steps for ALLMASTER_ISIN
cap program drop build_allmaster_isin
program build_allmaster_isin
	rename v1 ISSUER_NUM
	rename v2 ISSUE_NUM
	rename v3 ISSUE_CHECK
	rename v4 ISSUE_DESCRIPTION
	rename v5 ISSUE_ADDITIONAL_INFO
	rename v6 ISSUE_STATUS
	rename v7 ISSUE_TYPE_CODE
	rename v8 DATED_DATE
	rename v9 MATURITY_DATE
	rename v10 PARTIAL_MATURITY
	rename v11 COUPON_RATE
	cap tostring COUPON_RATE, replace force
	rename v12 CURRENCY_CODE
	rename v13 SECURITY_TYPE_DESCRIPTION
	rename v14 FISN
	rename v15 ISSUE_GROUP
	rename v16 ISIN
	rename v17 WHERE_TRADED
	rename v18 TICKER_SYMBOL
	rename v19 US_CFI_CODE
	rename v20 ISO_CFI_CODE
	rename v21 ISSUE_ENTRY_DATE
	rename v22 ALTERNATIVE_MINIMUM_TAX
	rename v23 BANK_QUALIFIED
	rename v24 CALLABLE
	rename v25 FIRST_COUPON_DATE
	rename v26 INITIAL_PUBLIC_OFFERING
	rename v27 PAYMENT_FREQUENCY_CODE
	rename v28 CLOSING_DATE
	rename v29 DEPOSITORY_ELIGIBLE
	rename v30 PRE_REFUNDED
	rename v31 REFUNDABLE
	rename v32 REMARKETED
	rename v33 SINKING_FUND
	rename v34 TAXABLE
	rename v35 BOND_FORM
	rename v36 ENHANCEMENTS
	rename v37 FUND_DISTRIBUTION_POLICY
	rename v38 FUND_INVESTMENT_POLICY
	rename v39 FUND_TYPE
	rename v40 GUARANTEE
	rename v41 INCOME_TYPE
	rename v42 INSURED_BY
	rename v43 OWNERSHIP_RESTRICTIONS
	rename v44 PAYMENT_STATUS
	rename v45 PREFERRED_TYPE
	rename v46 PUTABLE
	rename v47 RATE_TYPE
	rename v48 REDEMPTION
	rename v49 SOURCE_DOCUMENT
	rename v50 SPONSORING
	rename v51 VOTING_RIGHTS
	rename v52 WARRANT_ASSETS
	rename v53 WARRANT_STATUS
	rename v54 WARRANT_TYPE
	rename v55 UNDERWRITER
	rename v56 AUDITOR
	rename v57 PAYING_AGENT
	rename v58 TENDER_AGENT
	rename v59 TRANSFER_AGENT
	rename v60 BOND_COUNSEL
	rename v61 FINANCIAL_ADVISOR
	rename v62 MUNICIPAL_SALE_DATE
	rename v63 SALE_TYPE
	rename v64 OFFERING_AMOUNT
	rename v65 OFFERING_AMOUNT_CODE
	rename v66 ISSUE_TRANSACTION
	rename v67 ISSUE_LAST_UPDATE_DATE
	rename v68 RESERVED_1
	rename v69 RESERVED_2
	rename v70 RESERVED_3
	rename v71 RESERVED_4
	rename v72 RESERVED_5
	rename v73 RESERVED_6
	rename v74 RESERVED_7
	rename v75 RESERVED_8
	rename v76 RESERVED_9
	rename v77 RESERVED_10
	foreach x of varlist _all {
		local temp=lower("`x'")
		rename `x' `temp'
	}	
end

* ALLMASTER_ISIN, 2018 version
import delimited "$cmns1/temp/cgs_uncompress/ALLMASTER_ISIN.PIP", delimiter("|") encoding(ISO-8859-1) clear
build_allmaster_isin
save  $cmns1/temp/cgs/ALLMASTER_ISIN.v18.dta, replace

* ALLMASTER_ISIN, previous version
import delimited "$cmns1/temp/cgs_uncompress/delivery_2016/ALLMASTER_ISIN.PIP", delimiter("|") encoding(ISO-8859-1) clear
build_allmaster_isin
save  $cmns1/temp/cgs/ALLMASTER_ISIN.v16.dta, replace

* Consolidate versions
use "$cmns1/temp/cgs/ALLMASTER_ISIN.v18.dta", replace
cap drop reserved_*
destring issue_check, force replace
destring payment_frequency_code, force replace
destring tender_agent, force replace
destring issue_transaction, force replace
append using "$cmns1/temp/cgs/ALLMASTER_ISIN.v16.dta"
cap drop reserved_*
tostring issue_check, force replace
gen cusip = issuer_num + issue_num + issue_check 
duplicates drop cusip, force
drop cusip
save "$cmns1/temp/cgs/ALLMASTER_ISIN.dta", replace

* --------------------------------------------------------------------------------------------------
* ALLMASTER_ISSUER file
* --------------------------------------------------------------------------------------------------

* Common build steps for ALLMASTER_ISSUER
cap program drop build_allmaster_issuer
program build_allmaster_issuer
	rename v1 ISSUER_NUMBER
	rename v2 ISSUER_CHECK
	rename v3 ISSUER_NAME
	rename v4 ISSUER_ADL
	rename v5 ISSUER_TYPE
	rename v6 ISSUER_STATUS
	rename v7 DOMICILE
	rename v8 STATE_CD
	rename v9 CABRE_ID
	rename v10 CABRE_STATUS
	rename v11 LEI_GMEI
	rename v12 LEGAL_ENTITY_NAME
	rename v13 PREVIOUS_NAME
	rename v14 ISSUER_ENTRY_DATE
	rename v15 CP_INSTITUTION_TYPE_DESC
	rename v16 ISSUER_TRANSACTION
	rename v17 ISSUER_UPDATE_DATE
	rename v18 RESERVED_1
	rename v19 RESERVED_2
	rename v20 RESERVED_3
	rename v21 RESERVED_4
	rename v22 RESERVED_5
	rename v23 RESERVED_6
	rename v24 RESERVED_7
	rename v25 RESERVED_8
	rename v26 RESERVED_9
	rename v27 RESERVED_10
	foreach x of varlist _all {
		local temp=lower("`x'")
		rename `x' `temp'
	}	
	mmerge domicile using "$raw/Macro/Concordances/iso2_iso3.dta", umatch(iso2)
	drop if _merge==2
	replace dom=iso3 if _merge==3
	drop _merge iso3
end

* 2018 version
import delimited "$cmns1/temp/cgs_uncompress/ALLMASTER_ISSUER.PIP", delimiter("|") encoding(ISO-8859-1) clear
build_allmaster_issuer
save  $cmns1/temp/cgs/ALLMASTER_ISSUER.v18.dta, replace

* Previous version
import delimited "$cmns1/temp/cgs_uncompress/delivery_2016/ALLMASTER_ISSUER.PIP", delimiter("|") encoding(ISO-8859-1) clear
build_allmaster_issuer
save  $cmns1/temp/cgs/ALLMASTER_ISSUER.v16.dta, replace

* Consolidate versions
use "$cmns1/temp/cgs/ALLMASTER_ISSUER.v18.dta", replace
append using "$cmns1/temp/cgs/ALLMASTER_ISSUER.v16.dta"
duplicates drop issuer_number, force
save "$cmns1/temp/cgs/ALLMASTER_ISSUER.dta", replace

* --------------------------------------------------------------------------------------------------
* Associated Issuers (AI) file
* --------------------------------------------------------------------------------------------------

* Common build steps for AI MASTER
cap program drop build_aimaster
program build_aimaster
cap drop v17
	rename v1 issuer_link
	rename v2 issuer_num
	rename v3 issuer_desc
	rename v4 action_type_1
	rename v5 new_name_1
	rename v6 effective_date_1
	rename v7 pending_flag
	rename v8 action_type_2
	rename v9 new_name_2
	rename v10 effective_date_2
	rename v11 action_type_3
	rename v12 new_name_3
	rename v13 effective_date_3
	rename v14 issuer_status
	rename v15 issuer_type
	rename v16 update_flag
end

* 2018 version
import delimited "$cmns1/temp/cgs_uncompress/AIMASTER.PIP", delimiter("|") encoding(ISO-8859-1) clear
build_aimaster
save  $cmns1/temp/cgs/AIMASTER.v18.dta, replace

* Previous version
import delimited "$cmns1/temp/cgs_uncompress/delivery_2016/AIMASTER.PIP", delimiter("|") encoding(ISO-8859-1) clear
build_aimaster
save  $cmns1/temp/cgs/AIMASTER.v16.dta, replace

* Consolidate versions; if there are any conflicts we use the latest version of the file
use "$cmns1/temp/cgs/AIMASTER.v18.dta", replace
gen file_version = 18
append using "$cmns1/temp/cgs/AIMASTER.v16.dta"
replace file_version = 16 if missing(file_version)
duplicates drop issuer_num issuer_link, force
bysort issuer_num: egen max_file_version = max(file_version)
keep if file_version == max_file_version
save "$cmns1/temp/cgs/AIMASTER.dta", replace

* --------------------------------------------------------------------------------------------------
* LEI Plus file
* --------------------------------------------------------------------------------------------------

import delimited "$raw/cgs_lei_plus/CBRLEIMSTR.PIP", delimiter("|") encoding(ISO-8859-1) clear
rename v3 lei_gmei
rename v4 issuer_number
keep lei_gmei issuer_number
keep if ~missing(lei_gmei) & ~missing(issuer_number)
duplicates drop
save "$cmns1/temp/cgs/lei_plus_formerge.dta", replace

* --------------------------------------------------------------------------------------------------
* Essentials from the CUSIP/ISIN master file
* --------------------------------------------------------------------------------------------------

use iso_cfi_code isin issuer_num issue_num issue_check currency_code maturity_date coupon_rate using "$cmns1/temp/cgs/ALLMASTER_ISIN.dta", clear
capture tostring issue_check, replace force
gen cusip=issuer_num+issue_num+ issue_check
drop issue_num issue_check
drop if issuer_num=="999999"
gen length=strlen(cusip)
drop if length~=9
drop length
save "$cmns1/temp/cgs/allmaster_essentials_step1.dta", replace

use "$cmns1/temp/cgs/ALLMASTER_ISSUER.dta", clear
gen length=strlen(issuer_num)
drop if length~=6
drop length
save "$cmns1/temp/cgs/ALLMASTER_ISSUER_merge.dta", replace

use "$cmns1/temp/cgs/allmaster_essentials_step1.dta", clear
mmerge issuer_num using "$cmns1/temp/cgs/ALLMASTER_ISSUER_merge.dta", ukeep(domicile issuer_type) umatch(issuer_number)
keep if _merge==3
drop _merge
gen maturity2=date(maturity,"YMD")
format maturity2 %td
drop maturity_date
rename maturity2 maturity_date
save "$cmns1/temp/cgs/allmaster_essentials.dta", replace

* --------------------------------------------------------------------------------------------------
* Mortgages, TBA securities, SVF
* --------------------------------------------------------------------------------------------------

* GNMA, part 1
import delimited "$raw/cgs_master/master_20131211.GM", delimiter(space) encoding(ISO-8859-1)clear
drop if v3~=""
keep v1
rename v1 cusip10
gen cusip=substr(cusip10,1,9)
drop cusip10
gen agency="GNMA"
save "$cmns1/temp/cgs/GNMA.p1.dta", replace

* GNMA, part 2
import delimited "$raw/cgs_master/issue_20170912.GM", delimiter(comma) encoding(ISO-8859-1) clear
tostring v3, force replace
gen cusip = v1+v2+v3
keep cusip
gen agency="GNMA"
save "$cmns1/temp/cgs/GNMA.p2.dta", replace
append using "$cmns1/temp/cgs/GNMA.p1.dta"
duplicates drop
save "$cmns1/temp/cgs/GNMA.dta", replace

* SBA, part 1
import delimited "$raw/cgs_master/master_20100512.SB", delimiter(space) encoding(ISO-8859-1)clear
drop if v3~=""
keep v1
rename v1 cusip10
gen cusip=substr(cusip10,1,9)
drop cusip10
gen agency="SBA"
save "$cmns1/temp/cgs/SBA.p1.dta", replace

* SBA, part 2
import delimited "$raw/cgs_master/issue_20081208.SB", delimiter(comma) encoding(ISO-8859-1)clear
tostring v3, force replace
gen cusip = v1+v2+v3
keep cusip
gen agency="SBA"
save "$cmns1/temp/cgs/SBA.p2.dta", replace
append using "$cmns1/temp/cgs/SBA.p1.dta"
duplicates drop
save "$cmns1/temp/cgs/SBA.dta", replace

* FNMA, part 1
import delimited "$raw/cgs_master/master_20160809.FM", delimiter(space) encoding(ISO-8859-1)clear
drop if v3~=""
keep v1
rename v1 cusip10
gen cusip=substr(cusip10,1,9)
drop cusip10
gen agency="FNMA"
save "$cmns1/temp/cgs/FNMA.p1.dta", replace

* FNMA, part 2
import delimited "$raw/cgs_master/issue_20160809.FM", delimiter(comma) encoding(ISO-8859-1)clear
tostring v3, force replace
gen cusip = v1+v2+v3
keep cusip
gen agency="FNMA"
save "$cmns1/temp/cgs/FNMA.p2.dta", replace
append using "$cmns1/temp/cgs/FNMA.p1.dta"
duplicates drop
save "$cmns1/temp/cgs/FNMA.dta", replace

* FHLMC, part 1
import delimited "$raw/cgs_master/master_20160815.FD", delimiter(space) encoding(ISO-8859-1)clear
drop if v3~=""
keep v1
rename v1 cusip10
gen cusip=substr(cusip10,1,9)
drop cusip10
gen agency="FHLMC"
save "$cmns1/temp/cgs/FHLMC.p1.dta", replace

* FHLMC, part 2
import delimited "$raw/cgs_master/issue_upd_20180718.FD", delimiter(comma) encoding(ISO-8859-1)clear
tostring v3, force replace
gen cusip = v1+v2+v3
keep cusip
gen agency="FHLMC"
save "$cmns1/temp/cgs/FHLMC.p2.dta", replace
append using "$cmns1/temp/cgs/FHLMC.p1.dta"
duplicates drop
save "$cmns1/temp/cgs/FHLMC.dta", replace

* WB, part 1
import delimited "$raw/cgs_master/master_20061206.IB", delimiter(space) encoding(ISO-8859-1)clear
drop if v3~=""
keep v1
rename v1 cusip10
gen cusip=substr(cusip10,1,9)
drop cusip10
gen agency="WorldBank"
save "$cmns1/temp/cgs/WorldBank.p1.dta", replace

* WB, part 2
import delimited "$raw/cgs_master/issue_20061206.IB", delimiter(comma) encoding(ISO-8859-1)clear
tostring v3, force replace
gen cusip = v1+v2+v3
keep cusip
gen agency="WorldBank"
save "$cmns1/temp/cgs/WorldBank.p2.dta", replace
append using "$cmns1/temp/cgs/WorldBank.p1.dta"
duplicates drop
save "$cmns1/temp/cgs/WorldBank.dta", replace

* TBA
import delimited "$cmns1/temp/cgs_uncompress/TBA Master File - Sept 2012 Rev.txt", encoding(ISO-8859-1) clear
replace v2=trim(v2)
split(v2), p(" ")
keep v1 v21
rename v1 cusip
rename v21 agency
replace agency=agency+"_TBA"
save "$cmns1/temp/cgs/TBA.dta", replace

* Append the agency files
clear
foreach x in "GNMA" "FNMA" "FHLMC" "TBA" "SBA" "WorldBank" {
	append using "$cmns1/temp/cgs/`x'.dta"
}
save "$cmns1/temp/cgs/agency.dta", replace

* --------------------------------------------------------------------------------------------------
* Append all the above
* --------------------------------------------------------------------------------------------------

* Format for appending
use "$cmns1/temp/cgs/agency.dta", clear
gen domicile="USA"
gen currency_code="USD"
replace domicile="XSN" if agency=="WorldBank"
replace currency_code="" if agency=="WorldBank"
replace agency="" if agency=="WorldBank"
cap drop cusip10
save  "$cmns1/temp/cgs/agency_format.dta", replace

use "$cmns1/temp/cgs/allmaster_essentials.dta", clear
mmerge cusip using "$cmns1/temp/cgs/agency_format.dta"
drop _merge
save "$cmns1/temp/cgs/allmaster_essentials_m.dta", replace

use "$cmns1/temp/cgs/allmaster_essentials_m.dta", clear
drop if isin==""
gen counter = 1 if !missing(isin)
bysort isin: egen count=sum(counter)
drop counter
drop if count~=1
save "$cmns1/temp/cgs/allmaster_essentials_isin.dta", replace

* Full appended file with CUSIP
use cusip using "$cmns1/temp/cgs/allmaster_essentials_m.dta", clear
save "$cmns1/temp/cgs/all_cusip_p1.dta", replace
use cusip_144 using "$cmns1/temp/cgs/ffaplusmaster.dta", clear
rename cusip_144 cusip
save "$cmns1/temp/cgs/all_cusip_p2.dta", replace
use cusip using  "$cmns1/temp/cgs/incmstr.dta", clear
save "$cmns1/temp/cgs/all_cusip_p3.dta", replace
use "$cmns1/temp/cgs/all_cusip_p1.dta", clear
append using "$cmns1/temp/cgs/all_cusip_p2.dta"
append using "$cmns1/temp/cgs/all_cusip_p3.dta"
duplicates drop
gen issuer_number = substr(cusip, 1, 6)
drop if missing(issuer_number)
save "$cmns1/temp/cgs/all_cusips_universe", replace

* Create version of CGS issuer master file with only issuer_num domicile issuer_name
use issuer_number dom issuer_name using "$cmns1/temp/cgs/ALLMASTER_ISSUER.dta", clear
drop if issuer_num==""
replace dom="ANT" if dom=="AN"
replace dom="SRB" if dom=="CS"
replace dom="FXX" if dom=="FX"
replace dom="XSN" if dom=="S2"
replace dom="XSN" if dom=="XS"
replace dom="YUG" if dom=="YU"
replace dom="ZAR" if dom=="ZR"
duplicates drop issuer_num dom, force
save "$cmns1/temp/cgs/ALLMASTER_ISSUER_compact.dta", replace

* Append agencies, TBAs, and World Bank
clear
foreach x in GNMA SBA GNMA FHLMC TBA WorldBank {
	append using "$cmns1/temp/cgs/`x'.dta"
}
gen cusip6=substr(cusip,1,6)
keep cusip6 agency
rename cusip6 issuer_number
rename agency issuer_name
duplicates drop
gen domicile="USA"
replace dom="XSN" if issuer_name=="WorldBank"
save "$cmns1/temp/cgs/CGS_additional.dta", replace

* Prep file for 144A
use issuer_num issuer_name using "$cmns1/temp/cgs/ffaplusmaster.dta", clear
gen domicile="USA"
rename issuer_num issuer_number
duplicates drop
save "$cmns1/temp/cgs/ffaplusmaster_compact.dta", replace

* Prep file for commercial paper
use "$cmns1/temp/cgs/CPMASTER_ISSUER.dta", clear
rename v1 issuer_number
rename v2 issuer_check
rename v3 issuer_name
rename v11 issuer_type
rename v12 transaction_code
rename v15 state
rename v16 updated_date
drop v13 v14 v17
save "$cmns1/temp/cgs/CPMASTER_ISSUER_labeled.dta", replace
keep issuer_num issuer_name 
gen domicile="USA"
duplicates drop
save "$cmns1/temp/cgs/CPMASTER_ISSUER_compact.dta", replace

* Prep file for INCMSTR
use issuer_num issuer_name iso_domicile using "$cmns1/temp/cgs/incmstr.dta", clear
rename issuer_num issuer_number
rename iso_dom domicile
duplicates drop
mmerge issuer_num using "$cmns1/temp/cgs/ALLMASTER_ISSUER_compact.dta"
keep if _merge==1
drop _merge
mmerge dom using "$raw/Macro/Concordances/iso2_iso3.dta", umatch(iso2) ukeep(iso3)
replace iso3="ANT" if dom=="AN"
drop if _merge==2
drop _merge dom
rename iso3 domicile
duplicates drop issuer_num dom, force
gen counter=1 if !missing(issuer_num)
bysort issuer_num: egen count=sum(counter)
gen rand=runiform()
bysort issuer_num: egen rand_max=max(rand)
drop if rand<rand_max & count>1
drop rand rand_max counter count
save "$cmns1/temp/cgs/incmstr_additional_issuers.dta", replace

* Append incrementally all above files (in order of priority of info quality)
use "$cmns1/temp/cgs/ALLMASTER_ISSUER_compact.dta", clear

mmerge issuer_num using	"$cmns1/temp/cgs/CGS_additional.dta", uname(add_)
replace issuer_name=add_issuer_name if _merge==2
replace domicile=add_domicile if _merge==2
drop add_* _merge

mmerge issuer_num using "$cmns1/temp/cgs/incmstr_additional_issuers.dta", uname(add_)
replace issuer_name=add_issuer_name if _merge==2
replace domicile=add_domicile if _merge==2
drop add_* _merge

mmerge issuer_num using	"$cmns1/temp/cgs/ffaplusmaster_compact.dta", uname(add_)
replace issuer_name=add_issuer_name if _merge==2
replace domicile=add_domicile if _merge==2
drop add_* _merge

mmerge issuer_num using	"$cmns1/temp/cgs/CPMASTER_ISSUER_compact.dta", uname(add_)
replace issuer_name=add_issuer_name if _merge==2
replace domicile=add_domicile if _merge==2
drop add_* _merge
unique(issuer_num)
save "$cmns1/aggregation_sources/cgs_compact_complete.dta", replace

* Also append with full security universe
use "$cmns1/aggregation_sources/cgs_compact_complete.dta", clear
drop if issuer_number == ""
duplicates drop issuer_number, force
mmerge issuer_number using "$cmns1/temp/cgs/all_cusips_universe", unmatched(b)
replace cusip = issuer_number + "XXX" if missing(cusip)
duplicates drop cusip, force
save "$cmns1/temp/cgs/all_cusips_universe_all_issuers", replace

* Compact version of allmaster_isin
use issuer_num issue_num issue_check issue_description dated_date maturity_date currency_code security_type_description fisn isin issue_entry_date using "$cmns1/temp/cgs/ALLMASTER_ISIN", clear
gen cusip = issuer_num + issue_num + issue_check
drop issuer_num issue_num issue_check
save "$cmns1/temp/cgs/ALLMASTER_ISIN_compact", replace

* --------------------------------------------------------------------------------------------------
* ISIN to CUSIP mapping file
* --------------------------------------------------------------------------------------------------

use issuer_num issue_num issue_check isin curr using "$cmns1/temp/cgs/ALLMASTER_ISIN.dta", clear
cap tostring issue_check, replace
gen cusip9=issuer_num+issue_num+issue_check
drop issuer_num issue_num issue_check
drop if isin==""
rename curr cgs_currency
keep isin cusip9 cgs_currency
drop if missing(isin) | missing(cusip9)
bysort isin: keep if _n == 1
save "$cmns1/temp/cgs/isin_to_cusip", replace

* --------------------------------------------------------------------------------------------------
* Compact version of the Associated Issuers file
* --------------------------------------------------------------------------------------------------

* Create the AI file for use in UP aggregation
use issuer_link issuer_num action_type_1 using "$cmns1/temp/cgs/AIMASTER.dta", clear
rename issuer_num issuer_number
drop if issuer_link=="" | issuer_num==""
drop if issuer_link==issuer_num
drop if regexm(action_type_1,"Copyright 2016")==1
bysort issuer_num: gen n=_n
drop if n>1
drop n action
rename issuer_link ai_parent_issuer_num
mmerge ai_parent_issuer_num using "$cmns1/aggregation_sources/cgs_compact_complete.dta", umatch(issuer_num) uname("ai_parent_")
drop if _merge==2
drop _merge
keep ai_parent_issuer_num issuer_number ai_parent_issuer_name ai_parent_domicile
rename ai_parent_issuer_num ai_cusip6
rename issuer_number cusip6
rename ai_parent_issuer_name ai_name
rename ai_parent_domicile ai_residency
save "$cmns1/aggregation_sources/cgs_ai_aggregation.dta", replace

log close
