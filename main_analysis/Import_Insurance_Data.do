* ---------------------------------------------------------------------------------------------------
* Import_Insurance_Data: Imports raw holdings data for U.S. insurers
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Import_Insurance_Data, replace

* ---------------------------------------------------------------------------------------------------
* Import market shares and insurer lists
* ---------------------------------------------------------------------------------------------------

* List of companies: Life
import excel using $raw/sp_insurance/Firms_List.xlsm, clear sheet("Life")
cap drop A C
foreach var of varlist * {
    qui replace `var' = "assets_" + lower(`var') if _n == 1
}
qui replace B = "firm" if _n == 1
foreach var of varlist * {
     local try = strtoname(`var'[1]) 
     capture rename `var' `try' 
}
cap drop if _n == 1
foreach var of varlist assets_* {
    qui destring `var', force replace
}
save $cmns1/insurance/temp/market_shares/life_assets, replace


* List of companies: Health
import excel using $raw/sp_insurance/Firms_List.xlsm, clear sheet("Health")
cap drop A C
foreach var of varlist * {
    qui replace `var' = "assets_" + lower(`var') if _n == 1
}
qui replace B = "firm" if _n == 1
foreach var of varlist * {
     local try = strtoname(`var'[1]) 
     capture rename `var' `try' 
}
cap drop if _n == 1
foreach var of varlist assets_* {
    qui destring `var', force replace
}
save $cmns1/insurance/temp/market_shares/health_assets, replace


* List of companies: P&C
import excel using $raw/sp_insurance/Firms_List.xlsm, clear sheet("PC")
cap drop A C
foreach var of varlist * {
    qui replace `var' = "assets_" + lower(`var') if _n == 1
}
qui replace B = "firm" if _n == 1
foreach var of varlist * {
     local try = strtoname(`var'[1]) 
     capture rename `var' `try' 
}
cap drop if _n == 1
foreach var of varlist assets_* {
    qui destring `var', force replace
}
save $cmns1/insurance/temp/market_shares/pc_assets, replace

* ---------------------------------------------------------------------------------------------------
* Import life insurer holdings
* ---------------------------------------------------------------------------------------------------

local base_date = tq(2005q1)
forval i = 0/55 {
    
    * Get date
    local date_q = `base_date' + `i'
    local year = year(dofq(`date_q'))
    local quarter = quarter(dofq(`date_q'))
    local _date_q = "`year'Q`quarter'"
    local _date_q_lower = "`year'q`quarter'"
    di ""
    di "Now processing: `_date_q'"
    
    * Get insurers list
    use $cmns1/insurance/temp/market_shares/life_assets.dta, clear
    keep firm assets_`year'y
    qui drop if firm == "Life Industry"
    gsort -assets_`year'y
    qui drop if assets_`year'y < 0
    qui drop if missing(assets_`year'y)
    qui drop if assets_`year'y == 0
    save $cmns1/temp/life_insurers_list_`year', replace

    * Confirm files are all present
    local N = _N
    di "Total insurers: `N'"
    local error_count = 0
    forvalues i=1/`N' {
        local firm = subinstr(firm[`i'], " ", "_", .)
        local firm = subinstr("`firm'", "/", "-", .)
        local fpath = ""
        cap confirm file "$raw/sp_insurance/life/`firm'-`_date_q'.xlsm"
        if _rc==0 {
            local fpath = "$raw/sp_insurance/life/`firm'-`_date_q'.xlsm"
        }
        else {
            cap confirm file "$raw/sp_insurance/life/`firm'-`_date_q'.xls"
            if _rc == 0 {
                local fpath = "$raw/sp_insurance/life/`firm'-`_date_q'.xls"
            }
            else {
                local error_count = `error_count' + 1
            }
        }
    }
    if `error_count' == 0 {
        di "SUCCESS: All life insurers present for `_date_q'!"
    }
    else {
        di "WARNING: Missing `error_count' insurers for `_date_q'"
    }

}

* Process bonds
local imported_count = 0
forvalues i=1/`N' {
    
    * Get file path
    use $cmns1/temp/life_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/life/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/life/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/life/`firm'-2017Q4.xls"
    }
    
    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/life/", "", .)
        
    qui import excel using "`fpath'", clear sheet("Bonds")
    qui drop if _n < 7
    qui drop if _n >= 2 & _n <= 7
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop AD
    cap drop AE

    qui keep Entity_Name CUSIP__ Group_Name Investment_Current_ Issuer_Name__ Asset_Issuer_Type__ ///
        Asset_Description_ Asset_Type Carrying_Value____000_ Conditional_Fair_Value____000_ Actual_Cost____000_ ///
        Dividend_Rate_____ Shares_Owned___actual_ Aggregate_Par_Value____000_ Change_in_Unrealized_Value____00 ///
        Fair_Value_per_Unit_____ Maturity_Date NAIC_Designation_or_Market_Indic _Moody_Credit_Rating__ ///
        _Credit_Rating_Direction__ Original_Investment_Date Latest_Investment_Date Bond_Characteristic ///
        As_Reported_Asset_Type__ As_Reported_Issuer_Type_ MI_Schedule_Name
    qui drop if _n == 1

    qui rename (Entity_Name CUSIP__ Group_Name Investment_Current_ Issuer_Name__ Asset_Issuer_Type__ Asset_Description_ Asset_Type Carrying_Value____000_ Conditional_Fair_Value____000_ Actual_Cost____000_ Dividend_Rate_____ Shares_Owned___actual_ Aggregate_Par_Value____000_ Change_in_Unrealized_Value____00 Fair_Value_per_Unit_____ Maturity_Date NAIC_Designation_or_Market_Indic _Moody_Credit_Rating__ _Credit_Rating_Direction__ Original_Investment_Date Latest_Investment_Date Bond_Characteristic As_Reported_Asset_Type__ As_Reported_Issuer_Type_ MI_Schedule_Name) (Entity_Name CUSIP Group_Name Investment_Current Issuer_Name Asset_Issuer_Type Asset_Description Asset_Type Carrying_Value Conditional_Fair_Value Actual_Cost Dividend_Rate Shares_Owned Aggregate_Par_Value Change_in_Unrealized_Value Fair_Value_per_Unit Maturity_Date NAIC_Designation Moody_Credit_Rating Credit_Rating_Direction Original_Investment_Date Latest_Investment_Date Bond_Characteristic As_Reported_Asset_Type As_Reported_Issuer_Type MI_Schedule_Name)

    qui destring Carrying_Value, force replace
    qui destring Conditional_Fair_Value, force replace
    qui destring Actual_Cost, force replace
    qui destring Dividend_Rate, force replace
    qui destring Shares_Owned, force replace
    qui destring Aggregate_Par_Value, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Fair_Value_per_Unit, force replace

    qui gen date_m = tm(2017q4)
    qui gen marketvalue_usd = Conditional_Fair_Value * 1000
    qui gen company_id = "`insurer'"
    qui gen asset_class = "Bonds"
    qui gen insurer_type = "Life"

    qui save "$cmns1/insurance/temp/life/bonds/`insurer'_2017q4.dta", replace    
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported bonds data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported bonds data for a total of `imported_count' life insurers, 2017Q4"

* Process common equities
local imported_count = 0
forvalues i=1/`N' {
    
    * Get file path
    use $cmns1/temp/life_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/life/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/life/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/life/`firm'-2017Q4.xls"
    }
    
    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/life/", "", .)
        
    qui import excel using "`fpath'", clear sheet("Com_Stk")
    qui drop if _n < 7
    qui drop if _n >= 2 & _n <= 7
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop AD
    cap drop AE

    qui keep Entity_Name	CUSIP__	Group_Name	Affiliated_Investment_ ///
        Investment_Current_	Issuer_Name__	Asset_Issuer_Type__	Asset_Description_	Asset_Type ///
        Shares_Owned___actual_	Carrying_Value____000_	Conditional_Fair_Value____000_ ///
        Actual_Cost____000_	Fair_Value_per_Unit_____	Interest_or_Dividends_Received__ ///
        Dividends_Declared_but_Unpaid___	Change_in_Unrealized_Value____00	Impairment____000_ ///
        NAIC_Designation_or_Market_Indic	Original_Investment_Date	Latest_Investment_Date ///
        As_Reported_Asset_Type__	As_Reported_Issuer_Type_	MI_Schedule_Name

    qui drop if _n == 1

    rename (Entity_Name	CUSIP__	Group_Name	Affiliated_Investment_	Investment_Current_	Issuer_Name__	Asset_Issuer_Type__	Asset_Description_	Asset_Type	Shares_Owned___actual_	Carrying_Value____000_	Conditional_Fair_Value____000_	Actual_Cost____000_	Fair_Value_per_Unit_____	Interest_or_Dividends_Received__	Dividends_Declared_but_Unpaid___	Change_in_Unrealized_Value____00	Impairment____000_	NAIC_Designation_or_Market_Indic	Original_Investment_Date	Latest_Investment_Date	As_Reported_Asset_Type__	As_Reported_Issuer_Type_	MI_Schedule_Name) (Entity_Name CUSIP Group_Name Affiliated_Investment Investment_Current Issuer_Name Asset_Issuer_Type Asset_Description Asset_Type Shares_Owned Carrying_Value Conditional_Fair_Value Actual_Cost Fair_Value_per_Unit Interest_or_Dividends_Receive Dividends_Declared_but_Unpaid Change_in_Unrealized_Value Impairment NAIC_Designation Original_Investment_Date Latest_Investment_Date As_Reported_Asset_Type As_Reported_Issuer_Type MI_Schedule_Name)

    qui destring Shares_Owned, force replace
    qui destring Carrying_Value, force replace
    qui destring Conditional_Fair_Value, force replace
    qui destring Actual_Cost, force replace
    qui destring Fair_Value_per_Unit, force replace
    qui destring Interest_or_Dividends_Receive, force replace
    qui destring Dividends_Declared_but_Unpaid, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Impairment, force replace

    qui gen date_m = tm(2017q4)
    qui gen marketvalue_usd = Conditional_Fair_Value * 1000
    qui gen company_id = "`insurer'"
    qui gen asset_class = "Common Equities"
    qui gen insurer_type = "Life"

    qui save "$cmns1/insurance/temp/life/common_equities/`insurer'_2017q4.dta", replace
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported common equities data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported common equities data for a total of `imported_count' life insurers, 2017Q4"

* Process preferred equities
local imported_count = 0
forvalues i=1/`N' {
    
    * Get file path
    use $cmns1/temp/life_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/life/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/life/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/life/`firm'-2017Q4.xls"
    }
    
    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/life/", "", .)
        
    qui import excel using "`fpath'", clear sheet("Pref_Stk")
    qui drop if _n < 7
    qui drop if _n >= 2 & _n <= 7
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop AD
    cap drop AE
    cap drop AA
    cap drop AB

    qui keep Entity_Name	CUSIP__	Group_Name	Affiliated_Investment___	Investment_Current_	Issuer_Name__	Asset_Issuer_Type__	Asset_Description_	Asset_Type	Shares_Owned___actual_	Carrying_Value____000_	Conditional_Fair_Value____000_	Fair_Value_per_Unit_____	Actual_Cost____000_	Interest_or_Dividends_Received__	Dividends_Declared_but_Unpaid___	Change_in_Unrealized_Value____00	Accretion____000_	Chg_in_Carrying_Value____000_	_Moody_Credit_Rating__	As_Reported_Asset_Type__	As_Reported_Issuer_Type_	MI_Schedule_Name

    qui drop if _n == 1

    qui rename (Entity_Name CUSIP__ Group_Name Affiliated_Investment___ Investment_Current_ Issuer_Name__ Asset_Issuer_Type__ Asset_Description_ Asset_Type Shares_Owned___actual_ Carrying_Value____000_ Conditional_Fair_Value____000_ Fair_Value_per_Unit_____ Actual_Cost____000_ Interest_or_Dividends_Received__ Dividends_Declared_but_Unpaid___ Change_in_Unrealized_Value____00 Accretion____000_ Chg_in_Carrying_Value____000_ _Moody_Credit_Rating__ As_Reported_Asset_Type__ As_Reported_Issuer_Type_ MI_Schedule_Name) (Entity_Name CUSIP Group_Name Affiliated_Investment Investment_Current Issuer_Name Asset_Issuer_Type Asset_Description Asset_Type Shares_Owned Carrying_Value Conditional_Fair_Value Fair_Value_per_Unit Actual_Cost Interest_or_Dividends_Received Dividends_Declared_but_Unpaid Change_in_Unrealized_Value Accretion Chg_in_Carrying_Value Moody_Credit_Rating As_Reported_Asset_Type As_Reported_Issuer_Type MI_Schedule_Name)

    qui destring Shares_Owned, force replace
    qui destring Carrying_Value, force replace
    qui destring Conditional_Fair_Value, force replace
    qui destring Fair_Value_per_Unit, force replace
    qui destring Actual_Cost, force replace
    qui destring Interest_or_Dividends_Received, force replace
    qui destring Dividends_Declared_but_Unpaid, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Accretion, force replace
    qui destring Chg_in_Carrying_Value, force replace

    qui gen date_m = tm(2017q4)
    qui gen marketvalue_usd = Conditional_Fair_Value * 1000
    qui gen company_id = "`insurer'"
    qui gen asset_class = "Preferred Equities"
    qui gen insurer_type = "Life"

    qui save "$cmns1/insurance/temp/life/preferred_equities/`insurer'_2017q4.dta", replace
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported preferred equities data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported preferred equities data for a total of `imported_count' life insurers, 2017Q4"

* Process other assets
local imported_count = 0
forvalues i=1/`N' {
    
    * Get file path
    use $cmns1/temp/life_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/life/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/life/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/life/`firm'-2017Q4.xls"
    }
    
    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/life/", "", .)
        
    qui import excel using "`fpath'", clear sheet("Oth_Inv_Asts")
    qui drop if _n < 7
    qui drop if _n >= 2 & _n <= 7
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop AD
    cap drop AE
    cap drop AA
    cap drop AB
    cap drop X

    qui keep Entity_Name CUSIP__ Group_Name Asset_Type Affiliated_Investment__ Investment_Current_ City__ State_or_Other_Location__ Vendor_or_General_Partner__ Original_Investment_Date Asset_Type_and_Investment_Strate Actual_Cost____000_ Conditional_Fair_Value____000_ Carrying_Value____000_ Change_in_Unrealized_Value____00 Impairment____000_ Capitalized_Deferred_Interest___ Investment_Income____000_ Commitment_for_Additional_Invest Shares_Owned__Shares_Outstanding MI_Schedule_Name

    qui drop if _n == 1

    qui rename Entity_Name Entity_Name
    qui rename CUSIP__ CUSIP
    qui rename Group_Name Group_Name
    qui rename Asset_Type Asset_Type
    qui rename Affiliated_Investment__ Affiliated_Investment
    qui rename Investment_Current_ Investment_Current
    qui rename City__ Oth_City
    qui rename State_or_Other_Location__ Oth_State_or_Other_Location
    qui rename Vendor_or_General_Partner__ Oth_Vendor_or_General_Partner
    qui rename Original_Investment_Date Original_Investment_Date
    qui rename Asset_Type_and_Investment_Strate Asset_Type_and_Investment_Strate
    qui rename Actual_Cost____000_ Actual_Cost
    qui rename Conditional_Fair_Value____000_ Conditional_Fair_Value
    qui rename Carrying_Value____000_ Carrying_Value
    qui rename Change_in_Unrealized_Value____00 Change_in_Unrealized_Value
    qui rename Impairment____000_ Impairment
    qui rename Capitalized_Deferred_Interest___ Capitalized_Deferred_Interest
    qui rename Investment_Income____000_ Investment_Income
    qui rename Commitment_for_Additional_Invest Commitment_for_Additional_Invest
    qui rename Shares_Owned__Shares_Outstanding Shares_Owned_by_Outstanding
    qui rename MI_Schedule_Name MI_Schedule_Name

    qui destring Actual_Cost, force replace
    qui destring Conditional_Fair_Value, force replace
    qui destring Carrying_Value, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Impairment, force replace
    qui destring Capitalized_Deferred_Interest, force replace
    qui destring Investment_Income, force replace
    qui destring Commitment_for_Additional_Invest, force replace
    qui destring Shares_Owned_by_Outstanding, force replace

    qui gen date_m = tm(2017q4)
    qui gen marketvalue_usd = Conditional_Fair_Value * 1000
    qui gen company_id = "`insurer'"
    qui gen asset_class = "Other"
    qui gen insurer_type = "Life"

    qui save "$cmns1/insurance/temp/life/other/`insurer'_2017q4.dta", replace
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported other assets data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported other assets data for a total of `imported_count' life insurers, 2017Q4"

* Process mortgage loans
local imported_count = 0
forvalues i=1/`N' {
        
    * Get file path
    use $cmns1/temp/life_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/life/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/life/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/life/`firm'-2017Q4.xls"
    }

    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/life/", "", .)

    qui import excel using "`fpath'", clear sheet("MRTG_Loans")
    qui drop if _n < 9
    qui drop if _n >= 2 & _n <= 4
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop V
    qui drop if _n == 1

    rename (Mortgage_Loan_Number_  SNL_Group_Name  Asset_Type_  Investment_Current__Yes_No  City_  State_or_Other_Location_  Mortgage_Loan_Standing_  Original_Investment__m_d_yyyy_  Mortgage_Loan_Building___Land_Va  Effective_Interest_Rate____  Change_in_Unrealized_Value___000  Accretion___000_  Impairment___000_  Capitalized_Deferred_Interest___  Foreign_Exchange_Change_in_Book_  Mortgage_Loan_Book_Value_excl_Ac  Appraisal_Date__mm_dd_yyyy_) (Mortgage_Loan_Number SNL_Group_Name Asset_Type Investment_Current  City State_or_Other_Location Mortgage_Loan_Standing Original_Investment Building_and_Land_Value Effective_Interest_Rate Change_in_Unrealized_Value Accretion Impairment Capitalized_Deferred_Interest Foreign_Exchange_Change_in_Book Book_Value_ex_Accrued_Interest Appraisal_Date)

    qui destring Mortgage_Loan_Standing, force replace
    qui destring Effective_Interest_Rate, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Accretion, force replace
    qui destring Impairment, force replace
    qui destring Capitalized_Deferred_Interest, force replace
    qui destring Foreign_Exchange_Change_in_Book, force replace
    qui destring Book_Value_ex_Accrued_Interest, force replace
    qui destring Building_and_Land_Value, force replace

    qui gen date_m = tm(2017m12)
    qui gen building_land_value_usd = Building_and_Land_Value * 1000
    qui gen book_value_usd = Book_Value_ex_Accrued_Interest * 1000

    qui gen company_id = "`insurer'"
    qui gen asset_class = "Mortgage Loans"
    qui gen insurer_type = "Life"

    qui save "$cmns1/insurance/temp/life/loans/`insurer'_2017q4.dta", replace
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported loans assets data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported loans assets data for a total of `imported_count' life insurers, 2017Q4"

* ---------------------------------------------------------------------------------------------------
* Import health insurer holdings
* ---------------------------------------------------------------------------------------------------

* Get insurers list
use $cmns1/insurance/temp/market_shares/health_assets.dta, clear
keep firm assets_2017y
qui drop if firm == "Health Industry"
gsort -assets_2017y
qui drop if assets_2017y < 0
qui drop if missing(assets_2017y)
qui drop if assets_2017y == 0
qui replace firm = "VersantHealth (SNL Health Group)" if firm == "Versant Health Inc. (SNL Health Group)"
count
save $cmns1/temp/health_insurers_list, replace

* Confirm files are all present
local N = _N
local error_count = 0
forvalues i=1/`N' {
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    local fpath = ""
    cap confirm file "$raw/sp_insurance/health/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/health/`firm'-2017Q4.xlsm"
    }
    else {
        cap confirm file "$raw/sp_insurance/health/`firm'-2017Q4.xls"
        if _rc == 0 {
            local fpath = "$raw/sp_insurance/health/`firm'-2017Q4.xls"
        }
        else {
            di "ERROR: Cannot find 2017Q4 file for `firm'"
            local error_count = `error_count' + 1
        }
    }
}
if `error_count' == 0 {
    di "SUCCESS: All health insurers present for 2017Q4!"
}

* Process bonds
local imported_count = 0
forvalues i=1/`N' {
    
    * Get file path
    use $cmns1/temp/health_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/health/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/health/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/health/`firm'-2017Q4.xls"
    }
    
    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/health/", "", .)
        
    qui import excel using "`fpath'", clear sheet("Bonds")
    qui drop if _n < 7
    qui drop if _n >= 2 & _n <= 7
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop AD
    cap drop AE

    qui keep Entity_Name CUSIP__ Group_Name Investment_Current_ Issuer_Name__ Asset_Issuer_Type__ ///
        Asset_Description_ Asset_Type Carrying_Value____000_ Conditional_Fair_Value____000_ Actual_Cost____000_ ///
        Dividend_Rate_____ Shares_Owned___actual_ Aggregate_Par_Value____000_ Change_in_Unrealized_Value____00 ///
        Fair_Value_per_Unit_____ Maturity_Date NAIC_Designation_or_Market_Indic _Moody_Credit_Rating__ ///
        _Credit_Rating_Direction__ Original_Investment_Date Latest_Investment_Date Bond_Characteristic ///
        As_Reported_Asset_Type__ As_Reported_Issuer_Type_ MI_Schedule_Name
    qui drop if _n == 1

    qui rename (Entity_Name CUSIP__ Group_Name Investment_Current_ Issuer_Name__ Asset_Issuer_Type__ Asset_Description_ Asset_Type Carrying_Value____000_ Conditional_Fair_Value____000_ Actual_Cost____000_ Dividend_Rate_____ Shares_Owned___actual_ Aggregate_Par_Value____000_ Change_in_Unrealized_Value____00 Fair_Value_per_Unit_____ Maturity_Date NAIC_Designation_or_Market_Indic _Moody_Credit_Rating__ _Credit_Rating_Direction__ Original_Investment_Date Latest_Investment_Date Bond_Characteristic As_Reported_Asset_Type__ As_Reported_Issuer_Type_ MI_Schedule_Name) (Entity_Name CUSIP Group_Name Investment_Current Issuer_Name Asset_Issuer_Type Asset_Description Asset_Type Carrying_Value Conditional_Fair_Value Actual_Cost Dividend_Rate Shares_Owned Aggregate_Par_Value Change_in_Unrealized_Value Fair_Value_per_Unit Maturity_Date NAIC_Designation Moody_Credit_Rating Credit_Rating_Direction Original_Investment_Date Latest_Investment_Date Bond_Characteristic As_Reported_Asset_Type As_Reported_Issuer_Type MI_Schedule_Name)

    qui destring Carrying_Value, force replace
    qui destring Conditional_Fair_Value, force replace
    qui destring Actual_Cost, force replace
    qui destring Dividend_Rate, force replace
    qui destring Shares_Owned, force replace
    qui destring Aggregate_Par_Value, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Fair_Value_per_Unit, force replace

    qui gen date_m = tm(2017q4)
    qui gen marketvalue_usd = Conditional_Fair_Value * 1000
    qui gen company_id = "`insurer'"
    qui gen asset_class = "Bonds"
    qui gen insurer_type = "Health"

    qui save "$cmns1/insurance/temp/health/bonds/`insurer'_2017q4.dta", replace    
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported bonds data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported bonds data for a total of `imported_count' health insurers, 2017Q4"

* Process common equities
local imported_count = 0
forvalues i=1/`N' {
    
    * Get file path
    use $cmns1/temp/health_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/health/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/health/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/health/`firm'-2017Q4.xls"
    }
    
    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/health/", "", .)
        
    qui import excel using "`fpath'", clear sheet("Com_Stk")
    qui drop if _n < 7
    qui drop if _n >= 2 & _n <= 7
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop AD
    cap drop AE

    qui keep Entity_Name    CUSIP__ Group_Name  Affiliated_Investment_ ///
        Investment_Current_ Issuer_Name__   Asset_Issuer_Type__ Asset_Description_  Asset_Type ///
        Shares_Owned___actual_  Carrying_Value____000_  Conditional_Fair_Value____000_ ///
        Actual_Cost____000_ Fair_Value_per_Unit_____    Interest_or_Dividends_Received__ ///
        Dividends_Declared_but_Unpaid___    Change_in_Unrealized_Value____00    Impairment____000_ ///
        NAIC_Designation_or_Market_Indic    Original_Investment_Date    Latest_Investment_Date ///
        As_Reported_Asset_Type__    As_Reported_Issuer_Type_    MI_Schedule_Name

    qui drop if _n == 1

    rename (Entity_Name CUSIP__ Group_Name  Affiliated_Investment_  Investment_Current_ Issuer_Name__   Asset_Issuer_Type__ Asset_Description_  Asset_Type  Shares_Owned___actual_  Carrying_Value____000_  Conditional_Fair_Value____000_  Actual_Cost____000_ Fair_Value_per_Unit_____    Interest_or_Dividends_Received__    Dividends_Declared_but_Unpaid___    Change_in_Unrealized_Value____00    Impairment____000_  NAIC_Designation_or_Market_Indic    Original_Investment_Date    Latest_Investment_Date  As_Reported_Asset_Type__    As_Reported_Issuer_Type_    MI_Schedule_Name) (Entity_Name CUSIP Group_Name Affiliated_Investment Investment_Current Issuer_Name Asset_Issuer_Type Asset_Description Asset_Type Shares_Owned Carrying_Value Conditional_Fair_Value Actual_Cost Fair_Value_per_Unit Interest_or_Dividends_Receive Dividends_Declared_but_Unpaid Change_in_Unrealized_Value Impairment NAIC_Designation Original_Investment_Date Latest_Investment_Date As_Reported_Asset_Type As_Reported_Issuer_Type MI_Schedule_Name)

    qui destring Shares_Owned, force replace
    qui destring Carrying_Value, force replace
    qui destring Conditional_Fair_Value, force replace
    qui destring Actual_Cost, force replace
    qui destring Fair_Value_per_Unit, force replace
    qui destring Interest_or_Dividends_Receive, force replace
    qui destring Dividends_Declared_but_Unpaid, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Impairment, force replace

    qui gen date_m = tm(2017q4)
    qui gen marketvalue_usd = Conditional_Fair_Value * 1000
    qui gen company_id = "`insurer'"
    qui gen asset_class = "Common Equities"
    qui gen insurer_type = "Health"

    qui save "$cmns1/insurance/temp/health/common_equities/`insurer'_2017q4.dta", replace
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported common equities data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported common equities data for a total of `imported_count' health insurers, 2017Q4"

* Process preferred equities
local imported_count = 0
forvalues i=1/`N' {
    
    * Get file path
    use $cmns1/temp/health_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/health/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/health/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/health/`firm'-2017Q4.xls"
    }
    
    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/health/", "", .)
        
    qui import excel using "`fpath'", clear sheet("Pref_Stk")
    qui drop if _n < 7
    qui drop if _n >= 2 & _n <= 7
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop AD
    cap drop AE
    cap drop AA
    cap drop AB

    qui keep Entity_Name    CUSIP__ Group_Name  Affiliated_Investment___    Investment_Current_ Issuer_Name__   Asset_Issuer_Type__ Asset_Description_  Asset_Type  Shares_Owned___actual_  Carrying_Value____000_  Conditional_Fair_Value____000_  Fair_Value_per_Unit_____    Actual_Cost____000_ Interest_or_Dividends_Received__    Dividends_Declared_but_Unpaid___    Change_in_Unrealized_Value____00    Accretion____000_   Chg_in_Carrying_Value____000_   _Moody_Credit_Rating__  As_Reported_Asset_Type__    As_Reported_Issuer_Type_    MI_Schedule_Name

    qui drop if _n == 1

    qui rename (Entity_Name CUSIP__ Group_Name Affiliated_Investment___ Investment_Current_ Issuer_Name__ Asset_Issuer_Type__ Asset_Description_ Asset_Type Shares_Owned___actual_ Carrying_Value____000_ Conditional_Fair_Value____000_ Fair_Value_per_Unit_____ Actual_Cost____000_ Interest_or_Dividends_Received__ Dividends_Declared_but_Unpaid___ Change_in_Unrealized_Value____00 Accretion____000_ Chg_in_Carrying_Value____000_ _Moody_Credit_Rating__ As_Reported_Asset_Type__ As_Reported_Issuer_Type_ MI_Schedule_Name) (Entity_Name CUSIP Group_Name Affiliated_Investment Investment_Current Issuer_Name Asset_Issuer_Type Asset_Description Asset_Type Shares_Owned Carrying_Value Conditional_Fair_Value Fair_Value_per_Unit Actual_Cost Interest_or_Dividends_Received Dividends_Declared_but_Unpaid Change_in_Unrealized_Value Accretion Chg_in_Carrying_Value Moody_Credit_Rating As_Reported_Asset_Type As_Reported_Issuer_Type MI_Schedule_Name)

    qui destring Shares_Owned, force replace
    qui destring Carrying_Value, force replace
    qui destring Conditional_Fair_Value, force replace
    qui destring Fair_Value_per_Unit, force replace
    qui destring Actual_Cost, force replace
    qui destring Interest_or_Dividends_Received, force replace
    qui destring Dividends_Declared_but_Unpaid, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Accretion, force replace
    qui destring Chg_in_Carrying_Value, force replace

    qui gen date_m = tm(2017q4)
    qui gen marketvalue_usd = Conditional_Fair_Value * 1000
    qui gen company_id = "`insurer'"
    qui gen asset_class = "Preferred Equities"
    qui gen insurer_type = "Health"

    qui save "$cmns1/insurance/temp/health/preferred_equities/`insurer'_2017q4.dta", replace
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported preferred equities data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported preferred equities data for a total of `imported_count' health insurers, 2017Q4"

* Process other assets
local imported_count = 0
forvalues i=1/`N' {
    
    * Get file path
    use $cmns1/temp/health_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/health/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/health/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/health/`firm'-2017Q4.xls"
    }
    
    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/health/", "", .)
        
    qui import excel using "`fpath'", clear sheet("Oth_Inv_Asts")
    qui drop if _n < 7
    qui drop if _n >= 2 & _n <= 7
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop AD
    cap drop AE
    cap drop AA
    cap drop AB
    cap drop X

    qui keep Entity_Name CUSIP__ Group_Name Asset_Type Affiliated_Investment__ Investment_Current_ City__ State_or_Other_Location__ Vendor_or_General_Partner__ Original_Investment_Date Asset_Type_and_Investment_Strate Actual_Cost____000_ Conditional_Fair_Value____000_ Carrying_Value____000_ Change_in_Unrealized_Value____00 Impairment____000_ Capitalized_Deferred_Interest___ Investment_Income____000_ Commitment_for_Additional_Invest Shares_Owned__Shares_Outstanding MI_Schedule_Name

    qui drop if _n == 1

    qui rename Entity_Name Entity_Name
    qui rename CUSIP__ CUSIP
    qui rename Group_Name Group_Name
    qui rename Asset_Type Asset_Type
    qui rename Affiliated_Investment__ Affiliated_Investment
    qui rename Investment_Current_ Investment_Current
    qui rename City__ Oth_City
    qui rename State_or_Other_Location__ Oth_State_or_Other_Location
    qui rename Vendor_or_General_Partner__ Oth_Vendor_or_General_Partner
    qui rename Original_Investment_Date Original_Investment_Date
    qui rename Asset_Type_and_Investment_Strate Asset_Type_and_Investment_Strate
    qui rename Actual_Cost____000_ Actual_Cost
    qui rename Conditional_Fair_Value____000_ Conditional_Fair_Value
    qui rename Carrying_Value____000_ Carrying_Value
    qui rename Change_in_Unrealized_Value____00 Change_in_Unrealized_Value
    qui rename Impairment____000_ Impairment
    qui rename Capitalized_Deferred_Interest___ Capitalized_Deferred_Interest
    qui rename Investment_Income____000_ Investment_Income
    qui rename Commitment_for_Additional_Invest Commitment_for_Additional_Invest
    qui rename Shares_Owned__Shares_Outstanding Shares_Owned_by_Outstanding
    qui rename MI_Schedule_Name MI_Schedule_Name

    qui destring Actual_Cost, force replace
    qui destring Conditional_Fair_Value, force replace
    qui destring Carrying_Value, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Impairment, force replace
    qui destring Capitalized_Deferred_Interest, force replace
    qui destring Investment_Income, force replace
    qui destring Commitment_for_Additional_Invest, force replace
    qui destring Shares_Owned_by_Outstanding, force replace

    qui gen date_m = tm(2017q4)
    qui gen marketvalue_usd = Conditional_Fair_Value * 1000
    qui gen company_id = "`insurer'"
    qui gen asset_class = "Other"
    qui gen insurer_type = "Health"

    qui save "$cmns1/insurance/temp/health/other/`insurer'_2017q4.dta", replace
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported other assets data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported other assets data for a total of `imported_count' health insurers, 2017Q4"

* Process mortgage loans
local imported_count = 0
forvalues i=1/`N' {
        
    * Get file path
    use $cmns1/temp/health_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/health/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/health/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/health/`firm'-2017Q4.xls"
    }

    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/health/", "", .)

    qui import excel using "`fpath'", clear sheet("MRTG_Loans")
    qui drop if _n < 9
    qui drop if _n >= 2 & _n <= 4
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop V
    qui drop if _n == 1

    rename (Mortgage_Loan_Number_  SNL_Group_Name  Asset_Type_  Investment_Current__Yes_No  City_  State_or_Other_Location_  Mortgage_Loan_Standing_  Original_Investment__m_d_yyyy_  Mortgage_Loan_Building___Land_Va  Effective_Interest_Rate____  Change_in_Unrealized_Value___000  Accretion___000_  Impairment___000_  Capitalized_Deferred_Interest___  Foreign_Exchange_Change_in_Book_  Mortgage_Loan_Book_Value_excl_Ac  Appraisal_Date__mm_dd_yyyy_) (Mortgage_Loan_Number SNL_Group_Name Asset_Type Investment_Current  City State_or_Other_Location Mortgage_Loan_Standing Original_Investment Building_and_Land_Value Effective_Interest_Rate Change_in_Unrealized_Value Accretion Impairment Capitalized_Deferred_Interest Foreign_Exchange_Change_in_Book Book_Value_ex_Accrued_Interest Appraisal_Date)

    qui destring Mortgage_Loan_Standing, force replace
    qui destring Effective_Interest_Rate, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Accretion, force replace
    qui destring Impairment, force replace
    qui destring Capitalized_Deferred_Interest, force replace
    qui destring Foreign_Exchange_Change_in_Book, force replace
    qui destring Book_Value_ex_Accrued_Interest, force replace
    qui destring Building_and_Land_Value, force replace

    qui gen date_m = tm(2017m12)
    qui gen building_land_value_usd = Building_and_Land_Value * 1000
    qui gen book_value_usd = Book_Value_ex_Accrued_Interest * 1000

    qui gen company_id = "`insurer'"
    qui gen asset_class = "Mortgage Loans"
    qui gen insurer_type = "Health"

    qui save "$cmns1/insurance/temp/health/loans/`insurer'_2017q4.dta", replace
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported loans assets data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported loans assets data for a total of `imported_count' health insurers, 2017Q4"

* ---------------------------------------------------------------------------------------------------
* Import P&C insurer holdings
* ---------------------------------------------------------------------------------------------------

* Get insurers list
use $cmns1/insurance/temp/market_shares/pc_assets.dta, clear
keep firm assets_2017y
qui drop if firm == "P&C Industry"
gsort -assets_2017y
qui drop if assets_2017y < 0
qui drop if missing(assets_2017y)
qui drop if assets_2017y == 0
qui replace firm = "United Americas Insurance Co." if firm == "Americas Insurance Company (SNL P&C Group)"
qui drop if firm == "Emergency Cap Mgmt LLC A RRG"
qui drop if firm == "Rush Fire Insurance Co."
qui drop if firm == "Roche Surety & Casualty Co."
qui drop if firm == "Centre County Mutl Fire Ins Co"
qui drop if firm == "Vasa-Spring Garden Mutl Ins Co"
qui drop if firm == "PA Professional Liability JUA"
qui drop if firm == "Trans City Casualty Ins Co."
count
save $cmns1/temp/pc_insurers_list, replace

* Confirm files are all present
local N = _N
local error_count = 0
forvalues i=1/`N' {
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    local fpath = ""
    cap confirm file "$raw/sp_insurance/pc/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/pc/`firm'-2017Q4.xlsm"
    }
    else {
        cap confirm file "$raw/sp_insurance/pc/`firm'-2017Q4.xls"
        if _rc == 0 {
            local fpath = "$raw/sp_insurance/pc/`firm'-2017Q4.xls"
        }
        else {
            di "ERROR: Cannot find 2017Q4 file for `firm'"
            local error_count = `error_count' + 1
        }
    }
}
if `error_count' == 0 {
    di "SUCCESS: All P&C insurers present for 2017Q4!"
}

* Process bonds
local imported_count = 0
forvalues i=1/`N' {
    
    * Get file path
    use $cmns1/temp/pc_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/pc/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/pc/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/pc/`firm'-2017Q4.xls"
    }
    
    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/pc/", "", .)
        
    qui import excel using "`fpath'", clear sheet("Bonds")
    qui drop if _n < 7
    qui drop if _n >= 2 & _n <= 7
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop AD
    cap drop AE

    qui keep Entity_Name CUSIP__ Group_Name Investment_Current_ Issuer_Name__ Asset_Issuer_Type__ ///
        Asset_Description_ Asset_Type Carrying_Value____000_ Conditional_Fair_Value____000_ Actual_Cost____000_ ///
        Dividend_Rate_____ Shares_Owned___actual_ Aggregate_Par_Value____000_ Change_in_Unrealized_Value____00 ///
        Fair_Value_per_Unit_____ Maturity_Date NAIC_Designation_or_Market_Indic _Moody_Credit_Rating__ ///
        _Credit_Rating_Direction__ Original_Investment_Date Latest_Investment_Date Bond_Characteristic ///
        As_Reported_Asset_Type__ As_Reported_Issuer_Type_ MI_Schedule_Name
    qui drop if _n == 1

    qui rename (Entity_Name CUSIP__ Group_Name Investment_Current_ Issuer_Name__ Asset_Issuer_Type__ Asset_Description_ Asset_Type Carrying_Value____000_ Conditional_Fair_Value____000_ Actual_Cost____000_ Dividend_Rate_____ Shares_Owned___actual_ Aggregate_Par_Value____000_ Change_in_Unrealized_Value____00 Fair_Value_per_Unit_____ Maturity_Date NAIC_Designation_or_Market_Indic _Moody_Credit_Rating__ _Credit_Rating_Direction__ Original_Investment_Date Latest_Investment_Date Bond_Characteristic As_Reported_Asset_Type__ As_Reported_Issuer_Type_ MI_Schedule_Name) (Entity_Name CUSIP Group_Name Investment_Current Issuer_Name Asset_Issuer_Type Asset_Description Asset_Type Carrying_Value Conditional_Fair_Value Actual_Cost Dividend_Rate Shares_Owned Aggregate_Par_Value Change_in_Unrealized_Value Fair_Value_per_Unit Maturity_Date NAIC_Designation Moody_Credit_Rating Credit_Rating_Direction Original_Investment_Date Latest_Investment_Date Bond_Characteristic As_Reported_Asset_Type As_Reported_Issuer_Type MI_Schedule_Name)

    qui destring Carrying_Value, force replace
    qui destring Conditional_Fair_Value, force replace
    qui destring Actual_Cost, force replace
    qui destring Dividend_Rate, force replace
    qui destring Shares_Owned, force replace
    qui destring Aggregate_Par_Value, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Fair_Value_per_Unit, force replace

    qui gen date_m = tm(2017q4)
    qui gen marketvalue_usd = Conditional_Fair_Value * 1000
    qui gen company_id = "`insurer'"
    qui gen asset_class = "Bonds"
    qui gen insurer_type = "P&C"

    qui save "$cmns1/insurance/temp/pc/bonds/`insurer'_2017q4.dta", replace    
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported bonds data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported bonds data for a total of `imported_count' P&C insurers, 2017Q4"

* Process common equities
local imported_count = 0
forvalues i=1/`N' {
    
    * Get file path
    use $cmns1/temp/pc_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/pc/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/pc/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/pc/`firm'-2017Q4.xls"
    }
    
    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/pc/", "", .)
        
    qui import excel using "`fpath'", clear sheet("Com_Stk")
    qui drop if _n < 7
    qui drop if _n >= 2 & _n <= 7
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop AD
    cap drop AE

    qui keep Entity_Name    CUSIP__ Group_Name  Affiliated_Investment_ ///
        Investment_Current_ Issuer_Name__   Asset_Issuer_Type__ Asset_Description_  Asset_Type ///
        Shares_Owned___actual_  Carrying_Value____000_  Conditional_Fair_Value____000_ ///
        Actual_Cost____000_ Fair_Value_per_Unit_____    Interest_or_Dividends_Received__ ///
        Dividends_Declared_but_Unpaid___    Change_in_Unrealized_Value____00    Impairment____000_ ///
        NAIC_Designation_or_Market_Indic    Original_Investment_Date    Latest_Investment_Date ///
        As_Reported_Asset_Type__    As_Reported_Issuer_Type_    MI_Schedule_Name

    qui drop if _n == 1

    rename (Entity_Name CUSIP__ Group_Name  Affiliated_Investment_  Investment_Current_ Issuer_Name__   Asset_Issuer_Type__ Asset_Description_  Asset_Type  Shares_Owned___actual_  Carrying_Value____000_  Conditional_Fair_Value____000_  Actual_Cost____000_ Fair_Value_per_Unit_____    Interest_or_Dividends_Received__    Dividends_Declared_but_Unpaid___    Change_in_Unrealized_Value____00    Impairment____000_  NAIC_Designation_or_Market_Indic    Original_Investment_Date    Latest_Investment_Date  As_Reported_Asset_Type__    As_Reported_Issuer_Type_    MI_Schedule_Name) (Entity_Name CUSIP Group_Name Affiliated_Investment Investment_Current Issuer_Name Asset_Issuer_Type Asset_Description Asset_Type Shares_Owned Carrying_Value Conditional_Fair_Value Actual_Cost Fair_Value_per_Unit Interest_or_Dividends_Receive Dividends_Declared_but_Unpaid Change_in_Unrealized_Value Impairment NAIC_Designation Original_Investment_Date Latest_Investment_Date As_Reported_Asset_Type As_Reported_Issuer_Type MI_Schedule_Name)

    qui destring Shares_Owned, force replace
    qui destring Carrying_Value, force replace
    qui destring Conditional_Fair_Value, force replace
    qui destring Actual_Cost, force replace
    qui destring Fair_Value_per_Unit, force replace
    qui destring Interest_or_Dividends_Receive, force replace
    qui destring Dividends_Declared_but_Unpaid, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Impairment, force replace

    qui gen date_m = tm(2017q4)
    qui gen marketvalue_usd = Conditional_Fair_Value * 1000
    qui gen company_id = "`insurer'"
    qui gen asset_class = "Common Equities"
    qui gen insurer_type = "P&C"

    qui save "$cmns1/insurance/temp/pc/common_equities/`insurer'_2017q4.dta", replace
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported common equities data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported common equities data for a total of `imported_count' P&C insurers, 2017Q4"

* Process preferred equities
local imported_count = 0
forvalues i=1/`N' {
    
    * Get file path
    use $cmns1/temp/pc_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/pc/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/pc/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/pc/`firm'-2017Q4.xls"
    }
    
    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/pc/", "", .)
        
    qui import excel using "`fpath'", clear sheet("Pref_Stk")
    qui drop if _n < 7
    qui drop if _n >= 2 & _n <= 7
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop AD
    cap drop AE
    cap drop AA
    cap drop AB

    qui keep Entity_Name    CUSIP__ Group_Name  Affiliated_Investment___    Investment_Current_ Issuer_Name__   Asset_Issuer_Type__ Asset_Description_  Asset_Type  Shares_Owned___actual_  Carrying_Value____000_  Conditional_Fair_Value____000_  Fair_Value_per_Unit_____    Actual_Cost____000_ Interest_or_Dividends_Received__    Dividends_Declared_but_Unpaid___    Change_in_Unrealized_Value____00    Accretion____000_   Chg_in_Carrying_Value____000_   _Moody_Credit_Rating__  As_Reported_Asset_Type__    As_Reported_Issuer_Type_    MI_Schedule_Name

    qui drop if _n == 1

    qui rename (Entity_Name CUSIP__ Group_Name Affiliated_Investment___ Investment_Current_ Issuer_Name__ Asset_Issuer_Type__ Asset_Description_ Asset_Type Shares_Owned___actual_ Carrying_Value____000_ Conditional_Fair_Value____000_ Fair_Value_per_Unit_____ Actual_Cost____000_ Interest_or_Dividends_Received__ Dividends_Declared_but_Unpaid___ Change_in_Unrealized_Value____00 Accretion____000_ Chg_in_Carrying_Value____000_ _Moody_Credit_Rating__ As_Reported_Asset_Type__ As_Reported_Issuer_Type_ MI_Schedule_Name) (Entity_Name CUSIP Group_Name Affiliated_Investment Investment_Current Issuer_Name Asset_Issuer_Type Asset_Description Asset_Type Shares_Owned Carrying_Value Conditional_Fair_Value Fair_Value_per_Unit Actual_Cost Interest_or_Dividends_Received Dividends_Declared_but_Unpaid Change_in_Unrealized_Value Accretion Chg_in_Carrying_Value Moody_Credit_Rating As_Reported_Asset_Type As_Reported_Issuer_Type MI_Schedule_Name)

    qui destring Shares_Owned, force replace
    qui destring Carrying_Value, force replace
    qui destring Conditional_Fair_Value, force replace
    qui destring Fair_Value_per_Unit, force replace
    qui destring Actual_Cost, force replace
    qui destring Interest_or_Dividends_Received, force replace
    qui destring Dividends_Declared_but_Unpaid, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Accretion, force replace
    qui destring Chg_in_Carrying_Value, force replace

    qui gen date_m = tm(2017q4)
    qui gen marketvalue_usd = Conditional_Fair_Value * 1000
    qui gen company_id = "`insurer'"
    qui gen asset_class = "Preferred Equities"
    qui gen insurer_type = "P&C"

    qui save "$cmns1/insurance/temp/pc/preferred_equities/`insurer'_2017q4.dta", replace
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported preferred equities data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported preferred equities data for a total of `imported_count' P&C insurers, 2017Q4"

* Process other assets
local imported_count = 0
forvalues i=1/`N' {
    
    * Get file path
    use $cmns1/temp/pc_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/pc/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/pc/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/pc/`firm'-2017Q4.xls"
    }
    
    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/pc/", "", .)
        
    qui import excel using "`fpath'", clear sheet("Oth_Inv_Asts")
    qui drop if _n < 7
    qui drop if _n >= 2 & _n <= 7
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop AD
    cap drop AE
    cap drop AA
    cap drop AB
    cap drop X

    qui keep Entity_Name CUSIP__ Group_Name Asset_Type Affiliated_Investment__ Investment_Current_ City__ State_or_Other_Location__ Vendor_or_General_Partner__ Original_Investment_Date Asset_Type_and_Investment_Strate Actual_Cost____000_ Conditional_Fair_Value____000_ Carrying_Value____000_ Change_in_Unrealized_Value____00 Impairment____000_ Capitalized_Deferred_Interest___ Investment_Income____000_ Commitment_for_Additional_Invest Shares_Owned__Shares_Outstanding MI_Schedule_Name

    qui drop if _n == 1

    qui rename Entity_Name Entity_Name
    qui rename CUSIP__ CUSIP
    qui rename Group_Name Group_Name
    qui rename Asset_Type Asset_Type
    qui rename Affiliated_Investment__ Affiliated_Investment
    qui rename Investment_Current_ Investment_Current
    qui rename City__ Oth_City
    qui rename State_or_Other_Location__ Oth_State_or_Other_Location
    qui rename Vendor_or_General_Partner__ Oth_Vendor_or_General_Partner
    qui rename Original_Investment_Date Original_Investment_Date
    qui rename Asset_Type_and_Investment_Strate Asset_Type_and_Investment_Strate
    qui rename Actual_Cost____000_ Actual_Cost
    qui rename Conditional_Fair_Value____000_ Conditional_Fair_Value
    qui rename Carrying_Value____000_ Carrying_Value
    qui rename Change_in_Unrealized_Value____00 Change_in_Unrealized_Value
    qui rename Impairment____000_ Impairment
    qui rename Capitalized_Deferred_Interest___ Capitalized_Deferred_Interest
    qui rename Investment_Income____000_ Investment_Income
    qui rename Commitment_for_Additional_Invest Commitment_for_Additional_Invest
    qui rename Shares_Owned__Shares_Outstanding Shares_Owned_by_Outstanding
    qui rename MI_Schedule_Name MI_Schedule_Name

    qui destring Actual_Cost, force replace
    qui destring Conditional_Fair_Value, force replace
    qui destring Carrying_Value, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Impairment, force replace
    qui destring Capitalized_Deferred_Interest, force replace
    qui destring Investment_Income, force replace
    qui destring Commitment_for_Additional_Invest, force replace
    qui destring Shares_Owned_by_Outstanding, force replace

    qui gen date_m = tm(2017q4)
    qui gen marketvalue_usd = Conditional_Fair_Value * 1000
    qui gen company_id = "`insurer'"
    qui gen asset_class = "Other"
    qui gen insurer_type = "P&C"

    qui save "$cmns1/insurance/temp/pc/other/`insurer'_2017q4.dta", replace
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported other assets data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported other assets data for a total of `imported_count' P&C insurers, 2017Q4"

* Process mortgage loans
local imported_count = 0
forvalues i=1/`N' {
        
    * Get file path
    use $cmns1/temp/pc_insurers_list, clear
    local firm = subinstr(firm[`i'], " ", "_", .)
    local firm = subinstr("`firm'", "/", "-", .)
    cap confirm file "$raw/sp_insurance/pc/`firm'-2017Q4.xlsm"
    if _rc==0 {
        local fpath = "$raw/sp_insurance/pc/`firm'-2017Q4.xlsm"
    }
    else {
        local fpath = "$raw/sp_insurance/pc/`firm'-2017Q4.xls"
    }

    local insurer = subinstr("`fpath'", "-2017Q4.xls", "", .)
    local insurer = subinstr("`insurer'", "$raw/sp_insurance/pc/", "", .)

    qui import excel using "`fpath'", clear sheet("MRTG_Loans")
    qui drop if _n < 9
    qui drop if _n >= 2 & _n <= 4
    foreach var of varlist * {
    cap {
         local try = strtoname(`var'[1]) 
         rename `var'  `try' 
    }
    }

    cap drop V
    qui drop if _n == 1

    rename (Mortgage_Loan_Number_  SNL_Group_Name  Asset_Type_  Investment_Current__Yes_No  City_  State_or_Other_Location_  Mortgage_Loan_Standing_  Original_Investment__m_d_yyyy_  Mortgage_Loan_Building___Land_Va  Effective_Interest_Rate____  Change_in_Unrealized_Value___000  Accretion___000_  Impairment___000_  Capitalized_Deferred_Interest___  Foreign_Exchange_Change_in_Book_  Mortgage_Loan_Book_Value_excl_Ac  Appraisal_Date__mm_dd_yyyy_) (Mortgage_Loan_Number SNL_Group_Name Asset_Type Investment_Current  City State_or_Other_Location Mortgage_Loan_Standing Original_Investment Building_and_Land_Value Effective_Interest_Rate Change_in_Unrealized_Value Accretion Impairment Capitalized_Deferred_Interest Foreign_Exchange_Change_in_Book Book_Value_ex_Accrued_Interest Appraisal_Date)

    qui destring Mortgage_Loan_Standing, force replace
    qui destring Effective_Interest_Rate, force replace
    qui destring Change_in_Unrealized_Value, force replace
    qui destring Accretion, force replace
    qui destring Impairment, force replace
    qui destring Capitalized_Deferred_Interest, force replace
    qui destring Foreign_Exchange_Change_in_Book, force replace
    qui destring Book_Value_ex_Accrued_Interest, force replace
    qui destring Building_and_Land_Value, force replace

    qui gen date_m = tm(2017m12)
    qui gen building_land_value_usd = Building_and_Land_Value * 1000
    qui gen book_value_usd = Book_Value_ex_Accrued_Interest * 1000

    qui gen company_id = "`insurer'"
    qui gen asset_class = "Mortgage Loans"
    qui gen insurer_type = "P&C"

    qui save "$cmns1/insurance/temp/pc/loans/`insurer'_2017q4.dta", replace
    local imported_count = `imported_count' + 1
    
    if mod(`imported_count', 10) == 0 {
        di "Imported loans assets data for `imported_count' of `N' insurers"
    }
}

assert `imported_count' == `N'
di "\nCOMPLETED - Imported loans assets data for a total of `imported_count' P&C insurers, 2017Q4"

* ---------------------------------------------------------------------------------------------------
* Consolidate data
* ---------------------------------------------------------------------------------------------------

cap mkdir $cmns1/insurance/insurance_master

* Life insurance
clear

di "Processing bonds"
local bonds_files : dir "$cmns1/insurance/temp/life/bonds" files "*.dta"
foreach file in `bonds_files' {
    qui append using "$cmns1/insurance/temp/life/bonds/`file'"
}

di "Processing common equities"
local common_equities_files : dir "$cmns1/insurance/temp/life/common_equities" files "*.dta"
foreach file in `common_equities_files' {
    qui append using "$cmns1/insurance/temp/life/common_equities/`file'"
}

di "Processing preferred equities"
local preferred_equities_files : dir "$cmns1/insurance/temp/life/preferred_equities" files "*.dta"
foreach file in `preferred_equities_files' {
    qui append using "$cmns1/insurance/temp/life/preferred_equities/`file'"
}

di "Processing other"
local other_files : dir "$cmns1/insurance/temp/life/other" files "*.dta"
foreach file in `other_files' {
    qui append using "$cmns1/insurance/temp/life/other/`file'"
}

qui drop if missing(marketvalue_usd)
qui compress

save $cmns1/insurance/insurance_master/life_insurance_master, replace

* Health insurance
clear

di "Processing bonds"
local bonds_files : dir "$cmns1/insurance/temp/health/bonds" files "*.dta"
foreach file in `bonds_files' {
    qui append using "$cmns1/insurance/temp/health/bonds/`file'"
}

di "Processing common equities"
local common_equities_files : dir "$cmns1/insurance/temp/health/common_equities" files "*.dta"
foreach file in `common_equities_files' {
    qui append using "$cmns1/insurance/temp/health/common_equities/`file'"
}

di "Processing preferred equities"
local preferred_equities_files : dir "$cmns1/insurance/temp/health/preferred_equities" files "*.dta"
foreach file in `preferred_equities_files' {
    qui append using "$cmns1/insurance/temp/health/preferred_equities/`file'"
}

di "Processing other"
local other_files : dir "$cmns1/insurance/temp/health/other" files "*.dta"
foreach file in `other_files' {
    qui append using "$cmns1/insurance/temp/health/other/`file'"
}

qui drop if missing(marketvalue_usd)
qui compress

save $cmns1/insurance/insurance_master/health_insurance_master, replace

* P&C insurance
clear

di "Processing bonds"
local bonds_files : dir "$cmns1/insurance/temp/pc/bonds" files "*.dta"
foreach file in `bonds_files' {
    qui append using "$cmns1/insurance/temp/pc/bonds/`file'"
}

di "Processing common equities"
local common_equities_files : dir "$cmns1/insurance/temp/pc/common_equities" files "*.dta"
foreach file in `common_equities_files' {
    qui append using "$cmns1/insurance/temp/pc/common_equities/`file'"
}

di "Processing preferred equities"
local preferred_equities_files : dir "$cmns1/insurance/temp/pc/preferred_equities" files "*.dta"
foreach file in `preferred_equities_files' {
    qui append using "$cmns1/insurance/temp/pc/preferred_equities/`file'"
}

di "Processing other"
local other_files : dir "$cmns1/insurance/temp/pc/other" files "*.dta"
foreach file in `other_files' {
    qui append using "$cmns1/insurance/temp/pc/other/`file'"
}

qui drop if missing(marketvalue_usd)
qui compress

save $cmns1/insurance/insurance_master/pc_insurance_master, replace

* Append all
clear
use $cmns1/insurance/insurance_master/life_insurance_master, clear
append using $cmns1/insurance/insurance_master/health_insurance_master
append using $cmns1/insurance/insurance_master/pc_insurance_master
replace date_m = tq(2017q4) if date_m == tm(2017m4)
rename date_m date_q
format %tq date_q
compress
rename CUSIP cusip
save $cmns1/insurance/insurance_master/all_insurance_master, replace

* ---------------------------------------------------------------------------------------------------
* Consolidate mortgage loans data
* ---------------------------------------------------------------------------------------------------

* Mortgage loans
clear
local files : dir "$cmns1/insurance/temp/life/loans" files "*.dta"
foreach file in `files' {
    qui append using "$cmns1/insurance/temp/life/loans/`file'"
}
local files : dir "$cmns1/insurance/temp/health/loans" files "*.dta"
foreach file in `files' {
    qui append using "$cmns1/insurance/temp/health/loans/`file'"
}
local files : dir "$cmns1/insurance/temp/pc/loans" files "*.dta"
foreach file in `files' {
    qui append using "$cmns1/insurance/temp/pc/loans/`file'"
}

drop if missing(Mortgage_Loan_Number) & missing(Asset_Type) & missing(Building_and_Land_Value) & missing(Change_in_Unrealized_Value) & missing(Book_Value_ex_Accrued_Interest)
compress
format %tm date_m

destring Building_and_Land_Value, force replace
cap drop building_land_value_usd
gen building_land_value_usd = Building_and_Land_Value * 1000

save $cmns1/insurance/insurance_master/insurance_mortgage_loans_all, replace

log close
