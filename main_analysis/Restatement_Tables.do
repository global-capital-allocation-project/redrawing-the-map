* ---------------------------------------------------------------------------------------------------
* Restatement_Tables: Produces and formats restatements tables for the paper
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Restatement_Tables, replace

* ---------------------------------------------------------------------------------------------------
* TIC/CPIS main restatement: US, corporate bonds
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "USA" & Asset_Class_Code == "BC", clear
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 4 if Issuer == "CAN"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 6 if Issuer == "FRA"
qui replace rank = 7 if Issuer == "DEU"
qui replace rank = 8 if Issuer == "IND"
qui replace rank = 9 if Issuer == "IDN"
qui replace rank = 10 if Issuer == "ITA"
qui replace rank = 11 if Issuer == "JPN"
qui replace rank = 12 if Issuer == "MEX"
qui replace rank = 13 if Issuer == "RUS"
qui replace rank = 14 if Issuer == "SAU"
qui replace rank = 15 if Issuer == "ESP"
qui replace rank = 16 if Issuer == "ZAF"
qui replace rank = 17 if Issuer == "KOR"
qui replace rank = 18 if Issuer == "TUR"
qui replace rank = 19 if Issuer == "GBR"
qui replace rank = 20 if Issuer == "BMU"
qui replace rank = 21 if Issuer == "CYM"
qui replace rank = 22 if Issuer == "CUW"
qui replace rank = 23 if Issuer == "GGY"
qui replace rank = 24 if Issuer == "HKG"
qui replace rank = 25 if Issuer == "IRL"
qui replace rank = 26 if Issuer == "JEY"
qui replace rank = 27 if Issuer == "LUX"
qui replace rank = 28 if Issuer == "NLD"
qui replace rank = 29 if Issuer == "PAN"
qui replace rank = 30 if Issuer == "VGB"
qui replace rank = 31 if Issuer == "USA"
qui drop if missing(rank)
gsort rank
drop rank
keep Issuer Position_Residency Position_Nationality_TH_Only
gen Delta_TH_Only = Position_Nationality_TH_Only - Position_Residency
qui gen _ = .
qui gen __ = .
order Issuer _ Position_Residency __ Position_Nationality_TH_Only
replace Position_Residency = round(Position_Residency)
replace Position_Nationality_TH_Only = round(Position_Nationality_TH_Only)
replace Delta_TH_Only = round(Delta_TH_Only)

save $cmns1/tables/baseline_restatements_us_bc, replace

* ---------------------------------------------------------------------------------------------------
* TIC/CPIS main restatement: US, equities
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "USA" & Asset_Class_Code == "E", clear
qui mmerge Issuer using $scratch/tic_all_equity.dta, unmatched(m)
qui gen _TIC_All = TIC_All if Issuer != "USA"
qui gen _Position_Residency = Position_Residency if Issuer != "USA"
qui egen Tot_All = total(_TIC_All)
qui egen Tot_Common = total(_Position_Residency)
qui replace TIC_All = Position_Residency * Tot_All / Tot_Common if Issuer == "USA"
drop Tot_* _*
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 4 if Issuer == "CAN"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 6 if Issuer == "FRA"
qui replace rank = 7 if Issuer == "DEU"
qui replace rank = 8 if Issuer == "IND"
qui replace rank = 9 if Issuer == "IDN"
qui replace rank = 10 if Issuer == "ITA"
qui replace rank = 11 if Issuer == "JPN"
qui replace rank = 12 if Issuer == "MEX"
qui replace rank = 13 if Issuer == "RUS"
qui replace rank = 14 if Issuer == "SAU"
qui replace rank = 15 if Issuer == "ESP"
qui replace rank = 16 if Issuer == "ZAF"
qui replace rank = 17 if Issuer == "KOR"
qui replace rank = 18 if Issuer == "TUR"
qui replace rank = 19 if Issuer == "GBR"
qui replace rank = 20 if Issuer == "BMU"
qui replace rank = 21 if Issuer == "CYM"
qui replace rank = 22 if Issuer == "CUW"
qui replace rank = 23 if Issuer == "GGY"
qui replace rank = 24 if Issuer == "HKG"
qui replace rank = 25 if Issuer == "IRL"
qui replace rank = 26 if Issuer == "JEY"
qui replace rank = 27 if Issuer == "LUX"
qui replace rank = 28 if Issuer == "NLD"
qui replace rank = 29 if Issuer == "PAN"
qui replace rank = 30 if Issuer == "VGB"
qui replace rank = 31 if Issuer == "USA"
qui drop if missing(rank)
gsort rank
drop rank
keep Issuer Position_Residency Position_Nationality_TH_Only TIC_All
gen Delta_TH_Only = Position_Nationality_TH_Only - Position_Residency
qui gen _ = .
qui gen __ = .
qui replace TIC_All = TIC_All / 1e3
order Issuer _ TIC_All Position_Residency __ Position_Nationality_TH_Only
replace Position_Residency = round(Position_Residency)
replace Position_Nationality_TH_Only = round(Position_Nationality_TH_Only)
replace Delta_TH_Only = round(Delta_TH_Only)

save $cmns1/tables/baseline_restatements_us_e, replace

* ---------------------------------------------------------------------------------------------------
* TIC/CPIS main restatement: EMU, bonds
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "EMU" & Asset_Class_Code == "B", clear
qui gen _Position_Residency = Position_Residency if inlist(Issuer, $eu1) | inlist(Issuer, $eu2) | inlist(Issuer, $eu3)
qui gen _Position_Nationality_TH_Only = Position_Nationality_TH_Only if inlist(Issuer, $eu1) | inlist(Issuer, $eu2) | inlist(Issuer, $eu3)
qui local new = _N + 1
qui set obs `new'
qui qui replace Issuer = "EMU" if _n == _N
qui egen EMU_Position_Residency = total(_Position_Residency)
qui egen EMU_Position_Nationality_TH_Only = total(_Position_Nationality_TH_Only)
qui replace Position_Residency = EMU_Position_Residency if Issuer == "EMU"
qui replace Position_Nationality_TH_Only = EMU_Position_Nationality_TH_Only if Issuer == "EMU"
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 4 if Issuer == "CAN"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 6 if Issuer == "IND"
qui replace rank = 7 if Issuer == "IDN"
qui replace rank = 8 if Issuer == "JPN"
qui replace rank = 9 if Issuer == "MEX"
qui replace rank = 10 if Issuer == "RUS"
qui replace rank = 11 if Issuer == "SAU"
qui replace rank = 12 if Issuer == "ZAF"
qui replace rank = 13 if Issuer == "KOR"
qui replace rank = 14 if Issuer == "TUR"
qui replace rank = 15 if Issuer == "GBR"
qui replace rank = 16 if Issuer == "USA"
qui replace rank = 17 if Issuer == "BMU"
qui replace rank = 18 if Issuer == "CYM"
qui replace rank = 19 if Issuer == "CUW"
qui replace rank = 20 if Issuer == "GGY"
qui replace rank = 21 if Issuer == "HKG"
qui replace rank = 22 if Issuer == "IRL"
qui replace rank = 23 if Issuer == "JEY"
qui replace rank = 24 if Issuer == "LUX"
qui replace rank = 25 if Issuer == "NLD"
qui replace rank = 26 if Issuer == "PAN"
qui replace rank = 27 if Issuer == "VGB"
qui replace rank = 28 if Issuer == "FRA"
qui replace rank = 29 if Issuer == "DEU"
qui replace rank = 30 if Issuer == "ITA"
qui replace rank = 31 if Issuer == "ESP"
qui replace rank = 32 if Issuer == "EMU"
qui drop if missing(rank)
gsort rank
drop rank
keep Issuer Position_Residency Position_Nationality_TH_Only
gen Delta_TH_Only = Position_Nationality_TH_Only - Position_Residency
qui gen _ = .
qui gen __ = .
order Issuer _ Position_Residency __ Position_Nationality_TH_Only
replace Position_Residency = round(Position_Residency)
replace Position_Nationality_TH_Only = round(Position_Nationality_TH_Only)
replace Delta_TH_Only = round(Delta_TH_Only)

save $cmns1/tables/baseline_restatements_emu_b, replace

* ---------------------------------------------------------------------------------------------------
* TIC/CPIS main restatement: EMU, equity
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "EMU" & Asset_Class_Code == "E", clear
qui gen _Position_Residency = Position_Residency if inlist(Issuer, $eu1) | inlist(Issuer, $eu2) | inlist(Issuer, $eu3)
qui gen _Position_Residency_Com = Position_Residency_Com if inlist(Issuer, $eu1) | inlist(Issuer, $eu2) | inlist(Issuer, $eu3)
qui gen _Position_Nationality_TH_Only = Position_Nationality_TH_Only if inlist(Issuer, $eu1) | inlist(Issuer, $eu2) | inlist(Issuer, $eu3)
qui local new = _N + 1
qui set obs `new'
qui replace Issuer = "EMU" if _n == _N
egen EMU_Position_Residency = total(_Position_Residency)
egen EMU_Position_Residency_Com = total(_Position_Residency_Com)
egen EMU_Position_Nationality_TH_Only = total(_Position_Nationality_TH_Only)
qui replace Position_Residency = EMU_Position_Residency if Issuer == "EMU"
qui replace Position_Residency_Com = EMU_Position_Residency_Com if Issuer == "EMU"
qui replace Position_Nationality_TH_Only = EMU_Position_Nationality_TH_Only if Issuer == "EMU"
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 4 if Issuer == "CAN"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 6 if Issuer == "IND"
qui replace rank = 7 if Issuer == "IDN"
qui replace rank = 8 if Issuer == "JPN"
qui replace rank = 9 if Issuer == "MEX"
qui replace rank = 10 if Issuer == "RUS"
qui replace rank = 11 if Issuer == "SAU"
qui replace rank = 12 if Issuer == "ZAF"
qui replace rank = 13 if Issuer == "KOR"
qui replace rank = 14 if Issuer == "TUR"
qui replace rank = 15 if Issuer == "GBR"
qui replace rank = 16 if Issuer == "USA"
qui replace rank = 17 if Issuer == "BMU"
qui replace rank = 18 if Issuer == "CYM"
qui replace rank = 19 if Issuer == "CUW"
qui replace rank = 20 if Issuer == "GGY"
qui replace rank = 21 if Issuer == "HKG"
qui replace rank = 22 if Issuer == "IRL"
qui replace rank = 23 if Issuer == "JEY"
qui replace rank = 24 if Issuer == "LUX"
qui replace rank = 25 if Issuer == "NLD"
qui replace rank = 26 if Issuer == "PAN"
qui replace rank = 27 if Issuer == "VGB"
qui replace rank = 28 if Issuer == "FRA"
qui replace rank = 29 if Issuer == "DEU"
qui replace rank = 30 if Issuer == "ITA"
qui replace rank = 31 if Issuer == "ESP"
qui replace rank = 32 if Issuer == "EMU"
qui drop if missing(rank)
gsort rank
drop rank
keep Issuer Position_Residency Position_Nationality_TH_Only Position_Residency_Com
gen Delta_TH_Only = Position_Nationality_TH_Only - Position_Residency
qui gen _ = .
qui gen __ = .
order Issuer _ Position_Residency Position_Residency_Com __ Position_Nationality_TH_Only
replace Position_Residency = round(Position_Residency)
replace Position_Nationality_TH_Only = round(Position_Nationality_TH_Only)
replace Delta_TH_Only = round(Delta_TH_Only)

save $cmns1/tables/baseline_restatements_emu_e, replace

* ---------------------------------------------------------------------------------------------------
* TIC/CPIS main restatement: UK, bonds
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "GBR" & Asset_Class_Code == "B", clear
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 4 if Issuer == "CAN"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 6 if Issuer == "FRA"
qui replace rank = 7 if Issuer == "DEU"
qui replace rank = 8 if Issuer == "IND"
qui replace rank = 9 if Issuer == "IDN"
qui replace rank = 10 if Issuer == "ITA"
qui replace rank = 11 if Issuer == "JPN"
qui replace rank = 12 if Issuer == "MEX"
qui replace rank = 13 if Issuer == "RUS"
qui replace rank = 14 if Issuer == "SAU"
qui replace rank = 15 if Issuer == "ESP"
qui replace rank = 16 if Issuer == "ZAF"
qui replace rank = 17 if Issuer == "KOR"
qui replace rank = 18 if Issuer == "TUR"
qui replace rank = 19 if Issuer == "USA"
qui replace rank = 20 if Issuer == "BMU"
qui replace rank = 21 if Issuer == "CYM"
qui replace rank = 22 if Issuer == "CUW"
qui replace rank = 23 if Issuer == "GGY"
qui replace rank = 24 if Issuer == "HKG"
qui replace rank = 25 if Issuer == "IRL"
qui replace rank = 26 if Issuer == "JEY"
qui replace rank = 27 if Issuer == "LUX"
qui replace rank = 28 if Issuer == "NLD"
qui replace rank = 29 if Issuer == "PAN"
qui replace rank = 30 if Issuer == "VGB"
qui replace rank = 31 if Issuer == "GBR"
qui drop if missing(rank)
gsort rank
drop rank
keep Issuer Position_Residency Position_Nationality_TH_Only Position_Nationality_Full
gen Delta_TH_Only = Position_Nationality_TH_Only - Position_Residency
gen Delta_Full = Position_Nationality_Full - Position_Residency
qui gen _ = .
qui gen __ = .
qui gen ___ = .
order Issuer _ Position_Residency __ Position_Nationality_TH_Only Delta_TH_Only ___ Position_Nationality_Full Delta_Full
replace Position_Nationality_TH_Only = round(Position_Nationality_TH_Only)
replace Delta_TH_Only = round(Delta_TH_Only)
replace Position_Nationality_Full = round(Position_Nationality_Full)
replace Delta_Full = round(Delta_Full)
replace Position_Residency = round(Position_Residency)

save $cmns1/tables/baseline_restatements_uk_b, replace

* ---------------------------------------------------------------------------------------------------
* TIC/CPIS main restatement: UK, equities
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "GBR" & Asset_Class_Code == "E", clear
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 4 if Issuer == "CAN"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 6 if Issuer == "FRA"
qui replace rank = 7 if Issuer == "DEU"
qui replace rank = 8 if Issuer == "IND"
qui replace rank = 9 if Issuer == "IDN"
qui replace rank = 10 if Issuer == "ITA"
qui replace rank = 11 if Issuer == "JPN"
qui replace rank = 12 if Issuer == "MEX"
qui replace rank = 13 if Issuer == "RUS"
qui replace rank = 14 if Issuer == "SAU"
qui replace rank = 15 if Issuer == "ESP"
qui replace rank = 16 if Issuer == "ZAF"
qui replace rank = 17 if Issuer == "KOR"
qui replace rank = 18 if Issuer == "TUR"
qui replace rank = 19 if Issuer == "USA"
qui replace rank = 20 if Issuer == "BMU"
qui replace rank = 21 if Issuer == "CYM"
qui replace rank = 22 if Issuer == "CUW"
qui replace rank = 23 if Issuer == "GGY"
qui replace rank = 24 if Issuer == "HKG"
qui replace rank = 25 if Issuer == "IRL"
qui replace rank = 26 if Issuer == "JEY"
qui replace rank = 27 if Issuer == "LUX"
qui replace rank = 28 if Issuer == "NLD"
qui replace rank = 29 if Issuer == "PAN"
qui replace rank = 30 if Issuer == "VGB"
qui replace rank = 31 if Issuer == "GBR"
qui drop if missing(rank)
gsort rank
drop rank
keep Issuer Position_Residency Position_Nationality_TH_Only Position_Nationality_Full
gen Delta_TH_Only = Position_Nationality_TH_Only - Position_Residency
gen Delta_Full = Position_Nationality_Full - Position_Residency
qui gen _ = .
qui gen __ = .
qui gen ___ = .
order Issuer _ Position_Residency __ Position_Nationality_TH_Only Delta_TH_Only ___ Position_Nationality_Full Delta_Full
replace Position_Nationality_TH_Only = round(Position_Nationality_TH_Only)
replace Delta_TH_Only = round(Delta_TH_Only)
replace Position_Nationality_Full = round(Position_Nationality_Full)
replace Delta_Full = round(Delta_Full)
replace Position_Residency = round(Position_Residency)

save $cmns1/tables/baseline_restatements_uk_e, replace

* ---------------------------------------------------------------------------------------------------
* TIC/CPIS main restatement: CAN, bonds
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "CAN" & Asset_Class_Code == "B", clear
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 6 if Issuer == "FRA"
qui replace rank = 7 if Issuer == "DEU"
qui replace rank = 8 if Issuer == "IND"
qui replace rank = 9 if Issuer == "IDN"
qui replace rank = 10 if Issuer == "ITA"
qui replace rank = 11 if Issuer == "JPN"
qui replace rank = 12 if Issuer == "MEX"
qui replace rank = 13 if Issuer == "RUS"
qui replace rank = 14 if Issuer == "SAU"
qui replace rank = 15 if Issuer == "ESP"
qui replace rank = 16 if Issuer == "ZAF"
qui replace rank = 17 if Issuer == "KOR"
qui replace rank = 18 if Issuer == "TUR"
qui replace rank = 18.5 if Issuer == "GBR"
qui replace rank = 19 if Issuer == "USA"
qui replace rank = 20 if Issuer == "BMU"
qui replace rank = 21 if Issuer == "CYM"
qui replace rank = 22 if Issuer == "CUW"
qui replace rank = 23 if Issuer == "GGY"
qui replace rank = 24 if Issuer == "HKG"
qui replace rank = 25 if Issuer == "IRL"
qui replace rank = 26 if Issuer == "JEY"
qui replace rank = 27 if Issuer == "LUX"
qui replace rank = 28 if Issuer == "NLD"
qui replace rank = 29 if Issuer == "PAN"
qui replace rank = 30 if Issuer == "VGB"
qui replace rank = 31 if Issuer == "CAN"
qui drop if missing(rank)
gsort rank
drop rank
keep Issuer Position_Residency Position_Nationality_TH_Only Position_Nationality_Full
gen Delta_TH_Only = Position_Nationality_TH_Only - Position_Residency
gen Delta_Full = Position_Nationality_Full - Position_Residency
qui gen _ = .
qui gen __ = .
qui gen ___ = .
order Issuer _ Position_Residency __ Position_Nationality_TH_Only Delta_TH_Only ___ Position_Nationality_Full Delta_Full
replace Position_Nationality_TH_Only = round(Position_Nationality_TH_Only)
replace Delta_TH_Only = round(Delta_TH_Only)
replace Position_Nationality_Full = round(Position_Nationality_Full)
replace Delta_Full = round(Delta_Full)
replace Position_Residency = round(Position_Residency)

save $cmns1/tables/baseline_restatements_can_b, replace

* ---------------------------------------------------------------------------------------------------
* TIC/CPIS main restatement: CAN, equities
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "CAN" & Asset_Class_Code == "E", clear
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 6 if Issuer == "FRA"
qui replace rank = 7 if Issuer == "DEU"
qui replace rank = 8 if Issuer == "IND"
qui replace rank = 9 if Issuer == "IDN"
qui replace rank = 10 if Issuer == "ITA"
qui replace rank = 11 if Issuer == "JPN"
qui replace rank = 12 if Issuer == "MEX"
qui replace rank = 13 if Issuer == "RUS"
qui replace rank = 14 if Issuer == "SAU"
qui replace rank = 15 if Issuer == "ESP"
qui replace rank = 16 if Issuer == "ZAF"
qui replace rank = 17 if Issuer == "KOR"
qui replace rank = 18 if Issuer == "TUR"
qui replace rank = 18.5 if Issuer == "GBR"
qui replace rank = 19 if Issuer == "USA"
qui replace rank = 20 if Issuer == "BMU"
qui replace rank = 21 if Issuer == "CYM"
qui replace rank = 22 if Issuer == "CUW"
qui replace rank = 23 if Issuer == "GGY"
qui replace rank = 24 if Issuer == "HKG"
qui replace rank = 25 if Issuer == "IRL"
qui replace rank = 26 if Issuer == "JEY"
qui replace rank = 27 if Issuer == "LUX"
qui replace rank = 28 if Issuer == "NLD"
qui replace rank = 29 if Issuer == "PAN"
qui replace rank = 30 if Issuer == "VGB"
qui replace rank = 31 if Issuer == "CAN"
qui drop if missing(rank)
gsort rank
drop rank
keep Issuer Position_Residency Position_Nationality_TH_Only Position_Nationality_Full
gen Delta_TH_Only = Position_Nationality_TH_Only - Position_Residency
gen Delta_Full = Position_Nationality_Full - Position_Residency
qui gen _ = .
qui gen __ = .
qui gen ___ = .
order Issuer _ Position_Residency __ Position_Nationality_TH_Only Delta_TH_Only ___ Position_Nationality_Full Delta_Full
replace Position_Nationality_TH_Only = round(Position_Nationality_TH_Only)
replace Delta_TH_Only = round(Delta_TH_Only)
replace Position_Nationality_Full = round(Position_Nationality_Full)
replace Delta_Full = round(Delta_Full)
replace Position_Residency = round(Position_Residency)

save $cmns1/tables/baseline_restatements_can_e, replace

* ---------------------------------------------------------------------------------------------------
* TIC/CPIS main restatement: US, government bonds
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "USA" & Asset_Class_Code == "BG", clear
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 4 if Issuer == "CAN"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 6 if Issuer == "FRA"
qui replace rank = 7 if Issuer == "DEU"
qui replace rank = 8 if Issuer == "IND"
qui replace rank = 9 if Issuer == "IDN"
qui replace rank = 10 if Issuer == "ITA"
qui replace rank = 11 if Issuer == "JPN"
qui replace rank = 12 if Issuer == "MEX"
qui replace rank = 13 if Issuer == "RUS"
qui replace rank = 14 if Issuer == "SAU"
qui replace rank = 15 if Issuer == "ESP"
qui replace rank = 16 if Issuer == "ZAF"
qui replace rank = 17 if Issuer == "KOR"
qui replace rank = 18 if Issuer == "TUR"
qui replace rank = 19 if Issuer == "GBR"
qui replace rank = 20 if Issuer == "BMU"
qui replace rank = 21 if Issuer == "CYM"
qui replace rank = 22 if Issuer == "CUW"
qui replace rank = 23 if Issuer == "GGY"
qui replace rank = 24 if Issuer == "HKG"
qui replace rank = 25 if Issuer == "IRL"
qui replace rank = 26 if Issuer == "JEY"
qui replace rank = 27 if Issuer == "LUX"
qui replace rank = 28 if Issuer == "NLD"
qui replace rank = 29 if Issuer == "PAN"
qui replace rank = 30 if Issuer == "VGB"
qui replace rank = 31 if Issuer == "USA"
qui drop if missing(rank)
gsort rank
drop rank
keep Issuer Position_Residency Position_Nationality_TH_Only Position_Nationality_Full
gen Delta_TH_Only = Position_Nationality_TH_Only - Position_Residency
gen Delta_Full = Position_Nationality_Full - Position_Residency
qui gen _ = .
qui gen __ = .
qui gen ___ = .
order Issuer _ Position_Residency __ Position_Nationality_TH_Only Delta_TH_Only ___ Position_Nationality_Full Delta_Full
replace Position_Nationality_TH_Only = round(Position_Nationality_TH_Only)
replace Delta_TH_Only = round(Delta_TH_Only)
replace Position_Nationality_Full = round(Position_Nationality_Full)
replace Delta_Full = round(Delta_Full)
replace Position_Residency = round(Position_Residency)

save $cmns1/tables/baseline_restatements_us_bg, replace

* ---------------------------------------------------------------------------------------------------
* TIC/CPIS main restatement: US, structured finance
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "USA" & Asset_Class_Code == "BSF", clear
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 4 if Issuer == "CAN"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 6 if Issuer == "FRA"
qui replace rank = 7 if Issuer == "DEU"
qui replace rank = 8 if Issuer == "IND"
qui replace rank = 9 if Issuer == "IDN"
qui replace rank = 10 if Issuer == "ITA"
qui replace rank = 11 if Issuer == "JPN"
qui replace rank = 12 if Issuer == "MEX"
qui replace rank = 13 if Issuer == "RUS"
qui replace rank = 14 if Issuer == "SAU"
qui replace rank = 15 if Issuer == "ESP"
qui replace rank = 16 if Issuer == "ZAF"
qui replace rank = 17 if Issuer == "KOR"
qui replace rank = 18 if Issuer == "TUR"
qui replace rank = 19 if Issuer == "GBR"
qui replace rank = 20 if Issuer == "BMU"
qui replace rank = 21 if Issuer == "CYM"
qui replace rank = 22 if Issuer == "CUW"
qui replace rank = 23 if Issuer == "GGY"
qui replace rank = 24 if Issuer == "HKG"
qui replace rank = 25 if Issuer == "IRL"
qui replace rank = 26 if Issuer == "JEY"
qui replace rank = 27 if Issuer == "LUX"
qui replace rank = 28 if Issuer == "NLD"
qui replace rank = 29 if Issuer == "PAN"
qui replace rank = 30 if Issuer == "VGB"
qui replace rank = 31 if Issuer == "USA"
qui drop if missing(rank)
gsort rank
drop rank
keep Issuer Position_Residency Position_Nationality_TH_Only Position_Nationality_Full
gen Delta_TH_Only = Position_Nationality_TH_Only - Position_Residency
gen Delta_Full = Position_Nationality_Full - Position_Residency
qui gen _ = .
qui gen __ = .
qui gen ___ = .
order Issuer _ Position_Residency __ Position_Nationality_TH_Only Delta_TH_Only ___ Position_Nationality_Full Delta_Full
replace Position_Nationality_TH_Only = round(Position_Nationality_TH_Only)
replace Delta_TH_Only = round(Delta_TH_Only)
replace Position_Nationality_Full = round(Position_Nationality_Full)
replace Delta_Full = round(Delta_Full)
replace Position_Residency = round(Position_Residency)

save $cmns1/tables/baseline_restatements_us_abs, replace

* ---------------------------------------------------------------------------------------------------
* Equity robustness: EMU
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates_funds_robustness.dta if Year == 2017 & Investor == "EMU" & Asset_Class_Code == "E", clear
qui gen _Position_Residency = Position_Residency if inlist(Issuer, $eu1) | inlist(Issuer, $eu2) | inlist(Issuer, $eu3)
qui gen _Position_Residency_Com = Position_Residency_Com if inlist(Issuer, $eu1) | inlist(Issuer, $eu2) | inlist(Issuer, $eu3)
qui gen _Position_Nationality_TH_Only = Position_Nationality_TH_Only if inlist(Issuer, $eu1) | inlist(Issuer, $eu2) | inlist(Issuer, $eu3)
qui local new = _N + 1
qui set obs `new'
qui replace Issuer = "EMU" if _n == _N
egen EMU_Position_Residency = total(_Position_Residency)
egen EMU_Position_Residency_Com = total(_Position_Residency_Com)
egen EMU_Position_Nationality_TH_Only = total(_Position_Nationality_TH_Only)
qui replace Position_Residency = EMU_Position_Residency if Issuer == "EMU"
qui replace Position_Residency_Com = EMU_Position_Residency_Com if Issuer == "EMU"
qui replace Position_Nationality_TH_Only = EMU_Position_Nationality_TH_Only if Issuer == "EMU"
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 4 if Issuer == "CAN"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 6 if Issuer == "IND"
qui replace rank = 7 if Issuer == "IDN"
qui replace rank = 8 if Issuer == "JPN"
qui replace rank = 9 if Issuer == "MEX"
qui replace rank = 10 if Issuer == "RUS"
qui replace rank = 11 if Issuer == "SAU"
qui replace rank = 12 if Issuer == "ZAF"
qui replace rank = 13 if Issuer == "KOR"
qui replace rank = 14 if Issuer == "TUR"
qui replace rank = 15 if Issuer == "GBR"
qui replace rank = 16 if Issuer == "USA"
qui replace rank = 17 if Issuer == "BMU"
qui replace rank = 18 if Issuer == "CYM"
qui replace rank = 19 if Issuer == "CUW"
qui replace rank = 20 if Issuer == "GGY"
qui replace rank = 21 if Issuer == "HKG"
qui replace rank = 22 if Issuer == "IRL"
qui replace rank = 23 if Issuer == "JEY"
qui replace rank = 24 if Issuer == "LUX"
qui replace rank = 25 if Issuer == "NLD"
qui replace rank = 26 if Issuer == "PAN"
qui replace rank = 27 if Issuer == "VGB"
qui replace rank = 28 if Issuer == "FRA"
qui replace rank = 29 if Issuer == "DEU"
qui replace rank = 30 if Issuer == "ITA"
qui replace rank = 31 if Issuer == "ESP"
qui replace rank = 32 if Issuer == "EMU"
qui drop if missing(rank)
gsort rank
drop rank
keep Issuer Position_Residency Position_Nationality_TH_Only
gen Delta_TH_Only = Position_Nationality_TH_Only - Position_Residency
qui gen _ = .
qui gen __ = .
order Issuer _ Position_Residency __ Position_Nationality_TH_Only
replace Position_Nationality_TH_Only = round(Position_Nationality_TH_Only)
replace Delta_TH_Only = round(Delta_TH_Only)
replace Position_Residency = round(Position_Residency)

save $cmns1/tables/restatements_equity_robustness_emu, replace

* ---------------------------------------------------------------------------------------------------
* Equity robustness: UK
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates_funds_robustness.dta if Year == 2017 & Investor == "GBR" & Asset_Class_Code == "E", clear
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 4 if Issuer == "CAN"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 8 if Issuer == "IND"
qui replace rank = 9 if Issuer == "IDN"
qui replace rank = 11 if Issuer == "JPN"
qui replace rank = 12 if Issuer == "MEX"
qui replace rank = 13 if Issuer == "RUS"
qui replace rank = 14 if Issuer == "SAU"
qui replace rank = 16 if Issuer == "ZAF"
qui replace rank = 17 if Issuer == "KOR"
qui replace rank = 18 if Issuer == "TUR"
qui replace rank = 19 if Issuer == "USA"
qui replace rank = 20 if Issuer == "BMU"
qui replace rank = 21 if Issuer == "CYM"
qui replace rank = 22 if Issuer == "CUW"
qui replace rank = 23 if Issuer == "GGY"
qui replace rank = 24 if Issuer == "HKG"
qui replace rank = 25 if Issuer == "IRL"
qui replace rank = 26 if Issuer == "JEY"
qui replace rank = 28 if Issuer == "NLD"
qui replace rank = 29 if Issuer == "PAN"
qui replace rank = 30 if Issuer == "VGB"
qui replace rank = 31 if Issuer == "GBR"
qui drop if missing(rank)
gsort rank
drop rank
keep Issuer Position_Residency Position_Nationality_TH_Only
gen Delta_TH_Only = Position_Nationality_TH_Only - Position_Residency
order Issuer Position_Residency Position_Nationality_TH_Only Delta_TH_Only
replace Position_Nationality_TH_Only = round(Position_Nationality_TH_Only)
replace Delta_TH_Only = round(Delta_TH_Only)
replace Position_Residency = round(Position_Residency)

save $cmns1/tables/restatements_equity_robustness_uk, replace

* ---------------------------------------------------------------------------------------------------
* Equity robustness: CAN
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates_funds_robustness.dta if Year == 2017 & Investor == "CAN" & Asset_Class_Code == "E", clear
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 8 if Issuer == "IND"
qui replace rank = 9 if Issuer == "IDN"
qui replace rank = 11 if Issuer == "JPN"
qui replace rank = 12 if Issuer == "MEX"
qui replace rank = 13 if Issuer == "RUS"
qui replace rank = 14 if Issuer == "SAU"
qui replace rank = 16 if Issuer == "ZAF"
qui replace rank = 17 if Issuer == "KOR"
qui replace rank = 18 if Issuer == "TUR"
qui replace rank = 18.5 if Issuer == "GBR"
qui replace rank = 19 if Issuer == "USA"
qui replace rank = 20 if Issuer == "BMU"
qui replace rank = 21 if Issuer == "CYM"
qui replace rank = 22 if Issuer == "CUW"
qui replace rank = 23 if Issuer == "GGY"
qui replace rank = 24 if Issuer == "HKG"
qui replace rank = 25 if Issuer == "IRL"
qui replace rank = 26 if Issuer == "JEY"
qui replace rank = 27 if Issuer == "LUX"
qui replace rank = 28 if Issuer == "NLD"
qui replace rank = 29 if Issuer == "PAN"
qui replace rank = 30 if Issuer == "VGB"
qui replace rank = 31 if Issuer == "CAN"
qui drop if missing(rank)
gsort rank
drop rank
keep Issuer Position_Residency Position_Nationality_TH_Only
gen Delta_TH_Only = Position_Nationality_TH_Only - Position_Residency
order Issuer Position_Residency Position_Nationality_TH_Only Delta_TH_Only
replace Position_Nationality_TH_Only = round(Position_Nationality_TH_Only)
replace Delta_TH_Only = round(Delta_TH_Only)
replace Position_Residency = round(Position_Residency)

save $cmns1/tables/restatements_equity_robustness_can, replace

* ---------------------------------------------------------------------------------------------------
* Alternative restatements: USA
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "USA" & Asset_Class_Code == "BC", clear
keep Issuer Position_Residency Position_Nationality_Full
rename Position_Residency BC_Residency
rename Position_Nationality_Full BC_Full_Nat
save $scratch/_usa_bc_fullnat, replace

use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "USA" & Asset_Class_Code == "E", clear
keep Issuer Position_Residency Position_Nationality_Full
rename Position_Residency E_Residency
rename Position_Nationality_Full E_Full_Nat
save $scratch/_usa_e_fullnat, replace

use $cmns1/alternative_restatements/sales_restatements_baseline.dta if Year == 2017 & Investor == "USA" & Asset_Class_Code == "BC", clear
keep Issuer Position_Sales
rename Position_Sales BC_Sales
save $scratch/_usa_bc_sales, replace

use $cmns1/alternative_restatements/sales_restatements_baseline.dta if Year == 2017 & Investor == "USA" & Asset_Class_Code == "E", clear
keep Issuer Position_Sales
rename Position_Sales E_Sales
save $scratch/_usa_e_sales, replace

use $cmns1/alternative_restatements/guarantor_restatements.dta if Year == 2017 & Investor == "USA" & Asset_Class_Code == "BC", clear
keep Issuer Position_Guarantor_THO
rename Position_Guarantor_THO BC_Guarantor
save $scratch/_usa_bc_guarantor, replace

use $scratch/_usa_bc_fullnat.dta, clear
qui mmerge Issuer using $scratch/_usa_e_fullnat.dta
qui mmerge Issuer using $scratch/_usa_bc_sales.dta
qui mmerge Issuer using $scratch/_usa_e_sales.dta
qui mmerge Issuer using $scratch/_usa_bc_guarantor.dta
drop _merge
save $scratch/_usa_beyond_th, replace

use $scratch/_usa_beyond_th, clear
order Issuer BC_Residency BC_Full_Nat BC_Sales BC_Guarantor E_Residency E_Full_Nat E_Sales
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 4 if Issuer == "CAN"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 6 if Issuer == "FRA"
qui replace rank = 7 if Issuer == "DEU"
qui replace rank = 8 if Issuer == "IND"
qui replace rank = 9 if Issuer == "IDN"
qui replace rank = 10 if Issuer == "ITA"
qui replace rank = 11 if Issuer == "JPN"
qui replace rank = 12 if Issuer == "MEX"
qui replace rank = 13 if Issuer == "RUS"
qui replace rank = 14 if Issuer == "SAU"
qui replace rank = 15 if Issuer == "ESP"
qui replace rank = 16 if Issuer == "ZAF"
qui replace rank = 17 if Issuer == "KOR"
qui replace rank = 18 if Issuer == "TUR"
qui replace rank = 19 if Issuer == "GBR"
qui replace rank = 20 if Issuer == "BMU"
qui replace rank = 21 if Issuer == "CYM"
qui replace rank = 22 if Issuer == "CUW"
qui replace rank = 23 if Issuer == "GGY"
qui replace rank = 24 if Issuer == "HKG"
qui replace rank = 25 if Issuer == "IRL"
qui replace rank = 26 if Issuer == "JEY"
qui replace rank = 27 if Issuer == "LUX"
qui replace rank = 28 if Issuer == "NLD"
qui replace rank = 29 if Issuer == "PAN"
qui replace rank = 30 if Issuer == "VGB"
qui replace rank = 31 if Issuer == "USA"
qui drop if missing(rank)
gsort rank
drop rank

replace BC_Residency = round(BC_Residency)
replace BC_Full_Nat = round(BC_Full_Nat)
replace BC_Sales = round(BC_Sales)
replace BC_Guarantor = round(BC_Guarantor)
replace E_Residency = round(E_Residency)
replace E_Full_Nat = round(E_Full_Nat)
replace E_Sales = round(E_Sales)

save $cmns1/tables/alternative_restatements_usa, replace

* ---------------------------------------------------------------------------------------------------
* Alternative restatements: EMU
* ---------------------------------------------------------------------------------------------------

use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "EMU" & Asset_Class_Code == "B", clear
keep Issuer Position_Residency Position_Nationality_Full
rename Position_Residency B_Residency
rename Position_Nationality_Full B_Full_Nat
save $scratch/_emu_bc_fullnat, replace

use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "EMU" & Asset_Class_Code == "E", clear
keep Issuer Position_Residency Position_Nationality_Full
rename Position_Residency E_Residency
rename Position_Nationality_Full E_Full_Nat
save $scratch/_emu_e_fullnat, replace

use $cmns1/alternative_restatements/sales_restatements_baseline.dta if Year == 2017 & Investor == "EMU" & Asset_Class_Code == "B", clear
keep Issuer Position_Sales
rename Position_Sales B_Sales
save $scratch/_emu_bc_sales, replace

use $cmns1/alternative_restatements/sales_restatements_baseline.dta if Year == 2017 & Investor == "EMU" & Asset_Class_Code == "E", clear
keep Issuer Position_Sales
rename Position_Sales E_Sales
save $scratch/_emu_e_sales, replace

use $cmns1/alternative_restatements/guarantor_restatements.dta if Year == 2017 & Investor == "EMU" & Asset_Class_Code == "B", clear
keep Issuer Position_Guarantor_THO
rename Position_Guarantor_THO B_Guarantor
save $scratch/_emu_bc_guarantor, replace

use $scratch/_emu_bc_fullnat.dta, clear
qui mmerge Issuer using $scratch/_emu_e_fullnat.dta
qui mmerge Issuer using $scratch/_emu_bc_sales.dta
qui mmerge Issuer using $scratch/_emu_e_sales.dta
qui mmerge Issuer using $scratch/_emu_bc_guarantor.dta
drop _merge
save $scratch/_emu_beyond_th, replace

use $scratch/_emu_beyond_th, clear
order Issuer B_Residency B_Full_Nat B_Sales B_Guarantor E_Residency E_Full_Nat E_Sales
foreach var in B_Residency B_Full_Nat B_Sales B_Guarantor E_Residency E_Full_Nat E_Sales {
    qui gen _`var' = `var' if inlist(Issuer, $eu1) | inlist(Issuer, $eu2) | inlist(Issuer, $eu3)
}
qui local new = _N + 1
qui set obs `new'
qui qui replace Issuer = "EMU" if _n == _N
foreach var in B_Residency B_Full_Nat B_Sales B_Guarantor E_Residency E_Full_Nat E_Sales {
    qui egen EMU_`var' = total(_`var')
    qui replace `var' = EMU_`var' if Issuer == "EMU"
}
drop EMU_* _*
qui gen rank = .
qui replace rank = 1 if Issuer == "ARG"
qui replace rank = 2 if Issuer == "AUS"
qui replace rank = 3 if Issuer == "BRA"
qui replace rank = 4 if Issuer == "CAN"
qui replace rank = 5 if Issuer == "CHN"
qui replace rank = 6 if Issuer == "IND"
qui replace rank = 7 if Issuer == "IDN"
qui replace rank = 8 if Issuer == "JPN"
qui replace rank = 9 if Issuer == "MEX"
qui replace rank = 10 if Issuer == "RUS"
qui replace rank = 11 if Issuer == "SAU"
qui replace rank = 12 if Issuer == "ZAF"
qui replace rank = 13 if Issuer == "KOR"
qui replace rank = 14 if Issuer == "TUR"
qui replace rank = 15 if Issuer == "GBR"
qui replace rank = 16 if Issuer == "USA"
qui replace rank = 17 if Issuer == "BMU"
qui replace rank = 18 if Issuer == "CYM"
qui replace rank = 19 if Issuer == "CUW"
qui replace rank = 20 if Issuer == "GGY"
qui replace rank = 21 if Issuer == "HKG"
qui replace rank = 22 if Issuer == "IRL"
qui replace rank = 23 if Issuer == "JEY"
qui replace rank = 24 if Issuer == "LUX"
qui replace rank = 25 if Issuer == "NLD"
qui replace rank = 26 if Issuer == "PAN"
qui replace rank = 27 if Issuer == "VGB"
qui replace rank = 28 if Issuer == "FRA"
qui replace rank = 29 if Issuer == "DEU"
qui replace rank = 30 if Issuer == "ITA"
qui replace rank = 31 if Issuer == "ESP"
qui replace rank = 32 if Issuer == "EMU"
qui drop if missing(rank)
gsort rank
drop rank

replace B_Residency = round(B_Residency)
replace B_Full_Nat = round(B_Full_Nat)
replace B_Sales = round(B_Sales)
replace B_Guarantor = round(B_Guarantor)
replace E_Residency = round(E_Residency)
replace E_Full_Nat = round(E_Full_Nat)
replace E_Sales = round(E_Sales)

save $cmns1/tables/alternative_restatements_emu, replace

log close
