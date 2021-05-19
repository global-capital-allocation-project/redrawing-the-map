* --------------------------------------------------------------------------------------------------
* Build_Exchange_Rates: This file uses IMF IFS data, which replaces WMR exchange rate data; it uses
* crosswalks using ISO currency codes and IMF currency codes to create clean IFS_ERdata.dta file
* --------------------------------------------------------------------------------------------------
set more off
cap log close
log using $cmns1/logs/ER_Data_Build, replace

* --------------------------------------------------------------------------------------------------
* Data prep
* --------------------------------------------------------------------------------------------------

* Generate country-currency crosswalk
import excel using $raw/IMF_IFS/ISO_currency.xls, cellrange(A4:C283) firstrow clear
rename (ENTITY AlphabeticCode) (Country iso_currency_code)
drop if missing(iso_currency_code)
replace Country = regexr(Country, "\((.)+\)", "")
replace Country = trim(Country)
replace Country = "DEMOCRATIC REPUBLIC OF THE CONGO" if Country == "CONGO" & Currency == "Congolese Franc"
replace Country = "REPUBLIC OF CONGO" if Country == "CONGO" & Currency == "CFA Franc BEAC"
save $temp/ER_Data/currency_codes, replace

import excel using $raw/IMF_IFS/IMF_codes.xlsx, cellrange(A2:D191) firstrow clear
gen Country_IMF = Country
replace Country = upper(Country)
replace Country = "CZECHIA" if Country == "CZECH REPUBLIC"
replace Country = "CÔTE D'IVOIRE" if Country == "CôTE D'IVOIRE"
replace Country = "NORTH MACEDONIA" if Country == "FYR MACEDONIA"
replace Country = "HONG KONG" if Country == "HONG KONG SAR"
replace Country = "KYRGYZSTAN" if Country == "KYRGYZ REPUBLIC"
replace Country = "LAO PEOPLE’S DEMOCRATIC REPUBLIC" if Country == "LAO P.D.R."
replace Country = "RUSSIAN FEDERATION" if Country == "RUSSIA"
replace Country = "SLOVAKIA" if Country == "SLOVAK REPUBLIC"
replace Country = "SAINT KITTS AND NEVIS" if Country == "ST. KITTS AND NEVIS"
replace Country = "SAINT LUCIA" if Country == "ST. LUCIA"
replace Country = "SAINT VINCENT AND THE GRENADINES" if Country == "ST. VINCENT AND THE GRENADINES"
replace Country = "ESWATINI" if Country == "SWAZILAND"
replace Country = "SYRIAN ARAB REPUBLIC" if Country == "SYRIA"
replace Country = "SAO TOME AND PRINCIPE" if Country == "SãO TOMé AND PRíNCIPE"
replace Country = "TAIWAN" if Country == "TAIWAN PROVINCE OF CHINA"
replace Country = "TANZANIA, UNITED REPUBLIC OF" if Country == "TANZANIA"
replace Country = "BAHAMAS" if Country == "THE BAHAMAS"
replace Country = "GAMBIA" if Country == "THE GAMBIA"
replace Country = "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND" if Country == "UNITED KINGDOM"
replace Country = "UNITED STATES OF AMERICA" if Country == "UNITED STATES"
replace Country = "VIET NAM" if Country == "VIETNAM"
save $temp/ER_Data/IMF_code_clean, replace


use $temp/ER_Data/currency_codes, clear
merge m:1 Country using $temp/ER_Data/IMF_code_clean, nogen keep(2 3)
replace iso_currency_code = "EUR" if Currency == "Euro"
drop if iso_currency_code == "KPW" & Country == "KOREA"
drop if iso_currency_code == "BOV" & Country == "BOLIVIA"
drop if iso_currency_code == "CLF" & Country == "CHILE"
drop if iso_currency_code == "BTN" & Country == "BHUTAN"
drop if iso_currency_code == "COU" & Country == "COLOMBIA"
drop if iso_currency_code == "LSL" & Country == "LESOTHO"
drop if iso_currency_code == "CUC" & Country == "CUBA"
drop if iso_currency_code == "USD" & Country == "EL SALVADOR"
drop if iso_currency_code == "MXV" & Country == "MEXICO"
drop if iso_currency_code == "USD" & Country == "PANAMA"
drop if iso_currency_code == "USD" & Country == "HAITI"
drop if iso_currency_code == "ZAR" & Country == "NAMIBIA"
drop if iso_currency_code == "USN" & Country == "UNITED STATES OF AMERICA"
drop if (iso_currency_code == "CHE" & Country == "SWITZERLAND") | (iso_currency_code == "CHW" & Country == "SWITZERLAND")
drop if (iso_currency_code == "UYI" & Country == "URUGUAY") | (iso_currency_code == "UYW" & Country == "URUGUAY")
drop Country
rename Country_IMF Country
order Country IMFCode , first
destring IMFCode, replace
save $temp/ER_Data/country_currency_crosswalk, replace

import excel using $raw/IMF_IFS/IFS_ERdata.xlsx, sheet("Data") firstrow clear
gen date = monthly(TimePeriod, "YM")
format date %tm
sort CountryName date
keep if IndicatorCode == "ENDE_XDC_USD_RATE"
save $temp/ER_Data/IFS_ERdata_eop_raw.dta, replace

use $temp/ER_Data/IFS_ERdata_eop_raw.dta, clear
keep CountryName CountryCode date Value
rename CountryCode IMFCode
merge m:1 IMFCode using $temp/ER_Data/country_currency_crosswalk, nogen keep(1 3)
drop Country
sort CountryName date
rename (ISOCode date) (iso_country_code date_m)

* For iso_currency_code that has more than one country, keep only one
* either representative / same
* iso_currency_code: AUD, INR, USD, XAF, XCD, XOF, ZAR
* keep: AUD (AUS), INR (IND), USD (USA), XAF (CAF), XCD (DMA), XOF (NER), ZAR (ZAF)

replace Currency = "Euro" if CountryName == "Euro Area"
replace iso_currency_code = "EUR" if CountryName == "Euro Area"
drop if iso_currency_code == "EUR" & CountryName != "Euro Area"
drop if iso_currency_code == "AUD" & iso_country_code != "AUS"
drop if iso_currency_code == "INR" & iso_country_code != "IND"
drop if iso_currency_code == "USD" & iso_country_code != "USA"
drop if iso_currency_code == "XAF" & iso_country_code != "CAF"
drop if iso_currency_code == "XCD" & iso_country_code != "DMA"
drop if iso_currency_code == "XOF" & iso_country_code != "NER"
drop if iso_currency_code == "ZAR" & iso_country_code != "ZAF"
drop if missing(iso_currency_code)

keep iso_currency_code date_m Value
rename Value lcu_per_usd

save $cmns1/exchange_rates/IFS_ERdata.dta, replace
log close
