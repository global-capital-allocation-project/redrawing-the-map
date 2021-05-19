* ---------------------------------------------------------------------------------------------------
* Cross_Country_Graphs: Produces the cross-country bar charts (Figures 1 and 5 in the paper), and the
* spurious foreign investments figures (Figure A.7 in the paper)
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Cross_Country_Graphs, replace

* ---------------------------------------------------------------------------------------------------
* Cross-country bar charts
* ---------------------------------------------------------------------------------------------------

* Portfolio shares in BRICS debt, across countries: residency vs. nationality
use $cmns1/holdings_based_restatements/nationality_estimates.dta, clear
replace Asset_Class_Code = "B" if inlist(Asset_Class_Code, "B", "BC", "BG", "BSF")
keep if Year == 2017
gcollapse (sum) Position_Residency Position_Nationality_TH_Only, by(Year Investor Asset_Class_Code Issuer)
keep if Asset_Class_Code == "B"
drop if Investor == Issuer
drop if Investor == "EMU" & inlist(Issuer, $eu1)
drop if Investor == "EMU" & inlist(Issuer, $eu2)
drop if Investor == "EMU" & inlist(Issuer, $eu3)
gen BRICS = 0
replace BRICS = 1 if inlist(Issuer, "BRA", "IND", "CHN", "RUS", "ZAF")
gcollapse (sum) Position_Residency Position_Nationality_TH_Only, by(Investor Asset_Class_Code BRICS)
bys Investor Asset_Class_Code: egen Tot_R = total(Position_Residency)
bys Investor Asset_Class_Code: egen Tot_N = total(Position_Nationality_TH_Only)
gen portfolio_share_R = Position_Residency / Tot_R
gen portfolio_share_N = Position_Nationality_TH_Only / Tot_N
keep if BRICS == 1

graph bar (asis) portfolio_share_N portfolio_share_R, over(Investor) ///
    graphregion(color(white)) nofill xsize(6) b1title("Investing Country") ///
    ytitle("Share of External Bond Portfolio in BRICS" " ") ///
    bar(1, color(blue) fintensity(inten40)) bar(2, color(red) fintensity(inten40)) ///
    ylab(0(.02).1, gmin gmax) legend(label(1 "Nationality") label(2 "Residency"))

graph export $cmns1/graphs/brics_portfolio_shares_across_countries.pdf, as(pdf) replace

* Portfolio shares in Chinese equities, across countries: residency vs. nationality
use $cmns1/holdings_based_restatements/nationality_estimates.dta, clear
keep if Year == 2017
keep if Asset_Class_Code == "E"
drop if Investor == Issuer
drop if Investor == "EMU" & inlist(Issuer, $eu1)
drop if Investor == "EMU" & inlist(Issuer, $eu2)
drop if Investor == "EMU" & inlist(Issuer, $eu3)
gen China = 0
replace China = 1 if Issuer == "CHN"
gcollapse (sum) Position_Residency Position_Nationality_TH_Only, by(Investor Asset_Class_Code China)
bys Investor Asset_Class_Code: egen Tot_R = total(Position_Residency)
bys Investor Asset_Class_Code: egen Tot_N = total(Position_Nationality_TH_Only)
gen portfolio_share_R = Position_Residency / Tot_R
gen portfolio_share_N = Position_Nationality_TH_Only / Tot_N
keep if China == 1

graph bar (asis) portfolio_share_N portfolio_share_R, over(Investor) ///
    graphregion(color(white)) nofill xsize(6) b1title("Investing Country") ///
    ytitle("Share of External Equity Portfolio in China" " ") ///
    bar(1, color(blue) fintensity(inten40)) bar(2, color(red) fintensity(inten40)) ///
    ylab(0(.02).1, gmin gmax) legend(label(1 "Nationality") label(2 "Residency"))

graph export $cmns1/graphs/china_portfolio_shares_across_countries.pdf, as(pdf) replace

* ---------------------------------------------------------------------------------------------------
* Spurious foreign investment
* ---------------------------------------------------------------------------------------------------

* Debt type shares for EMU
use $cmns1/holdings_master/mns_issuer_summary, clear
qui keep if year == 2017
qui keep if DomicileCountryId == "EMU"
qui keep if inlist(cgs_domicile, $tax_haven_1) | inlist(cgs_domicile, $tax_haven_2) | ///
    inlist(cgs_domicile, $tax_haven_3) | inlist(cgs_domicile, $tax_haven_4) | ///
    inlist(cgs_domicile, $tax_haven_5) | inlist(cgs_domicile, $tax_haven_6) | ///
    inlist(cgs_domicile, $tax_haven_7) | inlist(cgs_domicile, $tax_haven_8)
qui drop if inlist(cgs_domicile, "NLD", "IRL", "CYP", "LUX")
collapse (sum) marketvalue_usd, by(asset_class cgs_domicile)
drop if asset_class == "Equity"
bys cgs_domicile: egen totVal = total(marketvalue_usd)
gen class_share = marketvalue_usd / totVal
drop marketvalue_usd totVal
qui replace asset_class = "BC" if asset_class == "Bonds - Corporate"
qui replace asset_class = "BS" if asset_class == "Bonds - Government"
qui replace asset_class = "BSF" if asset_class == "Bonds - Structured Finance"
qui reshape wide class_share, i(cgs_domicile) j(asset_class) string
qui replace class_shareBC = 0 if missing(class_shareBC)
qui replace class_shareBS = 0 if missing(class_shareBS)
qui replace class_shareBSF = 0 if missing(class_shareBSF)
save $cmns1/temp/emu_th_b_shares, replace

* EMU tax haven reallocation shares
use $cmns1/temp/reallocation_matrices, clear
qui keep if Year == 2017
qui keep if Investor == "EMU"
qui replace Issuer_Nationality = "EMU" if inlist(Issuer_Nationality, $eu1) | ///
    inlist(Issuer_Nationality, $eu2) | inlist(Issuer_Nationality, $eu3) 
qui keep if Issuer_Nationality == "EMU"
qui keep if inlist(Issuer_Residency, $tax_haven_1) | inlist(Issuer_Residency, $tax_haven_2) | ///
    inlist(Issuer_Residency, $tax_haven_3) | inlist(Issuer_Residency, $tax_haven_4) | ///
    inlist(Issuer_Residency, $tax_haven_5) | inlist(Issuer_Residency, $tax_haven_6) | ///
    inlist(Issuer_Residency, $tax_haven_7) | inlist(Issuer_Residency, $tax_haven_8)
qui drop if inlist(Issuer_Residency, "NLD", "IRL", "CYP", "LUX")
collapse (sum) Reallocation_Share, by(Asset_Class Issuer_Residency)
qui replace Asset_Class = "BC" if Asset_Class == "Bonds - Corporate"
qui replace Asset_Class = "BS" if Asset_Class == "Bonds - Government"
qui replace Asset_Class = "BSF" if Asset_Class == "Bonds - Structured Finance"
qui replace Asset_Class = "E" if Asset_Class == "Equity"
qui reshape wide Reallocation_Share, i(Issuer_Residency) j(Asset_Class) string
qui replace Reallocation_ShareBC = 1 if missing(Reallocation_ShareBC)
qui replace Reallocation_ShareBS = 1 if missing(Reallocation_ShareBS)
qui replace Reallocation_ShareBSF = 1 if missing(Reallocation_ShareBSF)
qui mmerge Issuer_Residency using $cmns1/temp/emu_th_b_shares, umatch(cgs_domicile) unmatched(m)
drop _merge
gen Reallocation_ShareB = Reallocation_ShareBC * class_shareBC + Reallocation_ShareBS * class_shareBS ///
    + Reallocation_ShareBSF * class_shareBSF
qui keep Issuer_Residency Reallocation_ShareB Reallocation_ShareE
qui reshape long
qui replace Reallocation_Share = 0 if missing(Reallocation_Share)
save $cmns1/temp/emu_th_reallocation_shares, replace

* Spurious foreign investment in the EMU
use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "EMU", clear
qui keep if inlist(Asset_Class_Code, "B", "E")
qui keep if inlist(Issuer, $tax_haven_1) | inlist(Issuer, $tax_haven_2) | ///
    inlist(Issuer, $tax_haven_3) | inlist(Issuer, $tax_haven_4) | ///
    inlist(Issuer, $tax_haven_5) | inlist(Issuer, $tax_haven_6) | ///
    inlist(Issuer, $tax_haven_7) | inlist(Issuer, $tax_haven_8)
qui drop if inlist(Issuer, "NLD", "IRL", "CYP", "LUX")
qui keep Asset_Class_Code Issuer Position_Residency
qui mmerge Asset_Class_Code Issuer using $cmns1/temp/emu_th_reallocation_shares, umatch(Asset_Class Issuer_Residency) unmatched(m)
drop _merge
qui replace Reallocation_Share = 0 if missing(Reallocation_Share)
gen absolute_gap = Position_Residency * Reallocation_Share
collapse (sum) absolute_gap, by(Asset_Class_Code)
rename Asset_Class_Code Asset_Class
rename absolute_gap TH_Domestic_Reallocation
gen Investor = "EMU"
save $cmns1/temp/th_domestic_realloc_EMU, replace

* Get all SFI shares
use $cmns1/holdings_master/TIC-CPIS-Master-Main, clear
keep if inlist(Asset_Class, "Equity Securities", "Debt Securities", "Common Equity")
replace Investor = "EMU" if inlist(Investor, $eu1) | inlist(Investor, $eu2) | inlist(Investor, $eu3)
drop if Investor == Issuer
keep if Year == 2017
collapse (sum) Position, by(Investor Asset_Class)
keep if inlist(Investor, "USA", "EMU", "GBR", "CAN", "AUS", "NZL") | inlist(Investor, "DNK", "NOR", "SWE", "CHE")
drop if Asset_Class == "Equity Securities" & Investor == "USA"
replace Asset_Class = "E" if Asset_Class == "Common Equity"
replace Asset_Class = "E" if Asset_Class == "Equity Securities"
replace Asset_Class = "B" if Asset_Class == "Debt Securities"
save $cmns1/temp/sfi_cpis, replace

use $cmns1/temp/sfi_cpis, clear
qui levelsof Investor, local(investors)
foreach investor of local investors {
    if "`investor'" != "EMU" {
        use $cmns1/holdings_based_restatements/nationality_estimates if Year == 2017 & Investor == "`investor'", clear
        qui replace Asset_Class_Code = "B" if inlist(Asset_Class_Code, "BC", "BG", "BSF")
        qui keep if Issuer == "`investor'"
        qui keep if inlist(Asset_Class_Code, "B", "E")
        gcollapse (sum) Position_Residency Position_Nationality_TH_Only, by(Asset_Class_Code)
        gen TH_Domestic_Reallocation = Position_Nationality_TH_Only - Position_Residency
        qui keep Asset_Class_Code TH_Domestic_Reallocation
        gen Investor = "`investor'"
        rename Asset_Class_Code Asset_Class
        qui save $cmns1/temp/th_domestic_realloc_`investor', replace
    }
}

clear
foreach investor of local investors {
    append using $cmns1/temp/th_domestic_realloc_`investor'
}
save $cmns1/temp/th_domestic_realloc, replace

* Prepare data for SFI plots
use $cmns1/temp/sfi_cpis, clear
qui mmerge Investor Asset_Class using $cmns1/temp/th_domestic_realloc.dta
qui replace Position = Position / 1e3
drop _merge
gen SFI_Share = TH_Domestic_Reallocation / Position
qui drop if Investor == "NZL"
qui separate SFI_Share, by(Investor == "USA")

* SFI plots: bonds
graph bar (asis) SFI_Share0 SFI_Share1 if Asset_Class == "B", over(Investor) ///
    graphregion(color(white)) nofill xsize(6) b1title("Investing Country") ///
    ytitle("Spurious Foreign Investment Share") ///
    bar(1, color(blue) fintensity(inten40)) bar(2, color(blue) fintensity(inten40)) ///
    legend(off) ylab(0(.02).12, gmin gmax)
graph export $cmns1/graphs/spurious_fi_B.pdf, as(pdf) replace

* SFI plots: equities
graph bar (asis) SFI_Share0 SFI_Share1 if Asset_Class == "E", over(Investor) ///
    graphregion(color(white)) nofill xsize(6) b1title("Investing Country") ///
    ytitle("Spurious Foreign Investment Share") ///
    bar(1, color(blue) fintensity(inten40)) bar(2, color(blue) fintensity(inten40)) ///
    legend(off) ylab(0(.02).12, gmin gmax)
graph export $cmns1/graphs/spurious_fi_E.pdf, as(pdf) replace

log close
