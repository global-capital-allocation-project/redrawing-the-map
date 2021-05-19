* ---------------------------------------------------------------------------------------------------
* Fund_Shares_Estimation: Estimates the share of equity positions in CPIS that are in fund shares vs.
* common (listed) equities
* ---------------------------------------------------------------------------------------------------
cap log close
set more off
log using $cmns1/logs/Fund_Shares_Estimation, replace

* ---------------------------------------------------------------------------------------------------
* Fund shares estimation for Ireland
* ---------------------------------------------------------------------------------------------------

* SHS data for Ireland, common equity
import excel using "$raw/shs/SHS - EMU Holdings of IRL Common Equity.xlsx", clear cellrange(A3) firstrow
drop if _n < 3
rename A period
rename * _*
rename _period period
qui reshape long _, i(period) j(sector) string
rename _ value_eur
split period, parse(Q)
drop period
gen year = period1
destring period1 period2, replace
gen date_q = qofd(date("1-1-" + year, "DMY")) + period2 - 1
format %tq date_q
drop period1 period2 year
gen quarter = quarter(dofq(date_q))
order date_q quarter sector
qui replace sector = "General government" if sector == "Generalgovernment"
qui replace sector = "Households and nonprofits" if sector == "Householdsandnonprofitinstit"
qui replace sector = "Insurance and pensions" if sector == "InsurancecorporationsandPensi"
qui replace sector = "Nonfinancial corporations" if sector == "Nonfinancialcorporations"
qui replace sector = "Not sectorized" if sector == "Notsectorised"
qui replace sector = "Other financial institutions" if sector == "OtherfinancialinstitutionsFi"
qui replace sector = "MFI" if sector == "Monetaryfinancialinstitutions"
qui replace sector = "Total" if sector == "Othersectorsthancentralbank"
rename value common_equity
save $cmns1/temp/fund_shares_corrections/shs_emu_holdings_irl_common_equity, replace

* SHS data for Ireland, fund shares
import excel using "$raw/shs/SHS - EMU Holdings of IRL Fund Shares.xlsx", clear cellrange(A3) firstrow
drop if _n < 3
rename A period
rename * _*
rename _period period
qui reshape long _, i(period) j(sector) string
rename _ value_eur
split period, parse(Q)
drop period
gen year = period1
destring period1 period2, replace
gen date_q = qofd(date("1-1-" + year, "DMY")) + period2 - 1
format %tq date_q
drop period1 period2 year
gen quarter = quarter(dofq(date_q))
order date_q quarter sector
qui replace sector = "General government" if sector == "Generalgovernment"
qui replace sector = "Households and nonprofits" if sector == "Householdsandnonprofitinstit"
qui replace sector = "Insurance and pensions" if sector == "InsurancecorporationsandPensi"
qui replace sector = "Nonfinancial corporations" if sector == "Nonfinancialcorporations"
qui replace sector = "Not sectorized" if sector == "Notsectorised"
qui replace sector = "Other financial institutions" if sector == "OtherfinancialinstitutionsFi"
qui replace sector = "MFI" if sector == "Monetaryfinancialinstitutions"
qui replace sector = "Total" if sector == "Othersectorsthancentralbank"
rename value fund_shares
save $cmns1/temp/fund_shares_corrections/shs_emu_holdings_irl_fund_shares, replace

* SHS data for Ireland: merge and format
use $cmns1/temp/fund_shares_corrections/shs_emu_holdings_irl_common_equity.dta, clear
qui mmerge date_q sector using $cmns1/temp/fund_shares_corrections/shs_emu_holdings_irl_fund_shares.dta
drop _merge
destring common_equity fund_shares, replace
gen year = year(dofq(date_q))
qui keep if quarter == 4
drop quarter date_q
order year
keep if sector == "Total"
qui reshape wide common_equity fund_shares, i(sector) j(year)
forval y = 2007/2012 {
    gen common_equity`y' = (common_equity2014 + common_equity2013) / 2
    gen fund_shares`y' = (fund_shares2014 + fund_shares2013) / 2
}
qui reshape long
gsort -year
gen common_share = common_equity / (common_equity + fund_shares)
save $cmns1/temp/fund_shares_corrections/emu_irl_common_shares, replace

* Summary: common equity positions in IRL
use $cmns1/temp/fund_shares_corrections/emu_irl_common_shares, clear
keep year common_equity
replace common_equity = common_equity * 1.241
rename common_equity shs_common
save $scratch/shs_common, replace

* All known stakes in IRL common equity
use $cmns1/holdings_master/TIC-Disaggregated-Clean-Main.dta, clear
keep if iso == "IRL"
keep year common_equity
gsort -year
rename common_equity tic_common
qui mmerge year using $scratch/shs_common.dta, unmatched(m)
drop _merge
keep if year >= 2007 & year <= 2017
save $scratch/irl_known_common_equity, replace

* All known fund shares stakes
use $cmns1/holdings_master/TIC-Disaggregated-Clean-Main.dta, clear
keep if iso == "IRL"
gsort -year
gen tic_funds = fund_shares + preferred_other_equity
keep year tic_funds
save $scratch/tic_funds, replace

* Summary: fund share positions in IRL
use $cmns1/temp/fund_shares_corrections/emu_irl_common_shares, clear
keep year fund_shares
replace fund_shares = fund_shares * 1.241
rename fund_shares shs_funds
save $scratch/shs_funds, replace

use $scratch/shs_funds.dta, clear
qui mmerge year using $scratch/tic_funds.dta, unmatched(m)
drop _merge
keep if year >= 2007 & year <= 2017
gsort -year
save $scratch/irl_known_funds, replace

* Eurozone CPIS
use $cmns1/holdings_master/CPIS-Clean-Main-Disagg-EMU.dta, clear
keep if issuer == "IRL"
keep if inlist(investor, $eu1) | inlist(investor, $eu2) | inlist(investor, $eu3)
keep if asset_class == "Equity (All)"
gcollapse (sum) position, by(year)
gsort -year
rename position cpis_eurozone
save $scratch/irl_cpis_eurozone, replace

* Implied Irish holdings
use $scratch/irl_known_common_equity.dta, clear
qui mmerge year using $scratch/irl_known_funds.dta, unmatched(m)
qui mmerge year using $scratch/irl_cpis_eurozone.dta, unmatched(m)
drop _merge
gsort -year
gen shs_total = shs_common + shs_funds
gen implied_irl_holdings = shs_total - cpis_eurozone
gen irl_share = implied_irl_holdings / shs_total
save $scratch/implied_irl_holdings, replace

* Common equity outstanding in IRL
use $cmns1/equity_issuance_master/equity_issuance_master.dta, clear
keep if cgs_domicile == "IRL"
keep if year >= 2007 & year <= 2017
gcollapse (sum) marketcap_usd, by(year)
gsort -year
qui replace marketcap_usd = marketcap_usd * 1e6
rename marketcap_usd tot_common_outstanding
save $scratch/irl_common_outstanding, replace

* CPIS positions in IRL
use $cmns1/holdings_master/CPIS-Raw.dta, clear
keep if issuer == "IRL"
keep if asset_class == "Equity (All)"
gcollapse (sum) position, by(year)
gsort -year
rename position cpis_total_common
save $scratch/irl_cpis_total, replace

* Compute common equity shares for IRL
use $scratch/implied_irl_holdings.dta, clear
qui mmerge year using $scratch/irl_common_outstanding.dta, unmatched(m)
qui mmerge year using $scratch/irl_cpis_total.dta, unmatched(m)
drop _merge
gsort -year
gen net_common_left = tot_common_outstanding - tic_common - shs_common * (1 - irl_share)
gen cpis_remaining_portfolio = cpis_total_common - cpis_eurozone - tic_common - tic_funds
gen row_common_share = net_common_left / cpis_remaining_portfolio
gen tic_common_share = tic_common / (tic_common + tic_funds)
keep year tic_common_share row_common_share
qui mmerge year using $cmns1/temp/fund_shares_corrections/emu_irl_common_shares.dta, unmatched(m)
drop _merge
rename common_share emu_common_share
drop sector common_equity fund_shares
gsort -year
save $cmns1/temp/fund_shares_corrections/irl_common_shares, replace

* ---------------------------------------------------------------------------------------------------
* Fund shares estimation for the Cayman Islands
* ---------------------------------------------------------------------------------------------------

* Total common equity outstanding in CYM
use $cmns1/equity_issuance_master/equity_issuance_master.dta, clear
keep if cgs_domicile == "CYM"
keep if year >= 2007 & year <= 2017
gcollapse (sum) marketcap_usd, by(year)
gen tot_common_outstanding = marketcap_usd * 1e6
drop marketcap_usd
gsort -year
save $scratch/cym_tot_outstanding, replace

* TIC stakes
use $cmns1/holdings_master/TIC-Disaggregated-Clean-Main.dta, clear
keep if iso == "CYM"
gsort -year
gen tic_funds = fund_share + preferred_other_equity
keep year common_equity tic_funds
rename common_equity tic_common
save $scratch/cym_tic_stakes, replace

* CPIS total
use $cmns1/holdings_master/CPIS-Raw.dta, clear
keep if issuer == "CYM"
keep if asset_class == "Equity (All)"
keep if year >= 2007 & year <= 2017
gcollapse (sum) position, by(year)
gsort -year
rename position cpis_total
save $scratch/cym_cpis_total, replace

* Naspers stakes
use $cmns1/equity_issuance_master/equity_issuance_master.dta, clear
keep if cusip == "G87572163"
gsort -year
keep if year >= 2007 & year <= 2017
gen naspers_stake = marketcap_usd * .31 * 1e6 if year >= 2009 
replace naspers_stake = 0 if year < 2009
keep year naspers_stake
duplicates drop year, force
save $scratch/cym_naspers_stake, replace

* RoW shares
use $scratch/cym_tic_stakes.dta, clear
qui mmerge year using $scratch/cym_cpis_total.dta
qui mmerge year using $scratch/cym_naspers_stake.dta
qui mmerge year using $scratch/cym_tot_outstanding.dta
drop _merge
drop if year < 2007
gsort -year
gen net_common_left = tot_common_outstanding - tic_common - naspers_stake
gen cpis_remainder = cpis_total - tic_funds - tic_common
gen row_common_share = net_common_left / cpis_remainder
gen us_common_share = tic_common / (tic_common + tic_funds)
keep year row_common_share us_common_share
save $cmns1/temp/fund_shares_corrections/cym_common_shares, replace

* ---------------------------------------------------------------------------------------------------
* Fund shares estimation for the Netherlands
* ---------------------------------------------------------------------------------------------------

* SHS data for NLD - Common equity
import excel using "$raw/shs/SHS - EMU Holdings of NLD Common Equity.xlsx", clear cellrange(A3) firstrow
drop if _n < 3
rename A period
rename * _*
rename _period period
qui reshape long _, i(period) j(sector) string
rename _ value_eur
split period, parse(Q)
drop period
gen year = period1
destring period1 period2, replace
gen date_q = qofd(date("1-1-" + year, "DMY")) + period2 - 1
format %tq date_q
drop period1 period2 year
gen quarter = quarter(dofq(date_q))
order date_q quarter sector
qui replace sector = "General government" if sector == "Generalgovernment"
qui replace sector = "Households and nonprofits" if sector == "Householdsandnonprofitinstit"
qui replace sector = "Insurance and pensions" if sector == "InsurancecorporationsandPensi"
qui replace sector = "Nonfinancial corporations" if sector == "Nonfinancialcorporations"
qui replace sector = "Not sectorized" if sector == "Notsectorised"
qui replace sector = "Other financial institutions" if sector == "OtherfinancialinstitutionsFi"
qui replace sector = "MFI" if sector == "Monetaryfinancialinstitutions"
qui replace sector = "Total" if sector == "Othersectorsthancentralbank"
rename value common_equity
save $cmns1/temp/fund_shares_corrections/shs_emu_holdings_nld_common_equity, replace

* SHS data for NLD - Fund shares
import excel using "$raw/shs/SHS - EMU Holdings of NLD Fund Shares.xlsx", clear cellrange(A3) firstrow
drop if _n < 3
rename A period
rename * _*
rename _period period
qui reshape long _, i(period) j(sector) string
rename _ value_eur
split period, parse(Q)
drop period
gen year = period1
destring period1 period2, replace
gen date_q = qofd(date("1-1-" + year, "DMY")) + period2 - 1
format %tq date_q
drop period1 period2 year
gen quarter = quarter(dofq(date_q))
order date_q quarter sector
qui replace sector = "General government" if sector == "Generalgovernment"
qui replace sector = "Households and nonprofits" if sector == "Householdsandnonprofitinstit"
qui replace sector = "Insurance and pensions" if sector == "InsurancecorporationsandPensi"
qui replace sector = "Nonfinancial corporations" if sector == "Nonfinancialcorporations"
qui replace sector = "Not sectorized" if sector == "Notsectorised"
qui replace sector = "Other financial institutions" if sector == "OtherfinancialinstitutionsFi"
qui replace sector = "MFI" if sector == "Monetaryfinancialinstitutions"
qui replace sector = "Total" if sector == "Othersectorsthancentralbank"
rename value fund_shares
save $cmns1/temp/fund_shares_corrections/shs_emu_holdings_nld_fund_shares, replace

* SHS data for NLD
use $cmns1/temp/fund_shares_corrections/shs_emu_holdings_nld_common_equity.dta, clear
qui mmerge date_q sector using $cmns1/temp/fund_shares_corrections/shs_emu_holdings_nld_fund_shares.dta
drop _merge
destring common_equity fund_shares, replace
gen year = year(dofq(date_q))
qui keep if quarter == 4
drop quarter date_q
order year
keep if sector == "Total"
qui reshape wide common_equity fund_shares, i(sector) j(year)
forval y = 2007/2012 {
    gen common_equity`y' = (common_equity2014 + common_equity2013) / 2
    gen fund_shares`y' = (fund_shares2014 + fund_shares2013) / 2
}
qui reshape long
gen common_share = common_equity / (common_equity + fund_shares)
gsort -year
save $cmns1/temp/fund_shares_corrections/emu_nld_common_shares, replace

* Known common equity stakesin NLD
use $cmns1/temp/fund_shares_corrections/emu_nld_common_shares, clear
keep year common_equity
rename common_equity shs_common
save $scratch/shs_common, replace

* Merge in US positions in NLD
use $cmns1/holdings_master/TIC-Disaggregated-Clean-Main.dta, clear
keep if iso == "NLD"
keep year common_equity
gsort -year
rename common_equity tic_common
qui mmerge year using $scratch/shs_common.dta, unmatched(m)
drop _merge
keep if year >= 2007 & year <= 2017
save $scratch/nld_known_common_equity, replace

* Known fund shares stakes
use $cmns1/holdings_master/TIC-Disaggregated-Clean-Main.dta, clear
keep if iso == "NLD"
gsort -year
gen tic_funds = fund_shares + preferred_other_equity
keep year tic_funds
save $scratch/tic_funds, replace

use $cmns1/temp/fund_shares_corrections/emu_nld_common_shares, clear
keep year fund_shares
rename fund_shares shs_funds
save $scratch/shs_funds, replace

use $scratch/shs_funds.dta, clear
qui mmerge year using $scratch/tic_funds.dta, unmatched(m)
drop _merge
keep if year >= 2007 & year <= 2017
gsort -year
save $scratch/nld_known_funds, replace

* Eurozone CPIS
use $cmns1/holdings_master/CPIS-Clean-Main-Disagg-EMU.dta, clear
keep if issuer == "NLD"
keep if inlist(investor, $eu1) | inlist(investor, $eu2) | inlist(investor, $eu3)
keep if asset_class == "Equity (All)"
gcollapse (sum) position, by(year)
gsort -year
rename position cpis_eurozone
save $scratch/nld_cpis_eurozone, replace

* Implied Irish holdings
use $scratch/nld_known_common_equity.dta, clear
qui mmerge year using $scratch/nld_known_funds.dta, unmatched(m)
qui mmerge year using $scratch/nld_cpis_eurozone.dta, unmatched(m)
drop _merge
gsort -year
gen shs_total = shs_common + shs_funds
gen implied_nld_holdings = shs_total - cpis_eurozone
gen nld_share = implied_nld_holdings / shs_total
save $scratch/implied_nld_holdings, replace

* Common equity outstanding in NLD
use $cmns1/equity_issuance_master/equity_issuance_master.dta, clear
keep if cgs_domicile == "NLD"
keep if year >= 2007 & year <= 2017
gcollapse (sum) marketcap_usd, by(year)
gsort -year
qui replace marketcap_usd = marketcap_usd * 1e6
rename marketcap_usd tot_common_outstanding
save $scratch/nld_common_outstanding, replace

* CPIS positions in NLD
use $cmns1/holdings_master/CPIS-Raw.dta, clear
keep if issuer == "NLD"
keep if asset_class == "Equity (All)"
gcollapse (sum) position, by(year)
gsort -year
rename position cpis_total_common
save $scratch/nld_cpis_total, replace

* Compute common equity shares for NLD
use $scratch/implied_nld_holdings.dta, clear
qui mmerge year using $scratch/nld_common_outstanding.dta, unmatched(m)
qui mmerge year using $scratch/nld_cpis_total.dta, unmatched(m)
drop _merge
gsort -year
gen net_common_left = tot_common_outstanding - tic_common - shs_common * (1 - nld_share)
gen cpis_remaining_portfolio = cpis_total_common - cpis_eurozone - tic_common - tic_funds
gen row_common_share = net_common_left / cpis_remaining_portfolio
gen tic_common_share = tic_common / (tic_common + tic_funds)
keep year tic_common_share row_common_share
qui mmerge year using $cmns1/temp/fund_shares_corrections/emu_nld_common_shares.dta, unmatched(m)
drop _merge
rename common_share emu_common_share
drop sector common_equity fund_shares
gsort -year
save $cmns1/temp/fund_shares_corrections/nld_common_shares, replace

* ---------------------------------------------------------------------------------------------------
* Fund shares estimation for Cyprus
* ---------------------------------------------------------------------------------------------------

* SHS data for CYP - Common equity
import excel using "$raw/shs/SHS - EMU Holdings of CYP Common Equity.xlsx", clear cellrange(A3) firstrow
drop if _n < 3
rename A period
rename * _*
rename _period period
qui reshape long _, i(period) j(sector) string
rename _ value_eur
split period, parse(Q)
drop period
gen year = period1
destring period1 period2, replace
gen date_q = qofd(date("1-1-" + year, "DMY")) + period2 - 1
format %tq date_q
drop period1 period2 year
gen quarter = quarter(dofq(date_q))
order date_q quarter sector
qui replace sector = "General government" if sector == "Generalgovernment"
qui replace sector = "Households and nonprofits" if sector == "Householdsandnonprofitinstit"
qui replace sector = "Insurance and pensions" if sector == "InsurancecorporationsandPensi"
qui replace sector = "Nonfinancial corporations" if sector == "Nonfinancialcorporations"
qui replace sector = "Not sectorized" if sector == "Notsectorised"
qui replace sector = "Other financial institutions" if sector == "OtherfinancialinstitutionsFi"
qui replace sector = "MFI" if sector == "Monetaryfinancialinstitutions"
qui replace sector = "Total" if sector == "Othersectorsthancentralbank"
rename value common_equity
save $cmns1/temp/fund_shares_corrections/shs_emu_holdings_cyp_common_equity, replace

* SHS data for CYP - Fund shares
import excel using "$raw/shs/SHS - EMU Holdings of CYP Fund Shares.xlsx", clear cellrange(A3) firstrow
drop if _n < 3
rename A period
rename * _*
rename _period period
qui reshape long _, i(period) j(sector) string
rename _ value_eur
split period, parse(Q)
drop period
gen year = period1
destring period1 period2, replace
gen date_q = qofd(date("1-1-" + year, "DMY")) + period2 - 1
format %tq date_q
drop period1 period2 year
gen quarter = quarter(dofq(date_q))
order date_q quarter sector
qui replace sector = "General government" if sector == "Generalgovernment"
qui replace sector = "Households and nonprofits" if sector == "Householdsandnonprofitinstit"
qui replace sector = "Insurance and pensions" if sector == "InsurancecorporationsandPensi"
qui replace sector = "Nonfinancial corporations" if sector == "Nonfinancialcorporations"
qui replace sector = "Not sectorized" if sector == "Notsectorised"
qui replace sector = "Other financial institutions" if sector == "OtherfinancialinstitutionsFi"
qui replace sector = "MFI" if sector == "Monetaryfinancialinstitutions"
qui replace sector = "Total" if sector == "Othersectorsthancentralbank"
rename value fund_shares
save $cmns1/temp/fund_shares_corrections/shs_emu_holdings_cyp_fund_shares, replace

* SHS data for CYP
use $cmns1/temp/fund_shares_corrections/shs_emu_holdings_cyp_common_equity.dta, clear
qui mmerge date_q sector using $cmns1/temp/fund_shares_corrections/shs_emu_holdings_cyp_fund_shares.dta
drop _merge
destring common_equity fund_shares, replace
gen year = year(dofq(date_q))
qui keep if quarter == 4
drop quarter date_q
order year
keep if sector == "Total"
qui reshape wide common_equity fund_shares, i(sector) j(year)
forval y = 2007/2012 {
    gen common_equity`y' = (common_equity2014 + common_equity2013) / 2
    gen fund_shares`y' = (fund_shares2014 + fund_shares2013) / 2
}
qui reshape long
gsort -year
gen common_share = common_equity / (common_equity + fund_shares)
save $cmns1/temp/fund_shares_corrections/emu_cyp_common_shares, replace

* Known common equity stakes
use $cmns1/temp/fund_shares_corrections/emu_cyp_common_shares, clear
keep year common_equity
rename common_equity shs_common
save $scratch/shs_common, replace

* Merge in US positions
use $cmns1/holdings_master/TIC-Disaggregated-Clean-Main.dta, clear
keep if iso == "CYP"
keep year common_equity
gsort -year
rename common_equity tic_common
qui mmerge year using $scratch/shs_common.dta, unmatched(m)
drop _merge
keep if year >= 2007 & year <= 2017
save $scratch/cyp_known_common_equity, replace

* Known fund shares stakes
use $cmns1/holdings_master/TIC-Disaggregated-Clean-Main.dta, clear
keep if iso == "CYP"
gsort -year
gen tic_funds = fund_shares + preferred_other_equity
keep year tic_funds
save $scratch/tic_funds, replace

use $cmns1/temp/fund_shares_corrections/emu_cyp_common_shares, clear
keep year fund_shares
rename fund_shares shs_funds
save $scratch/shs_funds, replace

use $scratch/shs_funds.dta, clear
qui mmerge year using $scratch/tic_funds.dta, unmatched(m)
drop _merge
keep if year >= 2007 & year <= 2017
gsort -year
save $scratch/cyp_known_funds, replace

* Eurozone CPIS
use $cmns1/holdings_master/CPIS-Clean-Main-Disagg-EMU.dta, clear
keep if issuer == "CYP"
keep if inlist(investor, $eu1) | inlist(investor, $eu2) | inlist(investor, $eu3)
keep if asset_class == "Equity (All)"
gcollapse (sum) position, by(year)
gsort -year
rename position cpis_eurozone
save $scratch/cyp_cpis_eurozone, replace

* Implied Irish holdings
use $scratch/cyp_known_common_equity.dta, clear
qui mmerge year using $scratch/cyp_known_funds.dta, unmatched(m)
qui mmerge year using $scratch/cyp_cpis_eurozone.dta, unmatched(m)
drop _merge
gsort -year
gen shs_total = shs_common + shs_funds
gen implied_cyp_holdings = shs_total - cpis_eurozone
gen cyp_share = implied_cyp_holdings / shs_total
save $scratch/implied_cyp_holdings, replace

* Common equity outstanding in CYP
use $cmns1/equity_issuance_master/equity_issuance_master.dta, clear
keep if cgs_domicile == "CYP"
keep if year >= 2007 & year <= 2017
gcollapse (sum) marketcap_usd, by(year)
gsort -year
qui replace marketcap_usd = marketcap_usd * 1e6
rename marketcap_usd tot_common_outstanding
save $scratch/cyp_common_outstanding, replace

* CPIS positions in CYP
use $cmns1/holdings_master/CPIS-Raw.dta, clear
keep if issuer == "CYP"
keep if asset_class == "Equity (All)"
gcollapse (sum) position, by(year)
gsort -year
rename position cpis_total_common
save $scratch/cyp_cpis_total, replace

* Compute common equity shares for CYP
use $scratch/implied_cyp_holdings.dta, clear
qui mmerge year using $scratch/cyp_common_outstanding.dta, unmatched(m)
qui mmerge year using $scratch/cyp_cpis_total.dta, unmatched(m)
drop _merge
gsort -year
gen net_common_left = tot_common_outstanding - tic_common - shs_common * (1 - cyp_share)
gen cpis_remaining_portfolio = cpis_total_common - cpis_eurozone - tic_common - tic_funds
gen row_common_share = net_common_left / cpis_remaining_portfolio
gen tic_common_share = tic_common / (tic_common + tic_funds)
keep year tic_common_share row_common_share
qui mmerge year using $cmns1/temp/fund_shares_corrections/emu_cyp_common_shares.dta, unmatched(m)
drop _merge
rename common_share emu_common_share
drop sector common_equity fund_shares
gsort -year
save $cmns1/temp/fund_shares_corrections/cyp_common_shares, replace

* ---------------------------------------------------------------------------------------------------
* Fund shares estimation for other tax havens
* ---------------------------------------------------------------------------------------------------

foreach country in "CUW" "GGY" "HKG" "IMN" "JEY" "PAN" "VGB" "BMU" "BHS" "NLD" {

    di "Processing `country'"
    
    * Total common outstanding
    use $cmns1/equity_issuance_master/equity_issuance_master.dta, clear
    keep if cgs_domicile == "`country'"
    keep if year >= 2007 & year <= 2017
    gcollapse (sum) marketcap_usd, by(year)
    gen tot_common_outstanding = marketcap_usd * 1e6
    drop marketcap_usd
    gsort -year
    save $scratch/`country'_tot_outstanding, replace

    * TIC stakes
    use $cmns1/holdings_master/TIC-Disaggregated-Clean-Main.dta, clear
    keep if iso == "`country'"
    gsort -year
    gen tic_funds = fund_share + preferred_other_equity
    keep year common_equity tic_funds
    rename common_equity tic_common
    save $scratch/`country'_tic_stakes, replace

    * CPIS total
    use $cmns1/holdings_master/CPIS-Raw.dta, clear
    keep if issuer == "`country'"
    keep if asset_class == "Equity (All)"
    keep if year >= 2007 & year <= 2017
    gcollapse (sum) position, by(year)
    gsort -year
    rename position cpis_total
    save $scratch/`country'_cpis_total, replace

    * RoW shares
    use $scratch/`country'_tic_stakes.dta, clear
    qui mmerge year using $scratch/`country'_cpis_total.dta
    qui mmerge year using $scratch/`country'_tot_outstanding.dta
    drop _merge
    drop if year < 2007
    gsort -year
    gen net_common_left = tot_common_outstanding - tic_common
    gen cpis_remainder = cpis_total - tic_funds - tic_common
    gen row_common_share = net_common_left / cpis_remainder
    gen us_common_share = tic_common / (tic_common + tic_funds)
    keep year row_common_share us_common_share
    gen country = "`country'"
    save $cmns1/temp/fund_shares_corrections/`country'_common_shares, replace
    
}

* Append all other estimates
clear
foreach country in "CUW" "GGY" "HKG" "IMN" "JEY" "PAN" "VGB" "BMU" "BHS" "NLD" {
    append using $cmns1/temp/fund_shares_corrections/`country'_common_shares
}
save $cmns1/temp/fund_shares_corrections/other_common_shares, replace

* Fill in estimates where missing
use $cmns1/temp/fund_shares_corrections/other_common_shares, clear
qui reshape wide row_common_share us_common_share, i(year) j(country) string
foreach var of varlist row_common_share* {
    qui replace `var' = 1 if missing(`var')
}
foreach var of varlist us_common_share* {
    qui replace `var' = 1 if missing(`var')
}
qui reshape long
rename country Issuer
rename year Year
save $cmns1/temp/fund_shares_corrections/other_common_shares_fillin, replace

log close
