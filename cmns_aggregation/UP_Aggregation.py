# ---------------------------------------------------------------------------------------------------
# CMNS ultimate parent aggregation algorithm
#
# This file implements the ultimate parent aggregation algorithm of Coppola, Maggiori, Neiman, and 
# Schreger (2021). For a detailed discussion of the aggregation algorithm, please refer to that paper. 
# The algorithm aggregates ultimate parent (UP) and domicile information coming from CUSIP Global 
# Services (CGS), Morningstar, Capital IQ (CIQ), SDC Platinum, Orbis, and Factset. The algorithm 
# ultimately associates the universe of traded equity and debt securities with their issuer’s ultimate 
# parent. See accompanying paper for details (Coppola, Maggiori, Neiman, and Schreger, 2021).
#
# Key output: country_master/cmns_aggregation.dta
#
# Key variables:
#       issuer_number:              The CUSIP6 of each issuer
#       issuer_name:                The name of the issuer in issuer_number
#       residency:                  The country of residency of the issuer in issuer_number 
#                                      as in the CGS masterfile
#       issuer_number_up:           The estimated CUSIP6 of the ultimate parent for 
#                                      the issuer in issuer_number
#       issuer_name_up:             The name of the issuer in cusip6_bg
#       domicile_up:                The best guess of the country of domicile of the issuer 
#                                      in issuer_number_up
#       domicile_up_source:         The source data used for the guess in country_bg
#       issuer_number_up_source:    The source data used for the guess in cusip6_up_bg
# ---------------------------------------------------------------------------------------------------
import pandas as pd
import numpy as np
import logging
import copy
import os
from tqdm import tqdm, tqdm_pandas
from multiprocessing import Pool
from Project_Constants import tax_havens, data_path
from UP_Manual_Corrections import drop_cusip6, outdated_names
from UP_Helper import flatten_parent_child_map
tqdm.pandas()

# ---------------------------------------------------------------------------------------------------
# Algorithm parameters
# ---------------------------------------------------------------------------------------------------

# Aggregation data sources: This is the full list of data sources that this algorithm can use
sources = [
    'ciq', # Capital IQ
    'sdc', # SDC new issues database
    'bvd', # Bureau van Dijk's ORBIS ownership data
    'dlg', # Dealogic new issues database
    'fds', # Factset entity data management database
    'ai'   # CUSIP Global Services associated issuers file
]

# Hardcoded source preference order for CUSIP6 (lower number = higher priority)
# Note that "ai" need not be specified since it is always of last preference
source_preference_order = {'dlg': 1, 'bvd': 2, 'fds': 3, 'ciq': 4, 'sdc': 5}

# Hardcoded source preference order for country (lower number = higher priority)
source_preference_order_country = {'dlg': 1, 'fds': 2, 'sdc':3, 'bvd': 4, 'ciq': 5}

# Switches for the various data sources (these can be turned on and off to exclude particular 
# data sources); please note that the CGS master issuer data is required for the algorithm to 
# run (but the CGS associated issuers file is optional).
use_sdc = 1
use_capital_iq = 1
use_dealogic = 1
use_factset = 1
use_morningstar = 1
use_orbis = 1
use_cgs_associated_issuer_file = 1
use_factset_th_screens = 1

# ---------------------------------------------------------------------------------------------------
# Switch to working directory; ensure folder structure exists; set up logging
# ---------------------------------------------------------------------------------------------------

# Set up working directory
if os.path.isdir(data_path):
    os.chdir(data_path)
else:
    raise Exception("Path {} is not a valid directory".format(data_path))

# Set up directory structure
os.makedirs("temp", exist_ok=True)
os.makedirs("output", exist_ok=True)
os.makedirs("output/cmns_aggregation", exist_ok=True)

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info("Begin CMNS aggregation")

# ---------------------------------------------------------------------------------------------------
# Load the data sources used for aggregation
# ---------------------------------------------------------------------------------------------------

# Load CGS issuer master file (this is required); the file neds to have the following columns:
#   - issuer_number: The CUSIP6 of the immediate issuer
#   - issuer_name: The name of the immediate issuer
#   - residency: The place of residency of the immediate issuer (ISO3 code)
cgs = pd.read_stata("aggregation_sources/cgs_compact_complete.dta")

# Load CGS associated issuers file; the file needs to have the following columns:
#   - cusip6: The CUSIP6 of the immediate issuer
#   - ai_cusip6: The CUSIP6 of the associated issuer
#   - country_ai: The place of residency of the associated issuer (ISO3 code)
if use_cgs_associated_issuer_file:
    cgs_ai_aggregation = pd.read_stata("aggregation_sources/cgs_ai_aggregation.dta").rename(
        columns={'ai_residency': 'country_ai'})
else:
    cgs_ai_aggregation = pd.DataFrame({
        'ai_cusip6': [],
        'cusip6': [],
        'country_ai': []
    })
# Load Capital IQ data; the main file needs to have the following columns:
#   - cusip6: The CUSIP6 of the immediate issuer
#   - up_cusip6: The CUSIP6 of the ultimate parent
#   - country_ciq: The place of domicile of the ultimate parent (ISO3 code)
#
# An additional file with issuer names needs to have the following columns:
#   - cusip6: The CUSIP6 of the issuer
#   - issuer_name: The name of the issuer in Capital IQ
if use_capital_iq:
    ciq_aggregation = pd.read_stata("aggregation_sources/ciq_aggregation.dta")
    ciq_names = pd.read_stata("aggregation_sources/ciq_names.dta")
else:
    ciq_aggregation = pd.DataFrame({
        'cusip6': [],
        'up_cusip6': [],
        'country_ciq': []
    })
    ciq_names = pd.DataFrame({
        'cusip6': [],
        'issuer_name': []
    })

# Load Dealogic data; the file needs to have the following columns:
#   - cusip6: The CUSIP6 of the immediate issuer
#   - issuer_name: The name associated with the immediate issuer
#   - up_cusip6: The CUSIP6 of the ultimate parent
#   - up_issuer_name: The name of the ultimate parent
#   - country_dlg: The place of domicile of the ultimate parent (ISO3 code)
if use_dealogic:
    dealogic_aggregation = pd.read_stata("aggregation_sources/dealogic_aggregation.dta")
else:
    dealogic_aggregation = pd.DataFrame({
        'cusip6': [],
        'issuer_name': [],
        'up_cusip6': [],
        'up_issuer_name': [],
        'country_dlg': []
    })

# Load Morningstar data; the file needs to have the following columns:
#   - cusip6: The CUSIP6 of the issuer
#   - country_ms: The place of domicile of the issuer, imputed from Morningstar fund reports (ISO3 code)
if use_morningstar:
    morningstar_country = pd.read_stata("aggregation_sources/morningstar_country.dta")
else:
    morningstar_country = pd.DataFrame({
        'cusip6': [],
        'country_ms': []
    })

# Load SDC data; the main file needs to have the following columns:
#   - cusip6: The CUSIP6 of the immediate issuer
#   - up_cusip6: The CUSIP6 of the ultimate parent
#   - use_cusip: A flag (0/1) reflecting our determination as to whether the field "cusip6" contains a
#                valid CUSIP6 or is simply an internal SDC identifier (which we would still use for 
#                within-SDC chain resolution, but not for external data linkage.
#   - use_up_cusip: Same as above, for the field "up_cusip6"
#
# An additional file with the country codes associated with each issuer in SDC should have 
# the following columns:
#   - cusip6: The CUSIP6 of the issuer
#   - country_sdc: The place of domicile of the issuer (ISO3 code)
#
# An additional file with issuer names in SDC sholuld have the following columns:
#   - cusip6: The CUSIP6 of the issuer
#   - issuer_name: The name of the issuer
if use_sdc:
    sdc_aggregation = pd.read_stata("aggregation_sources/sdc_aggregation.dta")
    sdc_country = pd.read_stata("aggregation_sources/sdc_country.dta")
    sdc_names = pd.read_stata("aggregation_sources/sdc_names.dta")
else:
    sdc_aggregation = pd.DataFrame({
        'cusip6': [],
        'up_cusip6': [],
        'use_cusip': [],
        'use_up_cusip': []
    })
    sdc_country = pd.DataFrame({
        'cusip6': [],
        'country_sdc': []
    })
    sdc_names = pd.DataFrame({
        'cusip6': [],
        'issuer_name': []
    })
    
# Load Factset; the file needs to have the following columns:
#   - cusip6: The CUSIP6 of the immediate issuer
#   - up_cusip6: The CUSIP6 of the ultimate parent
#   - country_fds: The place of domicile of the ultimate parent (ISO3 code)
if use_factset:
    factset_aggregation = pd.read_stata("aggregation_sources/factset_aggregation_v2.dta")
else:
    factset_aggregation = pd.DataFrame({
        'cusip6': [],
        'up_cusip6': [],
        'country_fds': []
    })

# Load Factset HKG/LUX screens; the files need to have the following column:
#   - cusip6: The CUSIP6 of the company that is confirmed to be domiciled in HKG or LUX
if use_factset_th_screens:
    factset_hkg_companies = pd.read_stata("aggregation_sources/factset_hkg_companies.dta")
    factset_lux_companies = pd.read_stata("aggregation_sources/factset_lux_companies.dta")
else:
    factset_hkg_companies = pd.DataFrame({'cusip6': []})
    factset_lux_companies = pd.DataFrame({'cusip6': []})
    
# Load Orbis; the file needs to have the following columns:
#   - cusip6: The CUSIP6 of the immediate issuer
#   - up_cusip6: The CUSIP6 of the ultimate parent
#   - country_bvd: The place of residency of the ultimate parent (ISO3 country code)
if use_orbis:
    orbis_aggregation = pd.read_stata("aggregation_sources/orbis_aggregation.dta")
else:
    orbis_aggregation = pd.DataFrame({
        'cusip6': [],
        'up_cusip6': [],
        'country_bvd': []
    })

# ---------------------------------------------------------------------------------------------------
# Pre-process the data sources used for aggregation
# ---------------------------------------------------------------------------------------------------

# Standardize column names for AI file
cgs_ai_aggregation = cgs_ai_aggregation.rename(columns={'ai_cusip6': 'up_cusip6'})
orbis_aggregation = orbis_aggregation.rename(columns={'guo50_iso_country_code': 'country_bvd'})
ciq_aggregation = ciq_aggregation.rename(columns={'ciqup_cusip6': 'up_cusip6'})
dealogic_aggregation = dealogic_aggregation.rename(columns={'CUSIP': 'cusip6', 'p_CUSIP': 'up_cusip6'})
orbis_aggregation = orbis_aggregation.rename(columns={'bvdid_cusip6': 'cusip6', 'guo50_cusip6': 'up_cusip6'})
factset_aggregation = factset_aggregation.rename(columns={'cusip6_bg': 'up_cusip6'})
cgs_ai_aggregation = cgs_ai_aggregation.rename(columns={'issuer_number': 'cusip6', 'ai_parent_issuer_num': 'up_cusip6'})
sdc_country = sdc_country.rename(columns={'iso_country_code': 'country_sdc'})
ciq_aggregation = ciq_aggregation.rename(columns={'ciq_country_bg': 'country_ciq'})
cgs_ai_aggregation = cgs_ai_aggregation.rename(columns={'ai_parent_domicile': 'country_ai'})
dealogic_aggregation = dealogic_aggregation.rename(columns={'p_nationalityofbusinessisocode': 'country_dlg'})
factset_aggregation = factset_aggregation.rename(columns={'country_bg': 'country_fds'})
morningstar_country = morningstar_country.rename(columns={'iso_country_code': 'country_ms'})
ciq_names = ciq_names.rename(columns={'companyname': 'issuer_name'})
dealogic_aggregation = dealogic_aggregation.rename(columns={'name': 'issuer_name', 'p_name': 'up_issuer_name'})


# Adjust Orbis country codes (Orbis uses II rather than XSN for sovranationals)
orbis_aggregation.country_bvd = orbis_aggregation.country_bvd.replace("II", "XSN")

# Apply corrections
ciq_aggregation = ciq_aggregation[~ciq_aggregation.cusip6.isin(drop_cusip6['ciq'])]
ciq_aggregation = ciq_aggregation[~ciq_aggregation.up_cusip6.isin(drop_cusip6['ciq'])]
dealogic_aggregation = dealogic_aggregation[~dealogic_aggregation.cusip6.isin(drop_cusip6['dlg'])]
dealogic_aggregation = dealogic_aggregation[~dealogic_aggregation.up_cusip6.isin(drop_cusip6['dlg'])]
sdc_aggregation = sdc_aggregation[~sdc_aggregation.cusip6.isin(drop_cusip6['sdc'])]
sdc_aggregation = sdc_aggregation[~sdc_aggregation.up_cusip6.isin(drop_cusip6['sdc'])]
orbis_aggregation = orbis_aggregation[~orbis_aggregation.cusip6.isin(drop_cusip6['bvd'])]
orbis_aggregation = orbis_aggregation[~orbis_aggregation.up_cusip6.isin(drop_cusip6['bvd'])]
factset_aggregation = factset_aggregation[~factset_aggregation.cusip6.isin(drop_cusip6['fds'])]
factset_aggregation = factset_aggregation[~factset_aggregation.up_cusip6.isin(drop_cusip6['fds'])]
cgs_ai_aggregation = cgs_ai_aggregation[~cgs_ai_aggregation.cusip6.isin(drop_cusip6['ai'])]
cgs_ai_aggregation = cgs_ai_aggregation[~cgs_ai_aggregation.up_cusip6.isin(drop_cusip6['ai'])]
morningstar_country = morningstar_country[~morningstar_country.cusip6.isin(drop_cusip6['ms'])]

# ---------------------------------------------------------------------------------------------------
# Perform stationary transformation of all the aggregation sources
# ---------------------------------------------------------------------------------------------------

# Make everything stationary
pool = Pool(processes=6)
sdc_flat = pool.apply_async(flatten_parent_child_map, [sdc_aggregation, "cusip6", "up_cusip6", logger])
ciq_flat = pool.apply_async(flatten_parent_child_map, [ciq_aggregation, "cusip6", "up_cusip6", logger])
orbis_flat = pool.apply_async(flatten_parent_child_map, [orbis_aggregation, "cusip6", "up_cusip6", logger])
cgs_ai_flat = pool.apply_async(flatten_parent_child_map, [cgs_ai_aggregation, "cusip6", "up_cusip6", logger])
dealogic_flat = pool.apply_async(flatten_parent_child_map, [dealogic_aggregation, "cusip6", "up_cusip6", logger])
factset_flat = pool.apply_async(flatten_parent_child_map, [factset_aggregation, "cusip6", "up_cusip6", logger])
pool.close()
pool.join()

# Gather the results
sdc_flat = sdc_flat.get()
ciq_flat = ciq_flat.get()
orbis_flat = orbis_flat.get()
cgs_ai_flat = cgs_ai_flat.get()
dealogic_flat = dealogic_flat.get()
factset_flat = factset_flat.get()

# Special handling of SDC: Prepare quality flags
sdc_quality_flags = pd.concat([
    sdc_aggregation[['cusip6', 'use_cusip']],
    sdc_aggregation[['up_cusip6', 'use_up_cusip']].rename(columns={'up_cusip6': 'cusip6', 'use_up_cusip': 'use_cusip'})
]).groupby("cusip6")['use_cusip'].min().reset_index()

# Merge in SDC quality flags
sdc_clean = sdc_flat.merge(sdc_quality_flags, on="cusip6", how='left')
sdc_clean = sdc_clean.merge(sdc_quality_flags.rename(
    columns={'cusip6': 'up_cusip6', 'use_cusip': 'use_up_cusip'}), 
on="up_cusip6", how='left')
sdc_clean['use_cusip'] = sdc_clean['use_cusip'].fillna(1.)
sdc_clean['use_up_cusip'] = sdc_clean['use_up_cusip'].fillna(1.)

# Alway use entries for TH CUSIPs
sdc_clean = sdc_clean.merge(cgs.rename(columns={'issuer_number': 'cusip6'})[['cusip6', 'domicile']], 
    on="cusip6", how="left").fillna("")
sdc_clean.loc[sdc_clean.domicile.isin(tax_havens), "use_cusip"] = 1. 
sdc_clean.loc[sdc_clean.domicile.isin(tax_havens), "use_up_cusip"] = 1. 

# Merge SDC back with country information
sdc_clean = sdc_clean.merge(
    sdc_country[['cusip6', 'country_sdc']], left_on="up_cusip6", right_on="cusip6", how="left", 
    suffixes=("", "_y")).drop(columns=['cusip6_y']).fillna("")

# Drop anything that has bad-quality flags
sdc_clean.loc[(sdc_clean.use_cusip == 0.) | (sdc_clean.use_up_cusip == 0.), "up_cusip6"] = ""
sdc_clean.loc[(sdc_clean.use_cusip == 0.) | (sdc_clean.use_up_cusip == 0.), "country_sdc"] = ""
sdc = sdc_clean.drop(columns=['use_cusip', 'use_up_cusip', 'domicile'])

# Merge back with country information (SDC was already processed)
ciq = ciq_flat.merge(ciq_aggregation[['up_cusip6', 'country_ciq']].drop_duplicates(), 
    on="up_cusip6", how="left").fillna("")
orbis = orbis_flat.merge(orbis_aggregation[['up_cusip6', 'country_bvd']].drop_duplicates(), 
    on="up_cusip6", how="left").fillna("")
cgs_ai = cgs_ai_flat.merge(cgs_ai_aggregation[['up_cusip6', 'country_ai']].drop_duplicates(), 
    on="up_cusip6", how="left").fillna("")
dealogic = dealogic_flat.merge(dealogic_aggregation[['up_cusip6', 'country_dlg']].drop_duplicates(), 
    on="up_cusip6", how="left").fillna("")
factset = factset_flat.merge(factset_aggregation[['up_cusip6', 'country_fds']].drop_duplicates(), 
    on="up_cusip6", how="left").fillna("")

# ---------------------------------------------------------------------------------------------------
# Merge all data sources in consolidated data structure
# ---------------------------------------------------------------------------------------------------

# Subset and rename columns
cgs = cgs.rename(columns={'issuer_number': 'cusip6'})
cgs = cgs.rename(columns={'domicile': 'residency'})
cgs = cgs[['cusip6', 'residency', 'issuer_name']]
sdc = sdc.rename(columns={'up_cusip6': 'cusip6_up_sdc'})
dealogic = dealogic.rename(columns={'up_cusip6': 'cusip6_up_dlg'})
cgs_ai = cgs_ai.rename(columns={'up_cusip6': 'cusip6_up_ai'})
factset = factset.rename(columns={'up_cusip6': 'cusip6_up_fds'})
orbis = orbis.rename(columns={'up_cusip6': 'cusip6_up_bvd'})
ciq = ciq.rename(columns={'up_cusip6': 'cusip6_up_ciq'})

# Filter out bad CUSIPs
bad_cusips = ["\x1a", "", "#N/A N"]
cgs = cgs[~cgs.cusip6.isin(bad_cusips)]
cgs_ai = cgs_ai[~cgs_ai.cusip6.isin(bad_cusips)]
ciq = ciq[~ciq.cusip6.isin(bad_cusips)]
sdc = sdc[~sdc.cusip6.isin(bad_cusips)]
dealogic[~dealogic.cusip6.isin(bad_cusips)]
factset = factset[~factset.cusip6.isin(bad_cusips)]
morningstar_country = morningstar_country[~morningstar_country.cusip6.isin(bad_cusips)]

# Drop any duplicates
cgs = cgs.drop_duplicates("cusip6")
cgs_ai = cgs_ai.drop_duplicates("cusip6")
ciq = ciq.drop_duplicates("cusip6")
sdc = sdc.drop_duplicates("cusip6")
orbis = orbis.drop_duplicates("cusip6")
dealogic = dealogic.drop_duplicates("cusip6")
factset = factset.drop_duplicates("cusip6")
morningstar_country = morningstar_country.drop_duplicates("cusip6")

# Merge all data
step1 = cgs.merge(ciq, how="outer", on="cusip6").fillna("")
step1 = step1.merge(cgs_ai, how="outer", on="cusip6").fillna("")
step1 = step1.merge(morningstar_country, how="outer", on="cusip6").fillna("")
step1 = step1.merge(sdc, how="outer", on="cusip6").fillna("")
step1 = step1.merge(orbis, how="outer", on="cusip6").fillna("")
step1 = step1.merge(dealogic, how="outer", on="cusip6").fillna("")
step1 = step1.merge(factset, how="outer", on="cusip6").fillna("")
assert step1.cusip6.duplicated().sum() == 0
step1.to_pickle("temp/aggregation_step1.pkl")

# Special handling: we treat pre-specified Hong Kong and Luxembourg companies as if they were not in a tax haven
for column in tqdm(['residency'] + ['cusip6_up_{}'.format(source) for source in sources] + ['country_ms']):
    
    # The special notation "_HKG" is used as a placeholder, and won't be treated as a tax haven location
    for cusip in factset_hkg_companies.cusip6:
        col_value = step1.loc[step1.cusip6 == cusip, column]
        if len(col_value) > 0:
            step1.loc[step1.cusip6 == cusip, column] = col_value.values[0].replace("HKG", "_HKG")
    
    # The special notation "_LUX" is used as a placeholder, and won't be treated as a tax haven location
    for cusip in factset_lux_companies.cusip6:
        col_value = step1.loc[step1.cusip6 == cusip, column]
        if len(col_value) > 0:
            step1.loc[step1.cusip6 == cusip, column] = col_value.values[0].replace("LUX", "_LUX")

# Sanity-check the data
for source in ['ciq', 'sdc', 'bvd', 'dlg', 'fds', 'ai']:
    up_cusip = step1['cusip6_up_{}'.format(source)]
    assert up_cusip[up_cusip != ""].isin(step1.cusip6).all(), "Source {} fails assertion".format(source)

# Empty step
step2 = step1.copy()
step2.to_pickle("temp/aggregation_step2.pkl")

# ---------------------------------------------------------------------------------------------------
# Construct "master country" structure, which lists for each CUSIP6 the country assignments that are 
# reported by each of the sources. Then, harmonize the country assignments across sources.
# ---------------------------------------------------------------------------------------------------

# Helper function to get unique ISO codes for row
def get_unique_iso_with_sources(country_set):
    unique_iso = []
    iso_sources = []
    source_order = ['ciq', 'sdc', 'bvd', 'dlg', 'fds']
    for i in range(len(source_order)):
        if country_set[i] not in unique_iso and country_set[i] != "":
            unique_iso.append(country_set[i])
            iso_sources.append([source_order[j] for j in range(len(source_order)) if country_set[j] == country_set[i]])
    return unique_iso, iso_sources

# Load checkpoint
step2 = pd.read_pickle("temp/aggregation_step2.pkl")

# Gather country details
country_assignments = pd.DataFrame({'cusip6': [x for x in set(step2.cusip6)]})
for source in ['ciq', 'sdc', 'bvd', 'dlg', 'fds', 'ai']:
    country_assignments = country_assignments.merge(
        step2[['cusip6_up_{}'.format(source), 'country_{}'.format(source)]].drop_duplicates(), 
        left_on='cusip6', right_on='cusip6_up_{}'.format(source), how='left').fillna("")
country_assignments = country_assignments.merge(
    step2[['cusip6', 'country_ms', 'residency']].drop_duplicates(), 
    left_on='cusip6', right_on='cusip6', how='left').fillna("")
country_assignments = country_assignments.drop(['cusip6_up_ciq', 'cusip6_up_sdc', 'cusip6_up_bvd', 
    'cusip6_up_dlg', 'cusip6_up_fds', 'cusip6_up_ai'], axis=1).fillna("")

# Compile statistics on country coverage
_country_assignments = country_assignments.progress_apply(lambda x: get_unique_iso_with_sources(
    [x.country_ciq, x.country_sdc, x.country_bvd, x.country_dlg, x.country_fds]), axis=1)
country_assignments['unique_iso'] = [x[0] for x in _country_assignments]
country_assignments['unique_iso_sources'] = [x[1] for x in _country_assignments]
country_assignments['n_unique_iso'] = country_assignments.apply(lambda x: len(x.unique_iso), axis=1)

# Function for country conflict resolution
def resolve_country_conflicts(row):
    """Helper function for country conflict resolution among BVD, SDC, CIQ, BVD, FDS

    Parameters:
        row: The input dataframe row
        tax_havens: List of tax haven countries

    Returns: 
        (str: country_code, str: source)
    """

    # Helper function
    flatten = lambda l: [item for sublist in l for item in sublist]

    # Special case: if the ISO set is [USA, IRL], [EUR, IRL], [USA, IRL, CHE], [EUR, IRL, CHE], which are the
    # countries commonly involved in tax inversions, and Morningstar non-missing and says USA/EUR, then we use the
    # Morningstar code. This is because the Morningstar human reports appear better able in these
    # cases to look through the tax inversions
    if ((row.country_ms == "USA" and set(row.unique_iso + [row.country_ms]) == {'IRL', 'USA'}) or
        (row.country_ms == "USA" and set(row.unique_iso + [row.country_ms]) == {'CHE', 'IRL', 'USA'}) or
        (row.country_ms == "EUR" and set(row.unique_iso + [row.country_ms]) == {'IRL', 'EUR'}) or
        (row.country_ms == "EUR" and set(row.unique_iso + [row.country_ms]) == {'CHE', 'IRL', 'EUR'})
       ):
        return (row.country_ms, "ms")
 
    # Special case: if anything says XSN (i.e. sovranational), we return XSN (since we want to avoid assigning 
    # sovranational entities to any particular territory)
    if "XSN" in [row.country_ms, row.residency] + [row['country_{}'.format(source)] for source in sources]:
        return ("XSN", "xsn")

    # Case (a): There are three or more sources that agree on a non-TH country code
    for i, iso_code in enumerate(row.unique_iso):
        iso_sources = row.unique_iso_sources[i]
        if iso_code not in tax_havens and len(iso_sources) >= 3:
            return (iso_code, " ".join(set(iso_sources)))

    # Find non-TH codes with corresponding sources and priority ranks
    non_th_codes = []
    for i, iso_code in enumerate(row.unique_iso):
        iso_sources = row.unique_iso_sources[i]
        source_ranks = [source_preference_order_country[source] for source in iso_sources]
        source_min_rank = min(source_ranks)
        if iso_code not in tax_havens and len(iso_sources) >= 1:
            non_th_codes.append([iso_code, iso_sources, source_min_rank])
    n_non_th_codes = len(non_th_codes)
    
    # Case (b): Two out of the ownership data sources agree on a non-TH parent, with no competing majority
    if n_non_th_codes >= 1:
        n_non_th_supporting_sources = [len(x[1]) for x in non_th_codes]
        max_supporting_sources = max(n_non_th_supporting_sources)
        no_competing_majority = sum([x == max_supporting_sources for x in n_non_th_supporting_sources]) == 1
        if no_competing_majority and max_supporting_sources > 1:
            for i in range(len(non_th_codes)):
                iso_code, iso_sources, _ = non_th_codes[i]
                if len(iso_sources) == max_supporting_sources:
                    return (iso_code, " ".join(iso_sources))

    # Case (c): Only one out of the ownership data sources reports a non-TH UP country code
    if n_non_th_codes == 1:
        iso_code, iso_sources, _ = non_th_codes[0]
        return (iso_code, " ".join(set(iso_sources)))
    
    # Case (d.i): Multiple ownership data sources report a non-TH country code and disagree; one agrees with MS
    if n_non_th_codes > 1:
        for i in range(n_non_th_codes):
            iso_code, iso_sources, _ = non_th_codes[0]
            if iso_code == row.country_ms:
                return (iso_code, " ".join(set(iso_sources) | {'ms'}))
    
    # Case (d.ii): Multiple ownership data sources report a non-TH country code and disagree; none agree with MS
    if n_non_th_codes > 1:
        lowest_min_rank = min([x[2] for x in non_th_codes])
        for i in range(n_non_th_codes):
            iso_code, iso_sources, source_min_rank = non_th_codes[i]
            if source_min_rank == lowest_min_rank:
                return (iso_code, " ".join(set(iso_sources)))
    
    # It must then be that there are no non-TH codes reported
    assert n_non_th_codes == 0
    
    # Case (e): If MS reports a non-TH code, use it
    if row.country_ms != "" and row.country_ms not in tax_havens:
        return (row.country_ms, "ms")

    # Case (f): If CGS residency is non-TH, use it
    if row.residency != "" and row.residency not in tax_havens:
        return (row.residency, "residency")

    # Find TH codes with corresponding sources and priority ranks
    th_codes = []
    for i, iso_code in enumerate(row.unique_iso):
        iso_sources = row.unique_iso_sources[i]
        source_ranks = [source_preference_order_country[source] for source in iso_sources]
        source_min_rank = min(source_ranks)
        if iso_code in tax_havens and len(iso_sources) >= 1:
            th_codes.append([iso_code, iso_sources, source_min_rank])
    n_th_codes = len(th_codes)

    # Case (g): At this point use the TH code with the most preferred source
    if n_th_codes >= 1:
        lowest_min_rank = min([x[2] for x in th_codes])
        for i in range(n_th_codes):
            iso_code, iso_sources, source_min_rank = th_codes[i]
            if source_min_rank == lowest_min_rank:
                return (iso_code, " ".join(set(iso_sources)))

    # Case (h): Resort to CGS domicile if present (note that this is always equal to the country
    # field in the AI file)
    if row.residency != "":
        return (row.residency, "cgs_residency")

    # Case (i): There are simply no sources
    return ("", "no_country_sources_present")

# Now resolve any conflicts in country assignments
logger.info("Resolving country conflicts among sources")
resolved_countries = country_assignments.progress_apply(lambda x: resolve_country_conflicts(x), axis=1)  
country_assignments['preferred_country'] = [x[0] for x in resolved_countries]
country_assignments['preferred_country_sources'] = [x[1] for x in resolved_countries]
country_assignments = country_assignments[country_assignments.cusip6 != ""]

# Country master dictionary
country_master = country_assignments.set_index("cusip6").to_dict(orient="index")

# Checkpoint
country_assignments_compact = country_assignments[['cusip6', 'preferred_country']]
step3 = step2.copy()
step3.to_pickle("temp/aggregation_step3.pkl")

# ---------------------------------------------------------------------------------------------------
# Resolving ownership chains across the aggregation sources
# ---------------------------------------------------------------------------------------------------

# Load checkpoint
logger.info("Resolving ownership chains among UP sources")
step3 = pd.read_pickle("temp/aggregation_step3.pkl")

# Function to resolve across-source ownership chains
def resolve_ownership_chains_across_sources(data):
    """ This function resolves across-source ownership chains.

    Parameters:
        data: The input dataframe

    Returns:
        cross_source_resolved_parents: Resulting child-parent mapping
    """

    # Create dictionary structure
    logger.info("Resolving ownership chains across sources: ", ", ".join(sources))
    original_parent_links = data.set_index("cusip6")[
        ['cusip6_up_{}'.format(source) for source in sources]
    ].to_dict(orient='index')

    # This data structure holds the raw observed cross-source child-parent 
    # links, together with their supporting sources
    upward_steps_raw = {}

    # Determine the next steps up the ownership chain (or still ones), starting from each of the observed CUSIP6
    for cusip in data.cusip6:
        paths = {}
        for source in sources:
            parent = original_parent_links[cusip]['cusip6_up_{}'.format(source)]
            include_link = True
            
            # Links from a node to itself: special cases
            if parent == cusip and parent != "":
                
                # Include if at least 3 sources report the self-link
                include_link = False
                self_link_sources = [_source for _source in sources if original_parent_links[cusip]['cusip6_up_{}'.format(_source)] == cusip]
                non_self_link_sources = [_source for _source in sources if original_parent_links[cusip]['cusip6_up_{}'.format(_source)] != cusip 
                                         and original_parent_links[cusip]['cusip6_up_{}'.format(_source)] != ""]
                n_sources = len(self_link_sources)
                include_link = True if n_sources >= 3 else False
                
                # Also include if the self-link is reported by Orbis, and vice-versa 
                # (since we are confident the Orbis self-links are not merely filler)
                include_link = True if 'bvd' in self_link_sources else include_link
                include_link = False if 'bvd' in non_self_link_sources else include_link
                
                # But exclude if we are working with a TH CUSIP
                cusip_country = country_master[cusip]['preferred_country']
                include_link = False if cusip_country in tax_havens else include_link
                
                # Also exclude the self-link if we are pitting it against an AI step, since
                # we always want to aggregate at least to the AI level
                cusip_ai = original_parent_links[cusip]['cusip6_up_ai']
                if cusip_ai != "" and cusip_ai != cusip:
                    include_link = False

            # Append the link
            if include_link and parent != "":
                if parent in paths:
                    paths[parent].append(source)
                else:
                    paths[parent] = [source]
        
        upward_steps_raw[cusip] = [
            {'parent': k, 'supporting_sources': v, 'parent_country': country_master[k]['preferred_country']} 
            for k,v in paths.items() if k != ''
        ]

    # Special source preference ordering dictionary, inclusive of AI
    _source_preference_order = copy.copy(source_preference_order)
    _source_preference_order['ai'] = len(sources)

    # If there are multiple paths (upwards or still), we choose the one that is supported by most
    # sources; in the case of ties, we use our source preference ordering
    path_choices = {}
    bad_cases = []
    for cusip in tqdm(data.cusip6):
        
        # Unpack objects
        paths = upward_steps_raw[cusip]
        n_paths = len(paths)
        
        # These cases are unambiguous
        if n_paths == 0:
            path_choices[cusip] = []
        elif n_paths == 1:
            path_choices[cusip] = paths[0]
        else:
            
            # We find the number of supporting sources for each path
            n_supporting_sources = [len(path['supporting_sources']) for path in paths]
            max_n_supporting = max(n_supporting_sources)
            most_supported_paths = np.argwhere([x == max_n_supporting for x in n_supporting_sources]).T[0]
            
            # These are cases in which there is a single majority
            if len(most_supported_paths) == 1:
                most_supported_path = most_supported_paths[0]
                path_choices[cusip] = paths[most_supported_path]
            
            # In these cases we resort to our preference ordering
            else:
                
                # Find the candidate paths and determine their priority rank
                candidate_paths = [paths[i] for i in most_supported_paths]
                path_ranks = []
                for path in candidate_paths:
                    path_rank = min([_source_preference_order[source] for source in path['supporting_sources']])
                    path_ranks.append(path_rank)
                    
                # Use the path with the best priority rank
                min_path_rank = min(path_ranks)
                for i, path in enumerate(candidate_paths):
                    if path_ranks[i] == min_path_rank:
                        path_choices[cusip] = candidate_paths[i]

    # This data structure holds the full ownership chains, starting from each CUSIP6
    cross_source_chains = {}

    # Now we can populate the full ownership chain using our one-step-ahead paths
    max_chain_length = 100
    for cusip in data.cusip6:
        chain = []
        current_node = cusip
        for i in range(max_chain_length):
            if path_choices[current_node] != []:
                chain.append(path_choices[current_node])
                current_node = path_choices[current_node]['parent']
        cross_source_chains[cusip] = chain
        
    # Now we break any cycles that arise using our preference ordering
    cycle_length_threshold = 50
    chain_lengths = {k: len(v) for k,v in cross_source_chains.items()}
    cycle_cusips = [k for k,v in chain_lengths.items() if v > cycle_length_threshold]

    # This data structure serves to ensure consistency in cycle-breaking
    broken_cycle_cache = {}

    # Break the cycles
    for cusip in cycle_cusips:
        
        # Find the cycling elements 
        chain = cross_source_chains[cusip]
        chain_elements = [x['parent'] for x in chain]
        cycling_elements = [x for x in set(chain_elements) if chain_elements.count(x) > 1]
        non_cycling_elements = [x for x in set(chain_elements) if chain_elements.count(x) == 1]
        non_cycling_subchain = [x for x in chain if x['parent'] in non_cycling_elements]

        # Find the corresponding priority ranks
        cycling_sources = []
        element_ranks = []
        for cycling_element in cycling_elements:
            element_sources = [x['supporting_sources'] for x in chain if x['parent'] == cycling_element][0]
            cycling_sources.append(element_sources)
            element_rank = min([_source_preference_order[source] for source in element_sources])
            if cycling_element in broken_cycle_cache:
                element_rank = -999
            element_ranks.append(element_rank)

        # Use the element with the best priority rank
        min_element_rank = min(element_ranks)
        for i, _ in enumerate(cycling_elements):
            if element_ranks[i] == min_element_rank:
                preferred_element = cycling_elements[i]
                preferred_subchain = [[x for x in chain if x['parent'] == preferred_element][0]]

        # Store in cache
        for element in cycling_elements:
            broken_cycle_cache[element] = preferred_element
        
        # Chain determination
        chosen_chain = non_cycling_subchain + preferred_subchain
        cross_source_chains[cusip] = chosen_chain

    # Reflect the cycle breaking in the path choices
    for cusip in broken_cycle_cache:
        _path_choice = [x for x in upward_steps_raw[cusip] if x['parent'] == broken_cycle_cache[cusip]]
        if len(_path_choice) == 1:
            path_choices[cusip] = _path_choice[0]
        else:
            path_choices[cusip] = {'parent': broken_cycle_cache[cusip], 
                'parent_country': country_master[broken_cycle_cache[cusip]]['preferred_country'],
                'supporting_sources': []}
            
    # Now re-generate all chains using the broken cycles
    for cusip in data.cusip6:
        chain = []
        current_node = cusip
        for i in range(max_chain_length):
            if path_choices[current_node] != []:
                chain.append(path_choices[current_node])
                current_node = path_choices[current_node]['parent']
        cross_source_chains[cusip] = chain

    # Prune the chains until we arrive at a non-TH parent
    for cusip in data.cusip6:
        chain = cross_source_chains[cusip]
        for i in range(len(chain)):
            tail_element = chain[-1]
            if tail_element['parent_country'] in tax_havens:
                chain.pop()
            cross_source_chains[cusip] = chain

    # Get the dataset mapping CUSIPs to their resolved parents, after these
    # cross-source chain resolutions
    cross_source_resolved_parents = {}
    for cusip in data.cusip6:
        chain = cross_source_chains[cusip]
        if len(chain) == 0:
            cross_source_resolved_parents[cusip] = cusip
        else:
            topmost_element = chain[-1]
            cross_source_resolved_parents[cusip] = topmost_element['parent']
    cross_source_resolved_parents = pd.DataFrame({
        'cusip6': list(cross_source_resolved_parents.keys()),
        'cross_source_resolved_parent': list(cross_source_resolved_parents.values())
    })
    
    # Ensure flattening
    tst_df = cross_source_resolved_parents.merge(cross_source_resolved_parents, 
        left_on="cross_source_resolved_parent", right_on="cusip6")
    assert tst_df[tst_df.cross_source_resolved_parent_x != tst_df.cross_source_resolved_parent_y].shape[0] == 0
    
    return cross_source_resolved_parents

# Run the cross-source ownership chain resolution
cross_source_resolved_parents = resolve_ownership_chains_across_sources(step3)

# Now update each source's parent links to reflect the chain resolution; we also use the 
# previously harmonized country codes to update the source country assignments
for source in sources:
    
    # Preparation
    logger.info("Updating parents for source {}".format(source))
    source_up_field = 'cusip6_up_{}'.format(source)
    source_country_field = 'country_{}'.format(source)

    # Update parent CUSIP
    step3 = step3.merge(cross_source_resolved_parents.rename(columns={'cusip6': source_up_field}), 
                        on=source_up_field, how='left').fillna("")
    step3.loc[(step3[source_up_field] != step3.cross_source_resolved_parent) & 
              (step3.cross_source_resolved_parent != ""), source_up_field] = (
               step3.loc[(step3[source_up_field] != step3.cross_source_resolved_parent) & 
              (step3.cross_source_resolved_parent != ""), "cross_source_resolved_parent"])
    
    # Update parent country
    step3 = step3.merge(country_assignments_compact.rename(columns={
                        'cusip6': source_up_field, 'preferred_country': 'preferred_up_country'}), 
                        on=source_up_field, how='left').fillna("")
    step3.loc[(step3[source_country_field] != step3.preferred_up_country) & 
              (step3.preferred_up_country != ""), source_country_field] = (
               step3.loc[(step3[source_country_field] != step3.preferred_up_country) & 
              (step3.preferred_up_country != ""), "preferred_up_country"])
    
    # Clean up
    step3 = step3.drop(columns=['cross_source_resolved_parent', 'preferred_up_country'])

# Checkpoint
step4 = step3.copy()
step4.to_pickle("temp/aggregation_step4.pkl")

# ---------------------------------------------------------------------------------------------------
# Perform the aggregation
# ---------------------------------------------------------------------------------------------------

# Load checkpoint
step4 = pd.read_pickle("temp/aggregation_step4.pkl")

# Helper function to get unique parent CUSIPs for each row
def get_unique_parents_with_country_and_sources(parent_set, country_master):
    
    # Gather the information
    unique_parents = []
    parent_sources = []
    parent_country = []
    source_order = ['ciq', 'sdc', 'bvd', 'dlg', 'fds']
    for i in range(len(source_order)):
        if parent_set[i] not in unique_parents and parent_set[i] != "":
            unique_parents.append(parent_set[i])
            parent_sources.append([source_order[j] for j in range(len(source_order)) if parent_set[j] == parent_set[i]])
            parent_country.append(country_master[parent_set[i]]['preferred_country'])
            
    # Decode the special HKG/LUX notation (so that we can appropriately compute geographic majorities)
    if "HKG" in parent_country and "_HKG" in parent_country:
        parent_country = [x if x != "HKG" else "_HKG" for x in parent_country]
    if "LUX" in parent_country and "_LUX" in parent_country:
        parent_country = [x if x != "HKG" else "_HKG" for x in parent_country]

    return unique_parents, parent_sources, parent_country

# Compile statistics for aggregation
unique_parents_with_country_and_sources = step4.progress_apply(lambda x: get_unique_parents_with_country_and_sources(
    [x.cusip6_up_ciq, x.cusip6_up_sdc, x.cusip6_up_bvd, x.cusip6_up_dlg, x.cusip6_up_fds], country_master), axis=1)
step4['unique_parents'] = [x[0] for x in unique_parents_with_country_and_sources]
step4['unique_parents_sources'] = [x[1] for x in unique_parents_with_country_and_sources]
step4['unique_parents_country'] = [x[2] for x in unique_parents_with_country_and_sources]
step4['n_unique_parents'] = step4.apply(lambda x: len(x.unique_parents), axis=1)

# Main decision function for ultimate-parent aggregation
def determine_ultimate_parent(row):
    """
    This function performs ultimate-parent aggregation via a series of decision rules.
    
    Parameters:
        row: The input dataframe row
        source_preference_order: Arbitrary source preference ordering (dictionary)
        country_master: A dictionary listing each source's country assignment for a given CUSIP6

    Returns:
        (str: up_cusip6_bg, int: case_code, str: up_cusip6_bg_source)
    """

    # Case (a): There are three or more sources that agree on a non-TH parent
    for i, parent in enumerate(row.unique_parents):
        parent_sources = row.unique_parents_sources[i]
        parent_country = row.unique_parents_country[i]
        if parent_country not in tax_havens and parent_country != "" and len(parent_sources) >= 3:
            return (parent, 1, " ".join(parent_sources))

    # Find non-TH parents with corresponding sources and priority ranks
    non_th_parents = []
    for i, parent in enumerate(row.unique_parents):
        parent_sources = row.unique_parents_sources[i]
        parent_country = row.unique_parents_country[i]
        source_ranks = [source_preference_order[source] for source in parent_sources]
        source_min_rank = min(source_ranks)
        if parent_country not in tax_havens and parent_country != "" and len(parent_sources) >= 1:
            non_th_parents.append([parent, parent_sources, parent_country, source_min_rank])
    n_non_th_parents = len(non_th_parents)

    # Case (b): Two out of the ownership data sources agree on a non-TH parent, with no competing majority
    if n_non_th_parents >= 1:
        n_non_th_supporting_sources = [len(x[1]) for x in non_th_parents]
        max_supporting_sources = max(n_non_th_supporting_sources)
        no_competing_majority = sum([x == max_supporting_sources for x in n_non_th_supporting_sources]) == 1
        if no_competing_majority and max_supporting_sources > 1:
            for i in range(len(non_th_parents)):
                parent, parent_sources, parent_country, source_min_rank = non_th_parents[i]
                if len(parent_sources) == max_supporting_sources:
                    return (parent, 2, " ".join(parent_sources))

    # Case (c): Only one out of the ownership data sources reports a non-TH parent
    if n_non_th_parents == 1:
        parent, parent_sources, parent_country, _ = non_th_parents[0]
        return (parent, 3, " ".join(parent_sources))

    # Case (d): Multiple ownership data sources report a non-TH parent and disagree
    if n_non_th_parents > 1:

        # If there is a geographic majority, we only consider sources that constitute that geographic majority
        country_counts = {x: row.unique_parents_country.count(x) for x in set(row.unique_parents_country) if x != "" and x not in tax_havens}
        max_nth_count = max(country_counts.values())
        nth_modes = [x for x in country_counts.keys() if country_counts[x] == max_nth_count]
        if len(nth_modes) == 1:
            geo_mode = nth_modes[0]
            geographic_majority_parents = []
            for i in range(n_non_th_parents):
                _, _, parent_country, _ = non_th_parents[i]
                if parent_country == geo_mode:
                    geographic_majority_parents.append(non_th_parents[i])
        else:
            geographic_majority_parents = non_th_parents

        # Then we use the most preferred source
        lowest_min_rank = min([x[3] for x in geographic_majority_parents])
        for i in range(len(geographic_majority_parents)):
            parent, parent_sources, parent_country, source_min_rank = geographic_majority_parents[i]
            if source_min_rank == lowest_min_rank:
                return (parent, 4, " ".join(parent_sources))
            
    # It must then be that there are no non-TH parents reported
    assert n_non_th_parents == 0, "There are {} non-TH parents".format(n_non_th_parents)

    # Case (e): If the associated issuer is non-TH, we use it
    country_ai = country_master[row.cusip6_up_ai]['preferred_country'] if row.cusip6_up_ai != "" else ""
    if country_ai not in tax_havens and country_ai != "":
        return (row.cusip6_up_ai, 5, "cusip6_up_ai")

    # Case (f): The immediate issuer is non-TH
    country_cgs = country_master[row.cusip6]['preferred_country']
    if country_cgs not in tax_havens and country_cgs != "":
        return (row.cusip6, 6, "immediate_issuer_number")

    # Find remaining parents with corresponding sources and priority ranks
    th_or_blank_parents = []
    for i, parent in enumerate(row.unique_parents):
        parent_sources = row.unique_parents_sources[i]
        parent_country = row.unique_parents_country[i]
        source_ranks = [source_preference_order[source] for source in parent_sources]
        source_min_rank = min(source_ranks)
        if len(parent_sources) >= 1:
            th_or_blank_parents.append([parent, parent_sources, parent_country, source_min_rank])
    n_th_or_blank_parents = len(th_or_blank_parents)

    # Case (g): At this point use the TH (or blank-country) parent with the most preferred source
    if n_th_or_blank_parents >= 1:
        lowest_min_rank = min([x[3] for x in th_or_blank_parents])
        for i in range(n_th_or_blank_parents):
            parent, parent_sources, parent_country, source_min_rank = th_or_blank_parents[i]
            if source_min_rank == lowest_min_rank:
                return (parent, 7, " ".join(parent_sources))

    # Else it must be that we also have no other source
    assert n_th_or_blank_parents == 0, "There are {} TH (or blank-country) parents".format(n_th_or_blank_parents)

    # Case (i): Then we use the associated issuer if available
    if row.cusip6_up_ai != "":
        return (row.cusip6_up_ai, 8, "cusip6_up_ai")

    # Case (j): Else we simply use the immediate issuer
    else:
        return (row.cusip6, 9, "immediate_issuer_number")

# Run the aggregation
logger.info("Running final aggregation")
assignment_results = step4.progress_apply(lambda x: determine_ultimate_parent(x), axis=1)

# Add the output to the dataframe
logger.info("Storing results")
step5 = step4.copy().drop(columns=["country_bg"], errors="ignore")
step5['cusip6_up_bg'] = [x[0] for x in assignment_results]
step5['aggregation_case_code'] = [x[1] for x in assignment_results]
step5['cusip6_up_bg_source'] = [x[2] for x in assignment_results]

# Add in the country information
step5 = step5.merge(
    country_assignments[["cusip6", "preferred_country", "preferred_country_sources"]].rename(
       columns={"cusip6": "cusip6_up_bg", "preferred_country": "country_bg", "preferred_country_sources": "country_bg_source"} 
    ), on="cusip6_up_bg")

# Sanity-check: Never reassign from non-TH to TH
assert step5[(~step5.residency.isin(tax_havens + ["_HKG", "_LUX", "__HKG", "__LUX"]))
             & (step5.residency != "") & (step5.country_bg.isin(tax_havens))].shape[0] == 0

# Checkpoint
step5.to_pickle("temp/aggregation_step5.pkl")

# ---------------------------------------------------------------------------------------------------
# Final adjustments
# ---------------------------------------------------------------------------------------------------

# Process CIQ names
ciq_names = ciq_names[~ciq_names.cusip6.isin(["(Invalid Identifier)", ""])].drop_duplicates(subset="cusip6")
ciq_names = ciq_names[ciq_names.issuer_name != ""]

# Process Dealogic names
dealogic_names = pd.concat([
    dealogic_aggregation[['cusip6', 'issuer_name']],
    dealogic_aggregation[['up_cusip6', 'up_issuer_name']].rename(columns={'up_cusip6': 'cusip6', 'up_issuer_name': 'issuer_name'})        
])
dealogic_names = dealogic_names[dealogic_names.issuer_name != ""].drop_duplicates(subset="cusip6")

# Process CGS names
cgs_names = cgs[['cusip6', 'issuer_name']]
cgs_names = cgs_names[~cgs_names.cusip6.isin(outdated_names['cgs'])]

# Master name dataframe
name_master = pd.concat([cgs_names, ciq_names, dealogic_names, sdc_names], axis=0)[['cusip6', 'issuer_name']]
name_master = name_master[name_master.issuer_name != ""]
name_master = name_master.groupby("cusip6").head(1)
name_master.issuer_name = name_master.issuer_name.str.upper()
name_master = name_master.fillna("")
name_master = name_master[name_master.cusip6 != ""]

# Add company names from CGS master file
step5 = pd.read_pickle("temp/aggregation_step5.pkl")
step6 = step5.merge(name_master.rename(
    columns={'cusip6': 'cusip6_up_bg', 'issuer_name': 'issuer_name_up'}).drop_duplicates(
    subset=["cusip6_up_bg"]), on="cusip6_up_bg", suffixes=("", "_up"), how="left").fillna("")
step6['issuer_name_up'] = step6['issuer_name_up'].fillna("")
step6 = step6.merge(name_master.drop_duplicates(subset=["cusip6"]).rename(columns={'issuer_name': 'issuer_name_extra'}),
                      on="cusip6", suffixes=("", "_up"), how="left").fillna("")
step6.loc[step6.issuer_name == "", "issuer_name"] = step6.loc[step6.issuer_name == "", "issuer_name_extra"]
step6 = step6.drop(columns=["issuer_name_extra"])
step6.issuer_name = step6.issuer_name.str.upper()

# Rename columns
step6 = step6.rename(columns={
    'cusip6': 'issuer_number',
    'residency': 'cgs_domicile'
})

# Decode special HKG, LUX notation
step6 = step6.replace("_HKG", "HKG")
step6 = step6.replace("_LUX", "LUX")

# Ensure all columns are of the appropriate types
compact_cols = ['issuer_number', 'issuer_name', 'cgs_domicile', 'cusip6_up_bg', 'country_bg', 
    'issuer_name_up', 'cusip6_up_bg_source', 'country_bg_source']
    
for col in compact_cols:
    step6[col] = step6[col].astype(str).str.replace("\u039c", "")

# Store final output
step6[compact_cols].to_stata("country_master/cmns_aggregation.dta", write_index=False)
