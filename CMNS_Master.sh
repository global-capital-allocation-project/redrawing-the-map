#!/bin/bash
# ---------------------------------------------------------------------------------------------------
# CMNS_Master.sh: This file runs the entire replication from start to finish
# ---------------------------------------------------------------------------------------------------

echo "Begin CMNS Run ..."

# Error checks
if [[ $# -ne 1 ]]; then
  echo "Illegal number of parameters: input 1 must be username"
  exit
fi

# Define user
U=${1}
echo "User: "${U}

# Code path
cmns_code_path="<CODE_PATH>"

# Export path variables
echo "Code path = "${cmns_code_path}
export cmns_code_path

# --------------------------------------------------
# Ultimate parent aggregation
# --------------------------------------------------

# Set this flag to 1 to run the aggregation, with accompanying data
# sources build: this part of the code takes a long time to run
run_aggregation=1

if [ ${run_aggregation} = 1 ]; then

    # Extract raw CGS data
    echo "Running Unzip_CGS"
    bash "${cmns_code_path}/process_data_sources/unzip_cgs.sh"

    # Process CGS data
    echo "Running Process_CGS"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Process_CGS"

    # Process Morningstar country information
    echo "Running Process_Morningstar_Country"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Process_Morningstar_Country"

    # Process Dealogic data
    echo "Running Process_Dealogic"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Process_Dealogic"

    # Process SDC data
    echo "Running Process_SDC"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Process_SDC"

    # Process Capital IQ data
    echo "Running Process_CIQ"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Process_CIQ"

    # Process Factset data
    echo "Running Process_Factset"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Process_Factset"

    # Process Orbis data
    echo "Running Process_Orbis"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Process_Orbis"

    # Run the CMNS aggregation algorithm
    python "${cmns_code_path}/cmns_aggregation/UP_Aggregation.py"

fi

# --------------------------------------------------
# Insurance data import
# --------------------------------------------------

# Set this flag to 1 to run insurance data import
# This part of the code takes a long time to run
run_insurance_import=1

if [ ${run_further_data_build} = 1 ]; then

    # Import insurance data
    echo "Running Import_Insurance_Data"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Import_Insurance_Data"

fi

# --------------------------------------------------
# Further data source build
# --------------------------------------------------

# Set this flag to 1 to run further data sources build
run_further_data_build=1

if [ ${run_further_data_build} = 1 ]; then

    # Morningstar_Asset_Classes
    echo "Running Morningstar_Asset_Classes"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Morningstar_Asset_Classes"

    # Security_Master
    echo "Running Security_Master"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Security_Master"

    # Build_Exchange_Rates
    echo "Running Build_Exchange_Rates"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Build_Exchange_Rates"
    
    # Build_Dealogic_Master
    echo "Running Build_Dealogic_Master"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Build_Dealogic_Master"

    # Build_Factset_Master
    echo "Running Build_Factset_Master"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Build_Factset_Master"

    # Build_Bond_Issuance
    echo "Running Build_Bond_Issuance"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Build_Bond_Issuance"

    # Build_Equity_Issuance
    echo "Running Build_Equity_Issuance"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Build_Equity_Issuance"

    # Import_TIC
    echo "Running Import_TIC"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Import_TIC"

    # Import_CPIS
    echo "Running Import_CPIS"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Import_CPIS"

    # Process_GeoRev
    echo "Running Process_GeoRev"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Process_GeoRev"

    # Process_Insurance_Data
    echo "Running Process_Insurance_Data"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Process_Insurance_Data"

    # Build_Norway_SWF_Data_1
    echo "Running Build_Norway_SWF_Data_1"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Build_Norway_SWF_Data_1"

    # Build_Norway_SWF_Data_2
    echo "Running Build_Norway_SWF_Data_2"
    python "${cmns_code_path}/main_analysis/Build_Norway_SWF_Data_2.py"

    # Build_Norway_SWF_Data_3
    echo "Running Build_Norway_SWF_Data_3"
    stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Build_Norway_SWF_Data_3"

fi

# -------------------------------
# Analysis
# -------------------------------

# Summary_Data
echo "Running Summary_Data"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Summary_Data"

# Merge_TIC_CPIS
echo "Running Merge_TIC_CPIS"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Merge_TIC_CPIS"

# Reallocation_Matrices
echo "Running Reallocation_Matrices"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Reallocation_Matrices"

# Fund_Shares_Estimation
echo "Running Fund_Shares_Estimation"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Fund_Shares_Estimation"

# Issuance_Distribution_Matrices
echo "Running Issuance_Distribution_Matrices"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Issuance_Distribution_Matrices"

# Restated_TIC_CPIS
echo "Running Restated_TIC_CPIS"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Restated_TIC_CPIS"

# BRICS_Time_Series
echo "Running BRICS_Time_Series"
python "${cmns_code_path}/main_analysis/BRICS_Time_Series.py"

# Network_Charts
echo "Running Network_Charts"
Rscript "${cmns_code_path}/main_analysis/Network_Charts.R"

# China_VIE_NFA
echo "Running China_VIE_NFA"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "China_VIE_NFA"

# Cross_Country_Graphs
echo "Running Cross_Country_Graphs"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Cross_Country_Graphs"

# Sales_Analysis
echo "Running Sales_Analysis"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Sales_Analysis"

# Credit_Parent_Analysis
echo "Running Credit_Parent_Analysis"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Credit_Parent_Analysis"

# Restatement_Tables
echo "Running Restatement_Tables"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Restatement_Tables"

# Firm_Level_Tables
echo "Running Firm_Level_Tables"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Firm_Level_Tables"

# Representativeness_Analysis
echo "Running Representativeness_Analysis"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Representativeness_Analysis"

# Home_Bias
echo "Running Home_Bias"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Home_Bias"

# Currency_Analysis
echo "Running Currency_Analysis"
stata-mp -b "${cmns_code_path}/CMNS_Master.do" ${U} "Currency_Analysis"

# FINISH
echo "Finished"
exit
