# --------------------------------------------------------------------------------------------------
# Extract zipped raw CGS data
# --------------------------------------------------------------------------------------------------

raw_data_path="<DATA_PATH>/raw/"
cmns1_data_path="<DATA_PATH>/cmns1/"

mkdir -p "${cmns1_data_path}/temp/cgs_uncompress"
unzip "${raw_data_path}/cgs_master/AIMASTER.PIP.zip" -d "${cmns1_data_path}/temp/cgs_uncompress"
unzip "${raw_data_path}/cgs_master/ALLMASTER_ISSUER.PIP.zip" -d "${cmns1_data_path}/temp/cgs_uncompress"
unzip "${raw_data_path}/cgs_master/CPMASTER_ATTRIBUTE.PIP.zip" -d "${cmns1_data_path}/temp/cgs_uncompress"
unzip "${raw_data_path}/cgs_master/CPMASTER_ISSUE.PIP.zip" -d "${cmns1_data_path}/temp/cgs_uncompress"
unzip "${raw_data_path}/cgs_master/CPMASTER_ISSUER.PIP.zip" -d "${cmns1_data_path}/temp/cgs_uncompress"
unzip "${raw_data_path}/cgs_master/FFAPlusMASTER.PIP.zip" -d "${cmns1_data_path}/temp/cgs_uncompress"
unzip "${raw_data_path}/cgs_master/INCMSTR.PIP.zip" -d "${cmns1_data_path}/temp/cgs_uncompress"
unzip "${raw_data_path}/cgs_master/TBA Master File - Sept 2012 Rev.zip" -d "${cmns1_data_path}/temp/cgs_uncompress"
gunzip -c "${raw_data_path}/cgs_master/ALLMASTER_ISIN.PIP.gz" > "${cmns1_data_path}/temp/cgs_uncompress/ALLMASTER_ISIN.PIP"

mkdir -p "${cmns1_data_path}/temp/cgs_uncompress/delivery_2016"
unzip "${raw_data_path}/cgs_master/previous_versions/delivery_2016/AIMASTER.PIP.zip" -d "${cmns1_data_path}/temp/cgs_uncompress/delivery_2016"
unzip "${raw_data_path}/cgs_master/previous_versions/delivery_2016/ALLMASTER_ISSUER.PIP.zip" -d "${cmns1_data_path}/temp/cgs_uncompress/delivery_2016"
unzip "${raw_data_path}/cgs_master/previous_versions/delivery_2016/FFAPlusMASTER.PIP.zip" -d "${cmns1_data_path}/temp/cgs_uncompress/delivery_2016"
unzip "${raw_data_path}/cgs_master/previous_versions/delivery_2016/INCMSTR.PIP.zip" -d "${cmns1_data_path}/temp/cgs_uncompress/delivery_2016"
gunzip -c "${raw_data_path}/cgs_master/previous_versions/delivery_2016/ALLMASTER_ISIN.PIP.gz" > "${cmns1_data_path}/temp/cgs_uncompress/delivery_2016/ALLMASTER_ISIN.PIP"
