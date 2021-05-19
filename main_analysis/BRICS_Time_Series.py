# ---------------------------------------------------------------------------------------------------
# BRICS_Time_Series: This job produces a time series showing the importance of BRICS countries in
# tax haven bond issuance over time (Figure 4 in the paper)
# ---------------------------------------------------------------------------------------------------
from pathlib import Path
import numpy as np
import pandas as pd
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from matplotlib.patches import Patch
import matplotlib.dates as mdates

from datetime import datetime

import seaborn as sns
import colorsys

cmns1 = Path("<DATA_PATH>/cmns1/")

plt.rcParams.update({'font.size': 14})

tax_haven_list = [
    "ABW", "AIA", "AND", "ANT", "ATG", "BHR", "BHS", "BLZ", "BMU",
    "BRB", "COK", "CRI", "CUW", "CYM", "CYP", "DJI", "DMA", "FSM",
    "GGY", "GIB", "GRD", "HKG", "IMN", "IRL", "JEY", "JOR", "KNA",
    "LBN", "LBR", "LCA", "LIE", "LUX", "MAC", "MAF", "MCO", "MDV",
    "MHL", "MLT", "MSR", "MUS", "NIU", "NLD", "NRU", "PAN", "SGP",
    "SMR", "SYC", "TCA", "TON", "VCT", "VGB", "VUT", "WSM"
]

bric_list = ['BRA', 'CHN', 'IND', 'RUS', 'ZAF']


# # BRICS Time Series

# holdings
hold_df = pd.read_csv(cmns1 / "holdings_based_restatements/source_destination_data_lower.csv", encoding='cp1252')

# keep 2005-2018
hold_df = hold_df[hold_df.year<2019]
hold_df = hold_df[hold_df.year>2004]

# drop equity
hold_df = hold_df[hold_df['asset_class_code']!='E']
hold_df = hold_df[~((hold_df['investor']=='USA') & (hold_df['asset_class_code']=="B"))]

# drop if cgs_dom is missing, keep is cgs_dom is TH
hold_df = hold_df.loc[~hold_df['residency'].isnull()]
hold_df = hold_df.loc[hold_df['residency'].isin(tax_haven_list)]

# drop if nationality==residency
hold_df = hold_df[hold_df['residency']!=hold_df['nationality']]

# combine all other nationalities
hold_df.loc[~hold_df['nationality'].isin(bric_list), 'nationality'] = 'ROW'

# collapse by nat, res, year then reshape wide to have year observation
hold_df = hold_df.groupby(['nationality','year'])['position_nationality_th_only'].agg(['sum'])
hold_df = hold_df.unstack(level='nationality')

# rename variables to have one level of names
hold_df.columns = ['_'.join(col) for col in hold_df.columns.values]
hold_df.columns = [col.strip() for col in hold_df.columns.values]
hold_df.head(400)

# drop missing, generate total
hold_df = hold_df.dropna(axis=1, how='all')
hold_df['total'] = hold_df.sum(axis = 1, skipna = True) 

# calculate shares
for cou in bric_list:
    hold_df[cou] = hold_df[('sum_'+cou)]/hold_df[('total')]

# choose style 
col_num = 10
plt.style.use('seaborn-bright')

# choose and check color palette
pal = sns.color_palette("deep", col_num).as_hex()
plt.rcParams["axes.prop_cycle"] = plt.cycler("color", pal)

# plot color palette
if False:
    fig, ax = plt.subplots(figsize=(4,2))
    for i in range(1*col_num):
        ax.plot([0,1], [i,i], linewidth=16)
iss_pal = [pal[0],pal[1],pal[2],pal[3],pal[4]]

# plot parameters
plt.rcParams.update({'hatch.color': 'k'})
plt.rcParams.update({'font.size': 14})

# loop over variables and assign colors by country
i = 0
column_color_dict={}
all_amount_var = bric_list
for column in all_amount_var:
    column_color_dict.update( {column : iss_pal[i]} )
    i = i+1

# setup subplot shape/size
fig = plt.figure(figsize=(12, 8))
grid = plt.GridSpec(ncols=1, nrows=1, wspace=0, hspace=0)

# allocate plots
main_ax = fig.add_subplot(grid[0,0], zorder=10)

# set axis ranges
main_ax.set_xlim(2007,2017)

# area chart
main_ax.set_prop_cycle('color',[column_color_dict.get(x) for x in hold_df[all_amount_var].columns])
stack = main_ax.stackplot(hold_df.index.values, [hold_df[var].values for var in all_amount_var], edgecolor='k', zorder=10, alpha=0.8)
patterns = ('//', '+', 'xx', '..', 'o')
for area, pattern in zip(stack, patterns):
    area.set_hatch(pattern)

# FORMATTING
   
# add axis title 
fig.text(0.06, 0.5, 'Share of Bond Positions Reallocated from Tax Havens', ha='center', va='center', rotation='vertical')

# add legend
labels = all_amount_var
labels = ["Brazil", "China", "India", "Russia", "South Africa"]
fig.legend(stack, labels, ncol=5, loc='lower center', frameon=True)

# format lines and ticks
for spine in ["top", "right"]:
    main_ax.spines[spine].set_visible(False)
for axis in ['bottom','left']:
    main_ax.spines[axis].set_linewidth(1.5)
main_ax.grid(False)
main_ax.tick_params('both', length=10, width=1.5, which='major')
main_ax.tick_params('both', length=10, width=1.5, which='minor')

# format the lines
plt.setp(main_ax.get_xticklabels(), visible=True)
main_ax.set_xticks([2007, 2009, 2011, 2013, 2015, 2017])
main_ax.set_yticks([0, .05, .1, .15, .2, .25])
main_ax.yaxis.grid()

plt.savefig( cmns1 / "graphs/brics_timeseries.pdf" , bbox_inches='tight', figsize=(12, 8), dpi=1200)
plt.show()
