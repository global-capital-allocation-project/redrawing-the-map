# ---------------------------------------------------------------------------------------------------
# Ultimate parent aggregation: Helper methods
#
# These are helper methods for the main code in UP_Aggregation.py
# ---------------------------------------------------------------------------------------------------
import pandas as pd
import numpy as np
from tqdm import tqdm, tqdm_pandas
tqdm.pandas()

# Constant used to indicate cycles
cycle_constant = "__CYCLE__"

# General function for flattening parent-child maps
def flatten_parent_child_map(df, child_col, parent_col, logger=None, break_cycles=True, debug=False, include_parents_in_lhs=False):
    """ This function generates a stationary transformation of arbitrary
    child-to-parent mappings.

    Parameters:
        df: The input dataframe
        child_col: Column identifying the child 
        parent_col: Column identifying the parent
        logger: Logger object
        break_cycles: Whether to break cycles at random (otherwise cyclical elements are dropped)

    Returns: 
        A dataframe with the flattened child-parent map
    """

    # If empty input, simply return
    if df.shape[0] == 0:
        return df[[child_col, parent_col]]

    # Find singletons
    singletons = list(set(df[parent_col]) - set(df[child_col]))
    extras = pd.DataFrame({
        child_col: singletons,
        parent_col: singletons
    })

    # Iterate the source mapping onto itself a number of times
    tmp_df = pd.concat(
        [df.dropna(), extras], axis=0, sort=False
    ).rename(
        columns={parent_col: "{}_1".format(parent_col), child_col: "{}_0".format(parent_col)}
    )
    for i in range(2,15):
        tmp_df = tmp_df.merge(
            tmp_df[['{}_{}'.format(parent_col, i-2), "{}_{}".format(parent_col, i-1)]].drop_duplicates(), 
            left_on="{}_{}".format(parent_col, i-1), right_on="{}_{}".format(parent_col, i-2), how="left", 
            suffixes=("", "_{}".format(i))
        ).drop(
            ["{}_{}_{}".format(parent_col, i-2, i)], axis=1
        ).rename(
            columns={"{}_{}_{}".format(parent_col, i-1, i): "{}_{}".format(parent_col, i)}
        )
        
    # Detect whether we have reached a stationary outcome, and whether there are any cycles
    tmp_df['_is_stationary'] = (
     (tmp_df['{}_{}'.format(parent_col, i)] == tmp_df['{}_{}'.format(parent_col, i-1)]) & 
     (tmp_df['{}_{}'.format(parent_col, i-1)] ==  tmp_df['{}_{}'.format(parent_col, i-2)]) &
     (tmp_df['{}_{}'.format(parent_col, i-2)] ==  tmp_df['{}_{}'.format(parent_col, i-3)])
    )
    tmp_df['stationary_outcome'] = ""
    tmp_df.loc[tmp_df._is_stationary == 1, "stationary_outcome"] = tmp_df['{}_{}'.format(parent_col, i)]
    if break_cycles:

        def _prepare_observation_for_cycle_break(row):
            return (cycle_constant, row['_tail'])

        tmp_df['_tail'] = list(zip(
            tmp_df['{}_{}'.format(parent_col, i-4)], 
            tmp_df['{}_{}'.format(parent_col, i-3)], 
            tmp_df['{}_{}'.format(parent_col, i-2)], 
            tmp_df['{}_{}'.format(parent_col, i-1)], 
            tmp_df['{}_{}'.format(parent_col, i)], 
        ))
        tmp_df.loc[tmp_df._is_stationary != 1, "stationary_outcome"] = tmp_df[tmp_df._is_stationary != 1].apply(_prepare_observation_for_cycle_break, axis=1)
    else:
        tmp_df.loc[tmp_df._is_stationary != 1, "stationary_outcome"] = cycle_constant
    n_cycles = tmp_df[[x[0] == cycle_constant for x in tmp_df.stationary_outcome]].shape[0]

    # Debug mode: just return everything
    if break_cycles == "debug":
        tmp_df = tmp_df[["{}_0".format(parent_col), "stationary_outcome"]].rename(
            columns={"{}_0".format(parent_col): child_col, "stationary_outcome": parent_col}
        )
        return tmp_df
    
    # If not breaking cycles, we simply discard the cyclical rows and return
    if not break_cycles or n_cycles == 0:
        if logger and n_cycles > 0:
            logger.warning("WARNING: Discarding {} rows out of {} due to cycles".format(n_cycles, df.shape[0]))
        tmp_df = tmp_df[[x[0] != cycle_constant for x in tmp_df.stationary_outcome]][["{}_0".format(parent_col), "stationary_outcome"]].rename(
            columns={"{}_0".format(parent_col): child_col, "stationary_outcome": parent_col}
        )
    
    # Otherwise, we pick one element at random
    else:
        
        # Report
        if logger:
            logger.warning("WARNING: Breaking cycles involving {} rows at random".format(n_cycles))
        
        # Set seed
        np.random.seed(1)
        
        # Data structure with cycles
        tmp_df = tmp_df[["{}_0".format(parent_col), "stationary_outcome"]].rename(
            columns={"{}_0".format(parent_col): child_col, "stationary_outcome": parent_col}
        )
        cycles = tmp_df[[x[0] == cycle_constant for x in tmp_df[parent_col]]]
        n_cycle_rows = cycles.shape[0]
        
        # Development flags
        if debug:
            tmp_df['resolved_from_cycle'] = 0

        # This data structure holds the resolved child-to-parent links that are
        # established via random choice
        cycle_resolution_master = {}

        # Iterate over cyclical rows
        for i in tqdm(range(n_cycle_rows)):

            # Unpack the row
            cyclical_row = cycles.iloc[i]
            child = cyclical_row[child_col]
            _, cyclical_elements = cyclical_row[parent_col]

            # If this CUSIP already appeared in a previous cycle, we use the parent
            # that was already established 
            if child in cycle_resolution_master:
                chosen_parent = cycle_resolution_master[child]

            # Else, pick one at random
            else:
                chosen_parent = np.random.choice(cyclical_elements)

            # Now make sure everything that appears in the cycle is always
            # linked to this one chosen parent
            cycle_resolution_master[child] = chosen_parent
            for _child in cyclical_elements:
                cycle_resolution_master[_child] = chosen_parent

            # Now we update the full mapping to reflect the broken cycles
            tmp_df.loc[tmp_df[child_col] == child, parent_col] = chosen_parent
            if debug:
                tmp_df.loc[tmp_df[child_col] == child, "resolved_from_cycle"] = 1
        
        # Ensure we picked up everything
        for child in tqdm(cycle_resolution_master.keys()):
            tmp_df.loc[tmp_df[child_col] == child, parent_col] = cycle_resolution_master[child]
            if debug:
                tmp_df.loc[tmp_df[child_col] == child, "resolved_from_cycle"] = 1
        for child in tqdm(cycle_resolution_master.keys()):
            tmp_df.loc[tmp_df[parent_col] == child, parent_col] = cycle_resolution_master[child]
            if debug:
                tmp_df.loc[tmp_df[parent_col] == child, "resolved_from_cycle"] = 1
    
    # Blank out links that are not on RHS from LHS if specified as such
    if not include_parents_in_lhs:
        tmp_df.loc[~tmp_df[child_col].isin(set(df[child_col])), parent_col] = ""
        
    return tmp_df
