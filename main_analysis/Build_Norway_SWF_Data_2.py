# ---------------------------------------------------------------------------------------------------
# Build_Norway_SWF_Data_2: This job establishes a crosswalk between the fixed income positions
# reported by the Norwegian sovereign wealth fund and CUSIP6 codes by performing a fuzzy merge
# ---------------------------------------------------------------------------------------------------
import os
import pandas as pd
import numpy as np
import time
import string
import json
import warnings
import logging
import re
import string
import sys
from pathlib import Path
import jellyfish
import textdistance
warnings.filterwarnings("ignore")

# Populate data path here
data_path = "<DATA_PATH>/cmns1/temp/"

# ---------------------------------------------------------------------------------------------------
# Vocabulary for string parsing
# ---------------------------------------------------------------------------------------------------

terms_by_type = {
   'Corporation': ['company', 'incorporated', 'corporation', 'corp.', 'corp', 'inc',
      '& co.', '& co', 'inc.', 's.p.a.', 'n.v.', 'a.g.', 'ag', 'nuf', 's.a.', 'sa', 's.f.',
      'oao', 'co.', 'co', 'group', 'grp', 'groupe'
   ],
   'General Partnership': ['soc.col.', 'stg', 'd.n.o.', 'ltda.', 'v.o.s.', 'a spol.',
      u've\xc5\x99. obch. spol.', 'kgaa', 'o.e.', 's.f.', 's.n.c.', 's.a.p.a.', 'j.t.d.',
      'v.o.f.', 'sp.j.', 'og', 'sd', ' i/s', 'ay', 'snc', 'oe', 'bt.', 's.s.', 'mb',
      'ans', 'da', 'o.d.', 'hb', 'pt', 'spa', 'dac'
   ],
   'Joint Stock / Unlimited': ['unltd', 'ultd', 'sal', 'unlimited', 'saog', 'saoc', 'aj',
      'yoaj', 'oaj', 'akc. spol.', 'a.s.'
   ],
   'Joint Venture': ['esv', 'gie', 'kv.', 'qk'],
   'Limited': ['pty. ltd.', 'pty ltd', 'ltd', 'l.t.d.', 'bvba', 'd.o.o.', 'ltda', 'gmbh',
      'g.m.b.h', 'kft.', 'kht.', 'zrt.', 'ehf.', 's.a.r.l.', 'd.o.o.e.l.', 's. de r.l.',
      'b.v.', 'tapui', 'sp. z.o.o.', 's.r.l.', 's.l.', 's.l.n.e.', 'ood', 'oy', 'rt.',
      'teo', 'uab', 'scs', 'sprl', 'limited', 'bhd.', 'sdn. bhd.', 'sdn bhd', 'as',
      'lda.', 'tov', 'pp'
   ],
   'Limited Liability Company': ['pllc', 'llc', 'l.l.c.', 'plc.', 'plc', 'hf.', 'oyj',
      'a.e.', 'nyrt.', 'p.l.c.', 'sh.a.', 's.a.', 's.r.l.', 'srl.', 'srl', 'aat', '3at', 'd.d.',
      's.r.o.', 'spol. s r.o.', 's.m.b.a.', 'smba', 'sarl', 'nv', 'sa', 'aps',
      'a/s', 'p/s', 'sae', 'sasu', 'eurl', 'ae', 'cpt', 'as', 'ab', 'asa', 'ooo', 'dat',
      'vat', 'zat', 'mchj', 'a.d.'
   ],
   'Limited Liability Limited Partnership': ['lllp', 'l.l.l.p.'],
   'Limited Liability Partnership': ['llp', 'l.l.p.', 'sp.p.', 's.c.a.', 's.c.s.'],
   'Limited Partnership': ['gmbh & co. kg', 'gmbh & co. kg', 'lp', 'l.p.', 's.c.s.',
      's.c.p.a', 'comm.v', 'k.d.', 'k.d.a.', 's. en c.', 'e.e.', 's.a.s.', 's. en c.',
      'c.v.', 's.k.a.', 'sp.k.', 's.cra.', 'ky', 'scs', 'kg', 'kd', 'k/s', 'ee', 'secs',
      'kda', 'ks', 'kb','kt'
   ],
   'Mutual Fund': ['sicav', 'holdings', 'hlds'],
   'No Liability': ['nl'],
   'Non-Profit': ['vzw', 'ses.', 'gte.'],
   'Professional Corporation': ['p.c.', 'vof', 'snc'],
   'Professional Limited Liability Company': ['pllc', 'p.l.l.c.'],
   'Sole Proprietorship': ['e.u.', 's.p.', 't:mi', 'tmi', 'e.v.', 'e.c.', 'et', 'obrt',
      'fie', 'ij', 'fop', 'xt'
   ]
}

terms_by_country = {
   'Albania': ['sh.a.', 'sh.p.k.'],
   'Argentina': ['s.a.', 's.r.l.', 's.c.p.a', 'scpa', 's.c.e i.', 's.e.', 's.g.r',
      'soc.col.'
   ],
   'Australia': ['nl', 'pty. ltd.', 'pty ltd'],
   'Austria': ['e.u.', 'stg', 'gesbr', 'a.g.', 'ag', 'og', 'kg'],
   'Belarus': ['aat', '3at'],
   'Belgium': ['esv', 'vzw', 'vof', 'snc', 'comm.v', 'scs', 'bvba', 'sprl', 'cvba',
      'cvoa', 'sca', 'sep', 'gie'
   ],
   'Bosnia / Herzegovina': ['d.d.', 'a.d.', 'd.n.o.', 'd.o.o.', 'k.v.', 's.p.'],
   'Brazil': ['ltda', 's.a.', 'pllc', 'ad', 'adsitz', 'ead', 'et', 'kd', 'kda', 'sd'],
   'Bulgaria': ['ad', 'adsitz', 'ead', 'et', 'kd', 'kda', 'sd'],
   'Cambodia': ['gp', 'sm pte ltd.', 'pte ltd.', 'plc ltd.', 'peec', 'sp'],
   'Canada': ['gp', 'lp', 'sp'],
   'Chile': ['eirl', 's.a.', 'sgr', 's.g.r.', 'ltda', 's.p.a.', 'sa', 's. en c.',
      'ltda.'
   ],
   'Columbia': ['s.a.', 'e.u.', 's.a.s.', 'suc. de descendants', 'sca'],
   'Croatia': ['d.d.', 'd.o.o.', 'obrt'],
   'Czech Republic': ['a.s.', 'akc. spol.', 's.r.o.', 'spol. s r.o.', 'v.o.s.', u've\xc5\x99. obch. spol.', 'a spol.', 'k.s.', 'kom. spol.', 'kom. spol.'],
   'Denmark': ['i/s', 'a/s', 'k/s', 'p/s', 'amba', 'a.m.b.a.', 'fmba', 'f.m.b.a.', 'smba',
      's.m.b.a.', 'g/s'
   ],
   'Dominican Republic': ['c. por a.', 'cxa', 's.a.', 's.a.s.', 'srl.', 'srl', 'eirl.', 'sa',
      'sas'
   ],
   'Ecuador': ['s.a.', 'c.a.', 'sa', 'ep'],
   'Egypt': ['sae'],
   'Estonia': ['fie'],
   'Finland': ['t:mi', 'tmi', 'as oy', 'as.oy', 'ay', 'ky', 'oy', 'oyj', 'ok'],
   'France': ['sicav', 'sarl', 'sogepa', 'ei', 'eurl', 'sasu', 'fcp', 'gie', 'sep', 'snc',
      'scs', 'sca', 'scop', 'sem', 'sas'
   ],
   'Germany': ['gmbh & co. kg', 'gmbh & co. kg', 'e.g.', 'e.v.', 'gbr', 'ohg', 'partg',
      'kgaa', 'gmbh', 'g.m.b.h.', 'ag'
   ],
   'Greece': ['a.e.', 'ae', 'e.e.', 'ee', 'epe', 'e.p.e.', 'mepe', 'm.e.p.e.', 'o.e.',
      'oe', 'ovee', 'o.v.e.e.'
   ],
   'Guatemala': ['s.a.', 'sa'],
   'Haiti': ['sa'],
   'Hong Kong': ['ltd', 'unltd', 'ultd', 'limited'],
   'Hungary': ['e.v.', 'e.c.', 'bt.', 'kft.', 'kht.', 'kkt.', 'k.v.', 'zrt.', 'nyrt',
      'ev', 'ec', 'rt.'
   ],
   'Iceland': ['ehf.', 'hf.', 'ohf.', 's.f.', 'ses.'],
   'India': ['pvt. ltd.', 'ltd.', 'psu', 'pse'],
   'Indonesia': ['ud', 'fa', 'pt'],
   'Ireland': ['cpt', 'teo'],
   'Israel': ['b.m.', 'bm', 'ltd', 'limited'],
   'Italy': ['s.n.c.', 's.a.s.', 's.p.a.', 's.a.p.a.', 's.r.l.', 's.c.r.l.', 's.s.'],
   'Latvia': ['as', 'sia', 'ik', 'ps', 'ks'],
   'Lebanon': ['sal'],
   'Lithuania': ['uab', 'ab', 'ij', 'mb'],
   'Luxemborg': ['s.a.', 's.a.r.l.', 'secs'],
   'Macedonia': ['d.o.o.', 'd.o.o.e.l', 'k.d.a.', 'j.t.d.', 'a.d.', 'k.d.'],
   'Malaysia': ['bhd.', 'sdn. bhd.'],
   'Mexico': ['s.a.', 's. de. r.l.', 's. en c.', 's.a.b.', 's.a.p.i.'],
   'Mongolia': ['xk', 'xxk'],
   'Netherlands': ['v.o.f.', 'c.v.', 'b.v.', 'n.v.'],
   'New Zealand': ['tapui', 'ltd', 'limited'],
   'Nigeria': ['gte.', 'plc', 'ltd.', 'ultd.'],
   'Norway': ['asa', 'as', 'ans', 'ba', 'bl', 'da', 'etat', 'fkf', 'hf', 'iks', 'kf',
      'ks', 'nuf', 'rhf', 'sf'
   ],
   'Oman': ['saog', 'saoc'],
   'Pakistan': ['ltd.', 'pvt. ltd.', 'ltd', 'limited'],
   'Peru': ['sa', 's.a.', 's.a.a.'],
   'Philippines': ['coop.', 'corp.', 'corp', 'ent.', 'inc.', 'inc', 'llc', 'l.l.c.',
      'ltd.'
   ],
   'Poland': ['p.p.', 's.k.a.', 'sp.j.', 'sp.k.', 'sp.p.', 'sp. z.o.o.', 's.c.', 's.a.'],
   'Portugal': ['lda.', 'crl', 's.a.', 's.f.', 'sgps'],
   'Romania': ['s.c.a.', 's.c.s.', 's.n.c.', 's.r.l.', 'o.n.g.', 's.a.'],
   'Russia': ['ooo', 'oao', 'zao', '3ao', 'пао', 'оао', 'ооо'],
   'Serbia': ['d.o.o.', 'a.d.', 'k.d.', 'o.d.'],
   'Singapore': ['bhd', 'pte ltd', 'sdn bhd', 'llp', 'l.l.p.', 'ltd.', 'pte'],
   'Slovenia': ['d.d.', 'd.o.o.', 'd.n.o.', 'k.d.', 's.p.'],
   'Slovakia': ['a.s.', 'akc. spol.', 's.r.o.', 'spol. s r.o.', 'k.s.', 'kom. spol.', 'v.o.s.', 'a spol.'],
   'Spain': ['s.a.', 's.a.d.', 's.l.', 's.l.l.', 's.l.n.e.', 's.c.', 's.cra', 's.coop',
      'sal', 'sccl'
   ],
   'Sweden': ['ab', 'hb', 'kb'],
   'Switzerland': ['ab', 'sa', 'gmbh', 'g.m.b.h.', 'sarl', 'sagl'],
   'Turkey': ['koop.'],
   'Ukraine': ['dat', 'fop', 'kt', 'pt', 'tdv', 'tov', 'pp', 'vat', 'zat', 'at'],
   'United Kingdom': ['plc.', 'plc', 'cic', 'cio', 'l.l.p.', 'llp', 'l.p.', 'lp', 'ltd.',
      'ltd', 'limited'
   ],
   'United States of America': ['llc', 'inc.', 'corporation', 'incorporated', 'company',
      'limited', 'corp.', 'inc.', 'inc', 'llp', 'l.l.p.', 'pllc', 'and company',
      '& company', 'inc', 'inc.', 'corp.', 'corp', 'ltd.', 'ltd', '& co.', '& co', 'co.',
      'co', 'lp'
   ],
   'Uzbekistan': ['mchj', 'qmj', 'aj', 'oaj', 'yoaj', 'xk', 'xt', 'ok', 'uk', 'qk']
}

corp_identifier = sum(terms_by_type.values(), [])
corp_identifier =  corp_identifier + sum(terms_by_country.values(), [])
punc_translator=str.maketrans('','',string.punctuation)
corp_identifier = [wrd.translate(punc_translator) for wrd in corp_identifier]
corp_identifier = list(dict.fromkeys(corp_identifier))
country_identifier= ['republic','government', 'govt', 'gov', 'rep', 'repubblica', 'republik', 'republique', 'country', 'kingdom'] 
state_identifier = ['state of', 'state', 'st', 'city', 'city of', 'province']
bond_identifier = ['commonwealth', 'comwlth', 'treas', 'treasury', 'muni', 'state', 'cmo', 'zcb', 'nts', 'mtg', 'bonds']
reserve_words = ['united kingdom', 'bell canada', 'hp inc', 'petroleos mexicanos']

# ---------------------------------------------------------------------------------------------------
# Similarity score utilities: these calculate similarity scores using different algorithms:
#     1. eidt based:     ['levenshtein_distance', 'damerau_levenshtein_distance', 'hamming_distance', 
#                         'jaro_similarity', 'jaro_winkler_similarity']
#     2. tokens based:   ['cosine', 'sorensen', 'jaccard', 'overlap', 'tversky']
#     3. phonetic based: ['metaphone', 'soundex', 'match_rating_comparison']
# ---------------------------------------------------------------------------------------------------

# List of string matching algos:
# 	ascend_methods: higher score means higher simarlity
# 	descend_methods: lower score means higher simarlity
ascend_methods = ['levenshtein_distance', 'damerau_levenshtein_distance', 'hamming_distance']
descend_methods = ['jaro_similarity', 'jaro_winkler_similarity', 'match_rating_comparison', 'cosine', 'sorensen',                      'jaccard', 'overlap', 'tversky', 'soundex']

def match_score(name, mname, method='jaro_similarity'): 

    
    # edit based algos: ['levenshtein_distance', 'damerau_levenshtein_distance', 'hamming_distance', 'jaro_similarity', 'jaro_winkler_similarity']
    if method == 'levenshtein_distance':
        score = jellyfish.levenshtein_distance(name, mname) 
        
    elif method == 'damerau_levenshtein_distance':
        score = jellyfish.damerau_levenshtein_distance(name, mname) 
    
    elif method == 'hamming_distance':
        score = jellyfish.hamming_distance(name, mname) 

    elif method == 'jaro_similarity':
        score = jellyfish.jaro_similarity(name, mname)

    elif method == 'jaro_winkler_similarity':
        score = jellyfish.jaro_winkler_similarity(name, mname)
    

    # tokens based algos: ['cosine', 'sorensen', 'jaccard', 'overlap', 'tversky']
    elif method == 'cosine':
        score = textdistance.cosine(name, mname)
        
    elif method == 'sorensen': 
        score = textdistance.sorensen(name, mname)
    
    elif method == 'jaccard': 
        score = textdistance.jaccard(name, mname)
    
    # too many matched
    elif method == 'overlap': 
        score = textdistance.overlap(name, mname)
    
    elif method == 'tversky': 
        score = textdistance.tversky(name, mname)
        
    # phonetic match: ['metaphone', 'soundex', 'match_rating_comparison'] only returns 1 or 0  
    
    elif method == 'metaphone':
        return float(jellyfish.metaphone(nm) == jellyfish.metaphone(name))
    
    elif method == 'soundex': 
        try: 
            score = float(jellyfish.soundex(name) == jellyfish.soundex(mname))
        except:
            score = 0
    
    # Phonetic Algo 'Match rating comparison' needed to be test: too many matched    
    elif method == 'match_rating_comparison':
        score = jellyfish.match_rating_comparison(name, mname)
            

    return score

# ---------------------------------------------------------------------------------------------------
# Pre-processing functions
# ---------------------------------------------------------------------------------------------------

# package for translation
from googletrans import Translator

# package for stemmer
from nltk.stem.snowball import SnowballStemmer
import nltk

# import country converter (should be installed using pip if missing)
import country_converter as coco

import warnings
warnings.filterwarnings("ignore")

# Initialization: the functions we will use in the preprocessing 
translator = Translator()
stemmer = SnowballStemmer('english')

stopwords_list = nltk.corpus.stopwords.words('english')
suffix_list = corp_identifier  + country_identifier + bond_identifier + state_identifier
append_stop_words = suffix_list + ['govern', 'government', 'state', 'rep', 'treas']
minimal_stopword_list = stopwords_list + ['govern', 'government', 'state', 'rep', 'treas']
stopwords_list = stopwords_list + append_stop_words

punc_translator=str.maketrans('','',string.punctuation)

full_categ_list = ['corp', 'country', 'state', 'bond']

'''
Self Defined Analyzer: 
base_analyzer: baseline preprocessing (simply lower case, remove some of the punctuation)
preprocess: completely clean the string, lower case, remove all punctuations, remove all the stopwords, stem words, 
	    standardize the country names (optional), translate foreign language into English (optional)
'''
def base_analyzer(docstr):
    '''
	Minimal preprocess of the string, used in exact_merge

	Input: docstr: string to process (str)
	Output: the cleaned string (str)
		
    '''
    return docstr.lower().replace('.', '').replace(',', '').replace(' ', '').translate(punc_translator)

def get_categ(docstr): 
    wrds = docstr.lower().replace('/', ' ').split(' ')
    wrds = [wrd.translate(punc_translator) for wrd in wrds]
    
    corp = [wrd for wrd in wrds if wrd in corp_identifier]
    country = [wrd for wrd in wrds if wrd in country_identifier]
    bond = [wrd for wrd in wrds if wrd in bond_identifier]
    state = [wrd for wrd in wrds if wrd in state_identifier]
    

    if len(country) > 0: 
        return 'country'
    
    elif len(state) > 0:
        return 'state'
        
    elif len(bond) > 0: 
        return 'bond'
    
    elif len(corp) > 0: 
        return 'corp'
    
    else:
        return np.nan
    
def preprocess(docstr, country = False): 
    '''
	Comprehensive preprocessing of the string, used in fuz_match_preprocess and fuz_match_top
	
	Input: docstr: string to process (str)
	       country: whether replace the country name in the string
        Output: the cleaned string (str)

    '''
    categ = get_categ(docstr)
    
    wrds = docstr.lower().replace('/', ' ').split(' ')
    wrds = [wrd.translate(punc_translator) for wrd in wrds]
    
    sent = ' '.join(wrds)
    reserve_list = [wrd for wrd in reserve_words if wrd in sent] 
    if len(reserve_list) >= 1: 
        wrds = [wrd for wrd in wrds if wrd not in minimal_stopword_list]
        return ' '.join(wrds)
    else: 
        del sent
        
    tokens = [wrd for wrd in wrds if wrd not in stopwords_list]
    
    # standardize the nation 
    if country == True and len(tokens) < 3 and (categ not in ['corp', 'state']):
        try: 
            sent = ' '.join(tokens)

            # sent = translator.translate(sent, dest='en').text
            sent =  coco.convert(sent, to='name_short')
                           
            if len(sent[0]) > 1: 
                tokens = [wrd.lower() for wrd in sent]
                
                return ' '.join(wrds)
            
            elif sent != 'not found':
                tokens = sent.lower().split(' ')
                return ' '.join(tokens)
            
            else: 
                return ' '.join(tokens)
        
        except:
            sent = ' '.join(tokens)
            return sent
        
    else:
        sent = ' '.join(tokens)
        return sent

# ---------------------------------------------------------------------------------------------------
# Exact-merge functions
# ---------------------------------------------------------------------------------------------------
import pandas as pd

def exact_merge(df, base_df): 
    '''
    Exact merge between dataframe and base_df, under same Residency country and same Sector
    
    Input: df: dataframe that need to be matched, must have columns ['Name', 'Residency', 'Sector']
           base_df: dataframe that contains all the name, must have columns ['Name', 'Residency', 'Sector', 'IssuerNumber']
    Output: the merged dataframe (for name in df), 'IssuerNumber' == np.nan means that there is no matched value in base_df
    
    '''
    
    df['cleaned_name'] = df['Name'].apply(base_analyzer)
    base_df['cleaned_name'] = base_df['Name'].apply(base_analyzer)
    print('Total number of the bonds is ', df.shape[0])
    
    df_temp = df[df['Residency'] != '']
    df = df[df['Residency'] == '']
    print("There are ", df.shape[0], 'missing value in Residency in the data')
    
    print("For valid residency \nBefore merging, number is", df_temp.shape[0])
    df_temp = pd.merge(df_temp, base_df[['cleaned_name', 'IssuerNumber', 'Sector', 'Residency']], on=['cleaned_name', 'Sector', 'Residency'], how='left')
    print("After merging, number is", df_temp.shape[0])
    
    print("For missing residency \nBefore merging, number is", df.shape[0])
    df = pd.merge(df, base_df[['cleaned_name', 'IssuerNumber', 'Sector']], on=['cleaned_name', 'Sector'], how='left')
    print("After merging, number is", df.shape[0])
    
    df = df.append(df_temp)
    
    print("Total number of the record after merging: ", df.shape[0])
    
    df = df.groupby(['Name', 'Sector', 'Residency']).first().reset_index()
    df = df.drop(columns = ['cleaned_name'])
    
    print("Final number of the record after dropping duplicates (mutiple merge): ", df.shape[0])
    
    return df


# ---------------------------------------------------------------------------------------------------
# Merge functions via similarity score threshold
# The functions get the top-1 matched name if this name's similarity score higher than the threshold
# We also require a hard merge on residency, and prioritize matches on sector
# ---------------------------------------------------------------------------------------------------

def phonetic_match(name, namelist, method): 

    if method == 'metaphone': 
        return [cusip for nm, cusip in namelist.items() if (jellyfish.metaphone(nm) == jellyfish.metaphone(name))]
    elif method == 'soundex': 
        try: 
            return [cusip for nm, cusip in namelist.items() if (jellyfish.soundex(nm) == jellyfish.soundex(name))]
        except:
            return []
    
    elif method == 'nysiis': 
        return [cusip for nm, cusip in namelist.items() if (jellyfish.nysiis(nm) == jellyfish.nysiis(name))]


def precise_match_algo(name, namelist, method='jaro_similarity', thres = 0.95): 
    
    if method == 'match_rating_comparison':
        cusips = [cusip for nm, cusip in namelist.items() if (jellyfish.match_rating_comparison(name, nm) == True)]
    
    score_list = {cusip: match_score(name, nm, method) for nm, cusip in namelist.items() if match_score(name, nm, method) > thres}
    
    if len(score_list) >= 1: 
        return sorted(score_list, key=score_list.get, reverse=True)
    
    else:
        return []
    

def precise_match_agg(name, namelist, thres = 0.90, algos = ['jaro_similarity', 'jaro_winkler_similarity', 'cosine', 'overlap'], precise = True):
    
    # most precise
    result = phonetic_match(name, namelist, 'metaphone')
    if len(result) > 0: 
            return result[0]
        
    # get the top matched result 
    for algo in algos: 
        result = precise_match_algo(name, namelist, algo, thres)
        if len(result) >= 1:
            return result[0]
    
    if precise == True: 
        result = phonetic_match(name, namelist, 'soundex')
        if len(result) > 0:
            sub_name_list = {cname: cusip for cname, cusip in namelist.items() if cusip in result}
            for algo in algos: 
                result = precise_match_algo(name, sub_name_list, algo, thres-0.05)
                if len(result) >= 1:
                    return result[0]
        
    return -1


def fuzy_match_precise(x, match_list, sort_catg=True, thres=0.85, algos = [ 'jaro_winkler_similarity', 'sorensen', 'jaccard']): 
    name = x.cleaned_name
    sector = x.Sector
    resid = x.Residency
    categ = x.categ

    
    # 1. restrict the bond_match_list to the national
    if len(resid) > 0:
        match_list = match_list[match_list['Residency'] == resid]
        
    # 2. delete the securities that not in the same sector (after testing, I found that the only difference is the "corp" and other categories): 
    if sort_catg == True: 
        if categ == 'corp': 
            categ_list = [categ, np.nan] 
            match_list = match_list[match_list['categ'].apply(lambda x: x in categ_list)]
        elif categ != 'corp' and (categ in full_categ_list): 
            match_list = match_list[match_list['categ'].apply(lambda x: x != 'corp')]
            
    
    # 3. use the bond list in same sector: 
    if len(sector) > 0:
        match_list_sector = match_list[match_list['Sector'] == sector]
    
    match_list_sector = match_list_sector.set_index('bonds_cname')
    name_list_sector = match_list_sector['IssuerNumber'].to_dict()
    
    result = precise_match_agg(name, name_list_sector, thres, algos)

    if result == -1: 
        
        match_list = match_list.set_index('bonds_cname')
        name_list = match_list['IssuerNumber'].to_dict()
        result = precise_match_agg(name, name_list, thres, algos, True)
        

    return result

def top_match(name, namelist, algos = [ 'jaro_winkler_similarity', 'sorensen'], num = 5): 
    
    match_list = pd.DataFrame(namelist.items())
    match_list.columns = ['bonds_cname', 'IssuerNumber']
    match_list['rank_mean'] = 0
    
    for algo in algos: 
        match_list[algo] = match_list['bonds_cname'].apply(lambda nm: match_score(name, nm, algo))
        
        # replace the value of the score by the rank --> rank 1 means the best choice
        if algo in ascend_methods:
            match_list[algo] = match_list[algo].rank(ascending = True)
        else: 
            match_list[algo] = match_list[algo].rank(ascending = False)
            
        match_list['rank_mean'] = match_list['rank_mean'] + match_list[algo]
    
    match_list['rank_mean'] = match_list['rank_mean'] / len(algos)
    match_list.sort_values(by=['rank_mean'], inplace = True)
    #return match_list
    
    return match_list.iloc[:num]['IssuerNumber'].to_list()

def fuzy_match_top(name, sector, resid, match_list, algos = [ 'levenshtein_distance', 'jaro_winkler_similarity', 'sorensen'], num = 5, rest_sector = False):
        
    #1. restrict the bond_match_list to the national
    if len(resid) > 0:
        match_list = match_list[match_list['Residency'] == resid]
        
    if rest_sector == True: 
        if len(sector > 0): 
            match_list = match_list[match_list['Sector'] == sector]
        
    match_list = match_list.set_index('bonds_cname')
    name_list = match_list['IssuerNumber'].to_dict()
    result = top_match(name, name_list, algos, num)

    return result

# ---------------------------------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------------------------------

# read in data
norway_bond_df = pd.read_stata(data_path + 'swf_bonds_to_match.dta')
bond_match_list = pd.read_stata(data_path + 'bonds_matchfile_preferred.dta')
print('Total number of records in norway bonds data is ', norway_bond_df.shape[0])
print('Total number of records in bonds match file is ', bond_match_list.shape[0])

norway_identifier = pd.read_stata(data_path + 'issuer_numbers_in_norway_mutual_fund_data.dta')
norway_identifier['norway_mf_identifier'] = 1

bond_match_list = pd.merge(bond_match_list, norway_identifier, on='IssuerNumber', how='left')
bond_match_list['norway_mf_identifier'] = bond_match_list['norway_mf_identifier'].replace(np.nan, 0)

norway_bond_df['categ'] = norway_bond_df['Name'].apply(get_categ)
bond_match_list['categ'] = bond_match_list['Name'].apply(get_categ)

# create the data to get the orginal name from CUSIP
bond_mlist_temp = bond_match_list.groupby('IssuerNumber').first().reset_index()[['Name', 'IssuerNumber', 'norway_mf_identifier', 'Sector']].rename(columns = {'Name': 'matched_name','Sector': 'matched_sector'})

print('Number of Securities appears in Norway Mutual Funds from bonds match file: ', bond_match_list.norway_mf_identifier.sum())
print('Missing value in Residency Country in norway bonds data is: ', norway_bond_df[norway_bond_df['Residency'] == ''].shape[0], 'over total number of ', norway_bond_df.shape[0])

# roughly clean the name by using lower cases and remove the spaces
norway_bond_df['cleaned_name'] = norway_bond_df['Name'].apply(base_analyzer)
bond_match_list['cleaned_name'] = bond_match_list['Name'].apply(base_analyzer)
norway_bond_df = norway_bond_df.drop(columns = ['cleaned_name'])
bond_match_list = bond_match_list.drop(columns = ['cleaned_name'])

### Step 1. Precise Merge after Minimal Preprocessing 

NN = norway_bond_df.shape[0]
sum_MV = norway_bond_df.MarketValueUSD.sum()
print('PRECISE MERGE: using securities appears in norway mutual funds')
t0 = time.time()
norway_bond_df = exact_merge(norway_bond_df, bond_match_list[bond_match_list['norway_mf_identifier'] == 1])
stp1_norway_df = norway_bond_df[norway_bond_df.IssuerNumber.isna() == 0]
norway_bond_df = norway_bond_df[norway_bond_df.IssuerNumber.isna()].drop(columns = 'IssuerNumber')
print('Matched ratio in First Step in Securities Invested in Norway Mutual Fund: ', stp1_norway_df.shape[0]/NN)

print('\nPRECISE MERGE: using rest of the preferred securities')
norway_bond_df = exact_merge(norway_bond_df, bond_match_list[bond_match_list['norway_mf_identifier'] == 0])
stp1_norway_df_2 = norway_bond_df[norway_bond_df.IssuerNumber.isna() == 0]
norway_bond_df = norway_bond_df[norway_bond_df.IssuerNumber.isna()].drop(columns = 'IssuerNumber')
print('Matched ratio in First Step in Other Preferred Securities: ', stp1_norway_df_2.shape[0]/norway_bond_df.shape[0])

t1 = time.time()
print('step 1 merged in ', t1-t0)

stp1_norway_df = stp1_norway_df.append(stp1_norway_df_2)
del stp1_norway_df_2

stp1_norway_df = pd.merge(stp1_norway_df, bond_mlist_temp, on='IssuerNumber', how='left')
stp1_norway_df.sort_values(by = ['MarketValueUSD'], ascending=False, inplace=True)
stp1_norway_df['merge_step'] = 1
stp1_norway_df.to_csv(data_path + 'stp1_norway_df.csv', index=False)

### Step 2. Fuzzy Merge with a Similarity Score Threshold

norway_bond_df['categ'] = norway_bond_df['Name'].apply(get_categ)
bond_match_list['categ'] = bond_match_list['Name'].apply(get_categ)

t0 = time.time()
norway_bond_gov = norway_bond_df[norway_bond_df['Sector'] == 'Government']
norway_bond_ngov = norway_bond_df[norway_bond_df['Sector'] != 'Government']
norway_bond_gov['cleaned_name'] = norway_bond_gov['Name'].apply(preprocess)
norway_bond_ngov['cleaned_name'] = norway_bond_ngov['Name'].apply(lambda x: preprocess(x, country = True))

norway_bond_df = norway_bond_gov.append(norway_bond_ngov)
del norway_bond_gov, norway_bond_ngov

bond_mlist_gov = bond_match_list[bond_match_list['Sector'] == 'Government']
bond_mlist_ngov = bond_match_list[bond_match_list['Sector'] != 'Government']
bond_mlist_gov['bonds_cname'] = bond_mlist_gov['Name'].apply(preprocess)
bond_mlist_ngov['bonds_cname'] = bond_mlist_ngov['Name'].apply(lambda x: preprocess(x, country = True))

bond_match_list = bond_mlist_gov.append(bond_mlist_ngov)
del bond_mlist_gov, bond_mlist_ngov
t1 = time.time()
print('clean name in ', t1-t0)

norway_bond_df.to_csv(data_path + 'norway_bond_df.csv', index=False)
bond_match_list.to_csv(data_path + 'bond_match_list.csv', index=False)

print('FUZZY MERGE: using securities appears in norway mutual funds')
t0 = time.time()
norway_bond_df['IssuerNumber'] = norway_bond_df.apply(lambda x: fuzy_match_precise(x, bond_match_list[bond_match_list['norway_mf_identifier'] == 1], thres=0.91, algos = [ 'jaro_winkler_similarity', 'sorensen']), axis=1)
stp2_norway_df_temp1 = norway_bond_df[norway_bond_df.IssuerNumber != -1]
norway_bond_df = norway_bond_df[norway_bond_df.IssuerNumber == -1].drop(columns = 'IssuerNumber')
print('Matched ratio in Second Step in Securities Invested in Norway Mutual Fund: ', stp2_norway_df_temp1.shape[0]/NN)

norway_bond_df['IssuerNumber'] = norway_bond_df.apply(lambda x: fuzy_match_precise(x, bond_match_list[bond_match_list['norway_mf_identifier'] == 0], thres=0.91,algos = [ 'jaro_winkler_similarity', 'sorensen']), axis=1)
stp2_norway_df_temp2 = norway_bond_df[norway_bond_df.IssuerNumber != -1]
norway_bond_df = norway_bond_df[norway_bond_df.IssuerNumber == -1].drop(columns = 'IssuerNumber')
print('Matched ratio in Second Step in Other Preferred Securities: ', stp2_norway_df_temp2.shape[0]/NN)
t1 = time.time()
print('step 2 merged in ', t1-t0)

stp2_norway_df = stp2_norway_df_temp1.append(stp2_norway_df_temp2)
del stp2_norway_df_temp1, stp2_norway_df_temp2

stp2_norway_df = pd.merge(stp2_norway_df, bond_mlist_temp, on='IssuerNumber', how='left')
print('Number of left records is: ', norway_bond_df.shape[0])

stp2_norway_df.sort_values(by = ['MarketValueUSD'], ascending=False, inplace=True)
stp2_norway_df['merge_step'] = 2 
stp2_norway_df.to_csv(data_path + 'stp2_norway_df.csv', index=False)

norway_bond_df.to_csv(data_path + 'unmatched.csv', index=False)

### Step 3. Get the names with top similarity scores

norway_bond_df = pd.read_csv(data_path + 'unmatched.csv')

t0 = time.time()
algos = ['damerau_levenshtein_distance', 'jaro_winkler_similarity', 'sorensen', 'cosine', 'soundex']
norway_bond_df['top_matched'] = norway_bond_df.apply(lambda x: fuzy_match_top(x.cleaned_name, x.Sector, x.Residency, bond_match_list, algos), axis=1)
t1 = time.time()
print('step 3 merged in ', t1-t0)

stp3_norway_df_temp = norway_bond_df.copy()
for i in range(3): 
    stp3_norway_df_temp['top_CUSIP_' + str(i+1)] = stp3_norway_df_temp['top_matched'].apply(lambda x: x[i])
    stp3_norway_df_temp = pd.merge(stp3_norway_df_temp, bond_mlist_temp[['matched_name', 'IssuerNumber']].rename(columns = {'matched_name': 'top_name_' + str(i+1), 'IssuerNumber': 'top_CUSIP_' + str(i+1)}),                         on='top_CUSIP_' + str(i+1), how='left')

print(stp3_norway_df_temp.shape[0])
stp3_norway_df_temp.sort_values(by = ['MarketValueUSD'], ascending=False, inplace=True)
stp3_norway_df_temp.to_csv(data_path + 'stp3_norway_df_tops.csv', index=False)

### Step 4: Match with the rest of the bond list 

other_match_list = pd.read_stata(data_path + 'bonds_matchfile_other.dta')
bond_mlist_gov = other_match_list[other_match_list['Sector'] == 'Government']
bond_mlist_ngov = other_match_list[other_match_list['Sector'] != 'Government']
bond_mlist_gov['bonds_cname'] = bond_mlist_gov['Name'].apply(preprocess)
bond_mlist_ngov['bonds_cname'] = bond_mlist_ngov['Name'].apply(lambda x: preprocess(x, country = True))
other_match_list = bond_mlist_gov.append(bond_mlist_ngov)
del bond_mlist_gov, bond_mlist_ngov

other_match_list['categ'] = other_match_list['Name'].apply(get_categ)
other_mlist_temp = other_match_list.groupby('IssuerNumber').first().reset_index()[['Name', 'IssuerNumber','categ']].rename(columns = {'Name': 'matched_name', 'categ': 'matched_categ'})
other_match_list.to_csv(data_path + 'other_match_list.csv', index=False)

other_match_list = pd.read_csv(data_path + 'other_match_list.csv')
print('The total records amount of other bonds is: ', other_match_list.shape[0])
norway_bond_df = pd.read_csv(data_path + 'unmatched.csv')

other_match_list['bonds_cname'] = other_match_list['bonds_cname'].apply(str)

other_mlist_temp = other_match_list.groupby('IssuerNumber').first().reset_index()[['Name', 'IssuerNumber','Sector']].rename(columns = {'Name': 'matched_name', 'Sector': 'matched_sector'})

print('FUZZY MERGE in Total Bond List')
t0 = time.time()
norway_bond_df['IssuerNumber'] = norway_bond_df.apply(lambda x: fuzy_match_precise(x, other_match_list, thres=0.94, algos = [ 'jaro_winkler_similarity', 'sorensen']), axis=1)
stp3_norway_df = norway_bond_df[norway_bond_df.IssuerNumber != -1]
norway_bond_df = norway_bond_df[norway_bond_df.IssuerNumber == -1].drop(columns = 'IssuerNumber')
print('Matched ratio in Second Step in Securities Invested in Norway Mutual Fund: ', stp3_norway_df.shape[0]/NN)
t1 = time.time()
print('step 3 merged in ', t1-t0)

stp3_norway_df = pd.merge(stp3_norway_df, other_mlist_temp, on='IssuerNumber', how='left')

print(stp3_norway_df.shape[0])
stp3_norway_df.sort_values(by = ['MarketValueUSD'], ascending=False, inplace=True)
stp3_norway_df['merge_step'] = 3
stp3_norway_df.to_csv(data_path + 'stp3_norway_df.csv', index=False)

final_merged_df = stp1_norway_df.append(stp2_norway_df)
final_merged_df = final_merged_df.append(stp3_norway_df)
final_merged_df = final_merged_df.drop(columns = ['categ'])
final_merged_df.sort_values(by = ['MarketValueUSD'], ascending=False, inplace=True)
final_merged_df.to_csv(data_path + 'merged_norway_bonds.csv', index=False)

# some corrections after manual inspection
final_merged_df.loc[final_merged_df.IssuerNumber == "44981U", "IssuerNumber"] = "771195"   # roche
final_merged_df.loc[final_merged_df.IssuerNumber == "L29753", "IssuerNumber"] = "641062"   # nestle

norway_bond_df.sort_values(by = ['MarketValueUSD'], ascending=False, inplace=True)
norway_bond_df = norway_bond_df.drop(columns = ['categ'])
norway_bond_df.to_csv(data_path + 'unmatched_norway_bonds.csv', index=False)

final_merged_df.to_csv(data_path + 'merged_norway_bonds.csv', index=False)
norway_bond_df.to_csv(data_path + 'unmatched_norway_bonds.csv', index=False)

print("Output the Merged Result to the Path: ", data_path + 'merged_norway_bonds.csv')
print("Output the Unmatched Records to the Path: ", data_path + 'unmatched_norway_bonds.csv')
    
print('Merged Secruities has the total Market Value of ', final_merged_df.MarketValueUSD.sum(), 'which is: ', final_merged_df.MarketValueUSD.sum() / sum_MV, 'of total Market Value we need to merge')
print('Total Number of the Merged Securities is ', final_merged_df.shape[0], 'which is: ', final_merged_df.shape[0]/NN, 'of total securities we need to merge')    
