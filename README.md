Replication Code for Coppola, Maggiori, Neiman, and Schreger: "Redrawing the Map of Global Capital Flows", Forthcoming (2021) at the Quarterly Journal of Economics
==============

Antonio Coppola, Harvard University (`acoppola@g.harvard.edu`)  
Matteo Maggiori, Stanford University Graduate School of Business (`maggiori@stanford.edu`)  
Brent Neiman, University of Chicago Booth School of Business (`brent.neiman@chicagobooth.edu`)  
Jesse Schreger, Columbia Business School (`jesse.schreger@columbia.edu`)  

*Note:* It is not recommended to download this repository as a ZIP archive using the "Download ZIP" button on GitHub. This is because when building the ZIP archive, GitHub does not include large files stored on Git LFS (Large File System) in full, but rather only pointers to them. To download the repository, please clone it via command line or using the GitHub desktop app. Alternatively, if you would like to download it as a ZIP file, please use the "ZIP" download link at [globalcapitalallocation.com/data](https://www.globalcapitalallocation.com/data).

I. INTRODUCTION
--------------

This README describes the overall structure of the replication package for this paper. The uppermost directory of the replication folder contains the following objects:

1. `README.md` (the file you are reading right now)
2. `README.pdf` (the file you are reading right now)
3. `CMNS_Data_Guide.pdf`
4. `aggregation_data_sources` (a folder)
5. `main_analysis` (a folder)
6. `cmns_aggregation` (a folder)
7. `data.zip` (a compressed folder)
8. `CMNS_Master.sh`
9. `CMNS_Master.do`

II. STRUCTURE OF THE CODE
--------------

`CMNS_Master.sh`, found in the uppermost directory of this repository, is the primary executable script, which runs the replication from start to finish. It does this by calling the `CMNS_Master.do` file, written in Stata, which in turn runs the various steps of the code and also defines project globals. Since a few of the steps are written in Python and R as well, `CMNS_Master.sh` also calls the Python and R executables in order to run these. The individual jobs are split into three folders, which are executed in the following sequence:

1. The folder `aggregation_sources` contains jobs that read in security- and issuer-level information from various commercial data sources that are used as inputs into our aggregation algorithm, which associates the universe of traded equity and debt securities with their issuers' ultimate parents, including those issued in tax havens.

2. The folder `cmns_aggregation` contains files that implement our aggregation procedure. They take as input the processed files produced by the jobs in `aggregation_sources` (whose structure is described in the script `cmns_aggregation/UP_Aggregation.py`). This code can also be used in a stand-alone manner by researchers interested purely in our aggregation procedure. To that end, the script `cmns_aggregation/UP_Aggregation.py` provides configurable parameters that allow to bypass certain source files if these are missing, giving a user the option of only using a subset of the commercial data sources imported in the previous steps in their own research. For the purpose of replicating our paper, however, all input sources should be used. We also recommend that stand-alone users of the aggregation code employ all the aggregation sources for their own projects if possible, since the quality of the final mapping will deteriorate as sources are removed.

3. The folder `main_analysis` contains files that implement the rest of the analysis in our paper, which includes building the rest of our data sources and producing the figures and tables in the paper. The following files in `main_analysis` reproduce the tables and figures in the paper:

    - Table 1:        `Reallocation_Matrices.do`
    - Tables 2-5:     `Restatement_Tables.do`
    - Tables 6-7:     `Firm_Level_Tables.do`
    - Tables 8-9:     `Restatement_Tables.do`
    - Figure 1:       `Cross_Country_Graphs.do`
    - Figures 2-3:    `Network_Charts.R`
    - Figure 4:       `BRICS_Times_Series.py`
    - Figure 5:       `Cross_Country_Graphs.do`
    - Figure 6:       Separately-produced illustration, no .do file
    - Figures 7-8:    `China_VIE_NFA.do`
    - Figure 9:       `Representativeness_Analysis.do`
    - Figure 10:      `Home_Bias.do`
    - Figure 11:      `Sales_Analysis.do`


III. EXECUTING THE CODE
--------------

The bash script `CMNS_Master.sh` is the main executable file for the procedure. Launching `CMNS_Master.sh` will 
run the full replication. This file should be called as:

    bash CMNS_Master.sh


IV. TECHNICAL NOTES
--------------

  - The code is built for Unix systems and assumes that the Stata, R, and Python interpreters are configured
    on your executable path. The required versions are version 15+ for Stata, version 3.6+ for Python, and version 4.0.2+ for R. Packages may need to be installed using a package manager (e.g. pip) as necessary.

  - Prior to running the procedure, please be sure to perform a find-and-replace in the code folder for
    the following expressions. These are user- and system-specific procedure parameters that will need
    to be filled in accordingly. Individual files also point out these parameters whenever
    they are present:
		
		<CODE_PATH>: Path to the copy of this code repository on the host system
		<DATA_PATH>: Path to the folder containing the data, in which the procedure is executed
   		<API_KEY>: If using the script in `FIGI_Download.py`, an OpenFIGI API key is required. This should be
                   obtained directly from OpenFIGI and used in place of this placeholder
    
V. COMMERCIALLY AVAILABLE DATA
--------------

Most of the data used in this project are commercially available, so cannot be included in this replication packet. The file `CMNS_Data_Guide.pdf` gives an overview of all input files used. In the cases in which they are public, the actual files can be found in `data.zip`. For some private data files, in the cases in which the structure of the data might otherwise be ambiguous, the `data.zip` archive also contains sample version of the files that only include sample rows: these are provided in order to demonstrate the structure of the data. Given there are a large number of private data files that are required to run the code, we acknowledge that most readers of this packet will not use the code to literally replicate our results, but rather can look to the code to understand what calculations we made for each result.
