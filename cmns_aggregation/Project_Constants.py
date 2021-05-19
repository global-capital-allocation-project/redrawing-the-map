# ---------------------------------------------------------------------------------------------------
# Ultimate parent aggregation analysis; Project constants
# ---------------------------------------------------------------------------------------------------
import socket
import getpass

# Project paths (change as needed)
host = socket.gethostname()
user = getpass.getuser()
data_path = "<DATA_PATH>/cmns1"

# List of countries classified as tax havens for algorithm purposes
tax_havens = [
    "ABW", "AIA", "AND", "ANT", "ATG", "BHR", "BHS", "BLZ", "BMU", 
    "BRB", "COK", "CRI", "CUW", "CYM", "CYP", "DJI", "DMA", "FSM", 
    "GGY", "GIB", "GRD", "HKG", "IMN", "JEY", "JOR", "KNA", "LBN", 
    "LBR", "LCA", "LIE", "LUX", "MAC", "MAF", "MCO", "MDV", "MHL", 
    "MLT", "MSR", "MUS", "NIU", "NRU", "PAN", "SMR", "SYC", "TCA", 
    "TON", "VCT", "VGB", "VUT", "WSM", "SGP"
]
