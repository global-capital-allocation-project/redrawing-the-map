# ---------------------------------------------------------------------------------------------------
# Network_Charts: This job generates the tax haven network charts (Figures 2-3 in the paper)
# Note that the figure annotations and labeling are done via post-processing in Adobe Illustrator
# ---------------------------------------------------------------------------------------------------

# Project path: alter this to reflect your system's folder structure
project_folder <- "<DATA_PATH>/cmns1/"
setwd(project_folder)

# Install the packages below as needed
# install.packages(c("ggpubr", "dplyr", "ggplot2", "alluvial", "ggforce", "ggalluvial", "ggparallel", "readxl", "stringr"))# 

# Load libraries
library("ggpubr")
library("dplyr")
library("ggplot2")
library("alluvial")
library("ggforce")
library("ggalluvial")
library("ggparallel")
library("readxl")
library("stringr")

# Global parameters
default_palette = c("#e41a1c", "#377eb8", "#4daf4a", "#984ea3", "#ff7f00")
alpha <- 0.7 # transparency value
fct_levels <- c("A","C","B", "D", "E")

# Plotting function
gen_chart = function(dat, title="", color_palette=default_palette) {

  dat_ggforce <- dat  %>%
    gather_set_data(1:2) %>%
    arrange(x,Destination,desc(Conduit))

  A_col = color_palette[1]
  B_col = color_palette[2]
  C_col = color_palette[3]
  D_col = color_palette[4]
  E_col = color_palette[5]
  
  out = ggplot(dat_ggforce, aes(x=x, id=id, split=y, value=freq)) +
    geom_parallel_sets(aes(fill = Destination), alpha = alpha, axis.width = 0.2,
                       n=100, strength = 0.6) +
    geom_parallel_sets_axes(axis.width = 0.25, fill = "gray96",
                            color = "gray80", size = 0.15) +
    geom_parallel_sets_labels(colour = 'gray35', size = 4.5, angle = 0) +
    scale_fill_manual(values  = c(A_col, B_col, C_col, D_col, E_col)) +
    scale_color_manual(values = c(A_col, B_col, C_col, D_col, E_col)) +
    theme_minimal() +
    theme(
      legend.position = "none",
      panel.grid.major = element_blank(),
      panel.grid.minor = element_blank(),
      axis.text.y = element_blank(),
      axis.text.x = element_text(size = 0, face = "bold"),
      axis.title.x  = element_blank(),
      plot.title = element_text(hjust = 0.5)
    ) +
    ggtitle(title)

  return(out)
}

### Area
scale = .9
H = 14 * scale
W = 16 * scale

### (A) Bonds - USA
dat = readxl::read_excel("temp/raw_data_for_network_plot_usa.xlsx")
dat$Conduit = str_replace_all(dat$Conduit, "\r", "")
bonds_us = gen_chart(dat, title="")
bonds_us
ggsave("graphs/network_flows_usa_bonds.pdf", width=W, height=H)

### (B) Bonds - EMU
dat = readxl::read_excel("temp/raw_data_for_network_plot_emu.xlsx")
dat$Conduit = str_replace_all(dat$Conduit, "\r", "")
bonds_emu = gen_chart(dat, title="")
bonds_emu
ggsave("graphs/network_flows_emu_bonds.pdf", width=W, height=H)
