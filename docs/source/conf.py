# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
sys.path.insert(0, os.path.abspath('../../'))

project = 'CFAPyX Package'
copyright = ('2022-2024, Centre of Environmental Data Analysis Developers,'
             'Scientific and Technical Facilities Council (STFC),'
             'UK Research and Innovation (UKRI). '
             'BSD 2-Clause License. All rights reserved')
author = 'Daniel Westwood'
release = '2024.8.9'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.viewcode'
]

templates_path = ['_templates']
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

html_logo = '_images/CFAPyX_long.png'
html_favicon = '_images/CFAPyX.ico'
html_a1 = '_images/ceda.png'
html_a2 = '_images/ncas.png'
html_a3 = '_images/CFAPyX_nobg.png'
