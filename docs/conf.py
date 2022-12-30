# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import mpservice

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'mpservice'
copyright = '2020-, Zepu Zhang'
author = 'Zepu Zhang'
version = str(mpservice.__version__)

today_fmt = '%b %d %Y'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# See numpydoc documentation for a numpy-style docstring style guide.

extensions = [
    "numpydoc",
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    ]

# Disable autosummary stuff, which is enabled by numpydoc by default.
numpydoc_show_class_members = False
numpydoc_show_inherited_class_members = False


autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'special-members': '__init__, __getitem__, __iter__, __next__, __len__, __enter__, __exit__',
    'member-order': 'bysource',
    'show-inheritance': True,
}
autodoc_class_signature = 'separated'
autodoc_typehints = 'signature'

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', 'links.rst']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# Interesting styles to consider:
#  toc panel on left
#   bizstyle
#   pyramid
#   nature
#  toc panel on right
#   sphinxdoc
#   furo
#  no toc panel
#   scrolls  (good for very small, single-page doc)
html_theme = 'pydata_sphinx_theme'

html_theme_options = {
    'github_url': 'https://github.com/zpz/mpservice',
}


html_static_path = ['_static']


# make rst_epilog a variable, so you can add other epilog parts to it
rst_epilog = ""
# Read all link targets from file
with open('links.rst') as f:
     rst_epilog += f.read()