from typing import List

# -- Project information -----------------------------------------------------

project = "pantab"
copyright = "2019-2024, Will Ayd, innobi, LLC"
author = "Will Ayd, innobi, LLC"
release = "5.1.0"


# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx_rtd_theme",
    "sphinxext.opengraph",
]

templates_path = ["_templates"]
exclude_patterns: List[str] = []


# -- Options for HTML output -------------------------------------------------

html_theme = "sphinx_rtd_theme"
html_logo = "../../misc/pantab_logo.svg"
master_doc = "index"  # see RTD GH issue 2569
html_context = {
    "display_github": True,
}

html_static_path = ["_static"]

# -- Options for opengraph output --------------------------------------------

ogp_site_url = "https://pantab.readthedocs.io/"
ogp_use_first_image = False
ogp_image = "https://pantab.readthedocs.io/en/latest/_static/pantab_logo.png"
