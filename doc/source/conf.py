from typing import List

# -- Project information -----------------------------------------------------

project = "pantab"
copyright = "2019-2024, Will Ayd, innobi, LLC"
author = "Will Ayd, innobi, LLC"
release = "4.1.0"


# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx_rtd_theme",
]

templates_path = ["_templates"]
exclude_patterns: List[str] = []


# -- Options for HTML output -------------------------------------------------

html_theme = "sphinx_rtd_theme"
master_doc = "index"  # see RTD GH issue 2569
html_context = {
    "display_github": True,
}

html_static_path = ["_static"]
