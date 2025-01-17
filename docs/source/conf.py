# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys
sys.path.insert(0, os.path.abspath('../../src'))

autoapi_dirs = ['../../src']  # src 폴더 경로


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'YouTube Trend Analysis'
copyright = '2025, DE4-Final-Project'
author = 'DE4-Final-Project'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',    # 자동으로 docstring을 문서화
    'sphinx.ext.napoleon',   # Google 스타일과 NumPy 스타일 docstring 지원
]

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
html_theme_options = {
    'collapse_navigation': False,  # 사이드바 메뉴를 펼쳐서 표시
    'sticky_navigation': True,     # 사이드바가 스크롤을 따라가도록 설정
    'navigation_depth': 3,         # 사이드바 메뉴 깊이 설정
    'includehidden': True,         # 숨겨진 페이지 포함 여부
    'titles_only': False           # 제목만 표시 여부
}