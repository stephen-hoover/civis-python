#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Civis Client documentation build configuration file, created by
# sphinx-quickstart on Tue Sep 15 11:15:53 2015.
#
# This file is execfile()d with the current directory set to its
# containing dir.
#
# Note that not all possible configuration values are present in this
# autogenerated file.
#
# All configuration values have a default; values that are commented out
# serve to show the default.

import os
import datetime

import civis

# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.intersphinx', 'sphinx.ext.viewcode', 'sphinx.ext.autodoc',
    'sphinx.ext.autosummary', 'sphinx.ext.doctest', 'numpydoc.numpydoc'
]

autosummary_generate = True

intersphinx_mapping = {
    'pandas': ('http://pandas.pydata.org/pandas-docs/stable', None),
    'python': ('https://docs.python.org/3', None),
    'requests': ('https://requests.readthedocs.io/en/latest/', None),
    'sklearn': ('http://scikit-learn.org/stable', None),
    'joblib': ('https://joblib.readthedocs.io/en/latest/', None),
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
# source_suffix = ['.rst', '.md']
source_suffix = '.rst'

# The encoding of source files.
#source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = 'index'

# General information about the project.
current_year = datetime.datetime.now().year
project = 'Civis Client'
copyright = '2016-%d, Civis Analytics' % current_year
author = 'Civis Analytics'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
major_version, minor_version, _ = civis.__version__.split('.')
# The short X.Y version.
version = '%s.%s' % (major_version, minor_version)
# The full version, including alpha/beta/rc tags.
release = civis.__version__

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = None

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
#today = ''
# Else, today_fmt is used as the format for a strftime call.
#today_fmt = '%B %d, %Y'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = []

# The reST default role (used for this markup: `text`) to use for all
# documents.
#default_role = None

# If true, '()' will be appended to :func: etc. cross-reference text.
#add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
#add_module_names = True

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
#show_authors = False

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# A list of ignored prefixes for module index sorting.
#modindex_common_prefix = []

# If true, keep warnings as "system message" paragraphs in the built documents.
#keep_warnings = False

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False


# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
# If building on RTD, the theme is already included. The build will fail if we
# also include it here. Only add theme if building locally. See
# https://github.com/snide/sphinx_rtd_theme
_on_rtd = os.environ.get('READTHEDOCS', None) == 'True'
if not _on_rtd:
    import sphinx_rtd_theme
    html_theme = 'sphinx_rtd_theme'
    html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
#html_theme_options = {}

# Add any paths that contain custom themes here, relative to this directory.
#html_theme_path = []

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
#html_title = None

# A shorter title for the navigation bar.  Default is the same as html_title.
#html_short_title = None

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
html_logo = '_static/civis.png'

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
html_favicon = '_static/civis.ico'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Add any extra paths that contain custom files (such as robots.txt or
# .htaccess) here, relative to this directory. These files are copied
# directly to the root of the documentation.
#html_extra_path = []

# If not '', a 'Last updated on:' timestamp is inserted at every page bottom,
# using the given strftime format.
#html_last_updated_fmt = '%b %d, %Y'

# If true, SmartyPants will be used to convert quotes and dashes to
# typographically correct entities.
#html_use_smartypants = True

# Custom sidebar templates, maps document names to template names.
#html_sidebars = {}

# Additional templates that should be rendered to pages, maps page names to
# template names.
#html_additional_pages = {}

# If false, no module index is generated.
#html_domain_indices = True

# If false, no index is generated.
#html_use_index = True

# If true, the index is split into individual pages for each letter.
#html_split_index = False

# If true, links to the reST sources are added to the pages.
#html_show_sourcelink = True

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
#html_show_sphinx = True

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
#html_show_copyright = True

# If true, an OpenSearch description file will be output, and all pages will
# contain a <link> tag referring to it.  The value of this option must be the
# base URL from which the finished HTML is served.
#html_use_opensearch = ''

# This is the file name suffix for HTML files (e.g. ".xhtml").
#html_file_suffix = None

# Language to be used for generating the HTML full-text search index.
# Sphinx supports the following languages:
#   'da', 'de', 'en', 'es', 'fi', 'fr', 'h', 'it', 'ja'
#   'nl', 'no', 'pt', 'ro', 'r', 'sv', 'tr'
#html_search_language = 'en'

# A dictionary with options for the search language support, empty by default.
# Now only 'ja' uses this config value
#html_search_options = {'type': 'default'}

# The name of a javascript file (relative to the configuration directory) that
# implements a search results scorer. If empty, the default will be used.
#html_search_scorer = 'scorer.js'

# Output file base name for HTML help builder.
htmlhelp_basename = 'CivisClientdoc'

# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
# The paper size ('letterpaper' or 'a4paper').
#'papersize': 'letterpaper',

# The font size ('10pt', '11pt' or '12pt').
#'pointsize': '10pt',

# Additional stuff for the LaTeX preamble.
#'preamble': '',

# Latex figure (float) alignment
#'figure_align': 'htbp',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
  (master_doc, 'CivisClient.tex', 'Civis Client Documentation',
   'Civis Analytics', 'manual'),
]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
#latex_logo = None

# For "manual" documents, if this is true, then toplevel headings are parts,
# not chapters.
#latex_use_parts = False

# If true, show page references after internal links.
#latex_show_pagerefs = False

# If true, show URL addresses after external links.
#latex_show_urls = False

# Documents to append as an appendix to all manuals.
#latex_appendices = []

# If false, no module index is generated.
#latex_domain_indices = True


# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (master_doc, 'civisclient', 'Civis Client Documentation',
     [author], 1)
]

# If true, show URL addresses after external links.
#man_show_urls = False


# -- Options for Texinfo output -------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
  (master_doc, 'CivisClient', 'Civis Client Documentation',
   author, 'CivisClient', 'One line description of project.',
   'Miscellaneous'),
]

# Documents to append as an appendix to all manuals.
#texinfo_appendices = []

# If false, no module index is generated.
#texinfo_domain_indices = True

# How to display URL addresses: 'footnote', 'no', or 'inline'.
#texinfo_show_urls = 'footnote'

# If true, do not generate a @detailmenu in the "Top" node's menu.
#texinfo_no_detailmenu = False

nitpick_ignore = [
    ('envvar', 'CIVIS_API_KEY'),
    ('py:class', 'concurrent.futures._base.Future'),
    ('py:class', 'civis.base.CivisAsyncResultBase')
]
numpydoc_show_class_members = False


# Preserve signatures of a few methods wrapped with lru_cache. Need to set
# this before we import civis. See https://stackoverflow.com/a/28371786.
import functools
import inspect

def noop_lru_cache(*args, **kwargs):
    def wrapper(func):
        def f(*args, **kwargs):
            return func(*args, **kwargs)
        f.__doc__ = func.__doc__
        f.__signature__ = inspect.signature(func)
        return f
    return wrapper
functools.lru_cache = noop_lru_cache


# Special functionality to accommodate autogenerated client.
def _make_attr_docs(class_names, module_path):
    doc = '\n    Attributes\n    ----------\n'
    doc_fmt = '    {}\n        An instance of the {} endpoint\n'
    for name in class_names:
        ref_link = ':class:`~{}.{}`'.format(module_path, name.title())
        doc += doc_fmt.format(name, ref_link)
    return doc


def _attach_classes_to_module(module, class_data):
    for class_name, cls in class_data.items():
        setattr(module, class_name.title(), cls)


_autodoc_fmt = ('.. autoclass:: {}\n'
                '   :members:\n'
                '   :exclude-members: __init__\n\n'
                '   .. rubric:: Methods\n'
                '   .. generatedautosummary:: {}\n\n')


def _write_resources_rst(class_names, filename, civis_module):
    with open(filename, 'w') as _out:
        _out.write('API Resources\n=============\n\n')
        for class_name in class_names:
            name = class_name.title()
            header = '`{}`\n{}\n'.format(name, '"' * (len(name) + 2))
            full_path = '.'.join((civis_module, name))
            _out.write(header)
            _out.write(_autodoc_fmt.format(full_path, full_path))


import civis
_generated_attach_point = civis.resources._resources
_generated_attach_path = 'civis.resources._resources'
_rst_basename = 'api_resources.rst'
_test_build = os.getenv('TRAVIS') == 'true' or _on_rtd

if _test_build:
    import json
    from collections import OrderedDict
    from jsonref import JsonRef

    this_dir = os.path.dirname(os.path.realpath(__file__))
    test_dir = os.path.join(this_dir, os.pardir, os.pardir, 'civis', 'tests')
    api_path = os.path.join(test_dir, 'civis_api_spec.json')
    with open(api_path) as _raw:
        api_spec = JsonRef.replace_refs(
            json.load(_raw, object_pairs_hook=OrderedDict))
    extra_classes = civis.resources._resources.parse_api_spec(
        api_spec, '1.0', 'base')
else:
    api_key = os.environ.get("CIVIS_API_KEY")
    user_agent = "civis-python/SphinxDocs"
    api_version = "1.0"
    extra_classes = civis.resources._resources.generate_classes(
        api_key=api_key, api_version=api_version)
sorted_class_names = sorted(extra_classes.keys())

civis.APIClient.__doc__ += _make_attr_docs(sorted_class_names,
                                           _generated_attach_path)
_attach_classes_to_module(_generated_attach_point, extra_classes)
_write_resources_rst(sorted_class_names, _rst_basename, _generated_attach_path)


# The following directive makes all (public) methods of an Endpoint
# auto-discoverable. See https://stackoverflow.com/a/30783465
from sphinx.ext.autosummary import Autosummary, get_documenter
from sphinx.util.inspect import safe_getattr

class GeneratedAutosummary(Autosummary):
    """Helper for documenting all methods of an auto-generated endpoint."""

    required_arguments = 1

    @staticmethod
    def get_members(obj, typ, public_only=True):
        items = []
        for name in dir(obj):
            if public_only and name.startswith('_'):
                continue
            try:
                documenter = get_documenter(None, safe_getattr(obj, name), obj)
            except AttributeError:
                continue
            if documenter.objtype == typ:
                items.append(name)
        return items

    def run(self):
        input_name = self.arguments[0]
        module_name, class_name = input_name.rsplit('.', 1)
        module = __import__(module_name, globals(), locals(), [class_name])
        klass = getattr(module, class_name)
        methods = self.get_members(klass, 'method')
        self.content = ['~{}.{}'.format(input_name, method)
                        for method in methods]
        return super().run()


# We fake-attached all of the autogenerated classes to
# civis.resources._resources, but we don't want them to appear in the docs with
# that as the module name (since they can't actually be imported from there).
# There is a global `add_module_name` parameter that we want to set to False
# for the autogenerated classes only. This needs to be done at the `Domain`
# level since that is where the rst is actually parsed into nodes.
from sphinx.domains.python import PythonDomain, PyClasslike

_extra_class_titles = [klass.title() for klass in extra_classes.keys()]

class MaybeHiddenModulePyClasslike(PyClasslike):

    def handle_signature(self, sig, signode):
        add_module_orig = self.env.config.add_module_names
        # Is this an autogenerated class?
        toggle = (any(sig.startswith(title) for title in _extra_class_titles)
                  and 'session' in sig and 'return_type' in sig)
        if toggle:
            self.env.config.add_module_names = False
        result = super().handle_signature(sig, signode)
        self.env.config.add_module_names = add_module_orig
        return result

PythonDomain.directives['class'] = MaybeHiddenModulePyClasslike


def setup(app):
    app.add_directive('generatedautosummary', GeneratedAutosummary)
