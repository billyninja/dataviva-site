# -*- coding: utf-8 -*-
from flask import Blueprint, g, jsonify, request, render_template
from dataviva.apps.general.views import get_locale

mod = Blueprint('publications',
                __name__,
                url_prefix='/<lang_code>/publications')


@mod.url_value_preprocessor
def pull_lang_code(endpoint, values):
    g.locale = values.pop('lang_code')


@mod.url_defaults
def add_language_code(endpoint, values):
    values.setdefault('lang_code', get_locale())


@mod.route('/', methods=['GET'])
def index():
    return render_template("publications/index.html")
