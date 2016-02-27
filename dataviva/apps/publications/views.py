# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, g
from dataviva.apps.general.views import get_locale

mod = Blueprint('publications', __name__,
                template_folder='templates',
                url_prefix='/<lang_code>/publications')


@mod.url_value_preprocessor
def pull_lang_code(endpoint, values):
    g.locale = values.pop('lang_code')


@mod.url_defaults
def add_language_code(endpoint, values):
    values.setdefault('lang_code', get_locale())


@mod.route('/')
def index():

    # request.GET('page')
    # request.GET('query')

    articles = [1, 2, 3, 4, 5]
    themes = [1, 2, 3, 4, 5]

    return render_template(
        'publications/index.html',
        articles=articles,
        themes=themes
    )


@mod.route('/article')
def article():

    themes = [1, 2, 3, 4, 5]

    return render_template(
        'publications/article.html',
        themes=themes,
        article={},
    )
