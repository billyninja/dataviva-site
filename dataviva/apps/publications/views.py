# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, g
from dataviva.apps.general.views import get_locale

mod = Blueprint('publications', __name__,
                template_folder='templates',
                url_prefix='/<lang_code>/publications')


def _build_paginator(count, per_page):
    import math
    n_pages = math.ceil(float(count)/float(per_page))

    return {
        "pages": [{
            "n": (p + 1),
            "limit": per_page,
            "offset": (per_page * p)} for p in range(int(n_pages))]
    }


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

    articles = range(56)
    themes = [1, 2, 3, 4, 5]

    paginator = _build_paginator(len(articles), per_page=20)

    return render_template(
        'publications/index.html',
        articles=articles,
        paginator=paginator,
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


@mod.route('/publish', methods=["GET"])
def publish():

    themes = [1, 2, 3, 4, 5]

    return render_template(
        'publications/publish.html',
        themes=themes,
    )
