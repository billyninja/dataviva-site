#!/bin/bash
# python -m scripts.rais.step_1_aggregate -y $1
# # python -m scripts.rais.step_2_disaggregate -y $1
# python -m scripts.rais.step_3_required -y $1 -d
python -m scripts.rais.step_4_importance -y $1
python -m scripts.rais.step_5_diversity -y $1
python -m scripts.rais.step_6_rca_dist_opp -y $1
python -m scripts.rais.step_7_growth -y $1 -t yb
python -m scripts.rais.step_7_growth -y $1 -t ybi
python -m scripts.rais.step_7_growth -y $1 -t ybio
python -m scripts.rais.step_7_growth -y $1 -t ybo
python -m scripts.rais.step_7_growth -y $1 -t yi
python -m scripts.rais.step_7_growth -y $1 -t yio
python -m scripts.rais.step_7_growth -y $1 -t yo