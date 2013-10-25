#!/bin/bash
python -m scripts.rais.step_1_aggregate -y $1
python -m scripts.rais.step_2_disaggregate -y $1
python -m scripts.rais.step_3_required -y $1
python -m scripts.rais.step_4_importance -y $1
python -m scripts.rais.step_5_unique -y $1
python -m scripts.rais.step_6_rca_dist_opp -y $1
# python -m scripts.rais.step_7_growth -y $1 -i yb
# python -m scripts.rais.step_7_growth -y $1 -i ybi
# python -m scripts.rais.step_7_growth -y $1 -i ybio
# python -m scripts.rais.step_7_growth -y $1 -i ybo
# python -m scripts.rais.step_7_growth -y $1 -i yi
# python -m scripts.rais.step_7_growth -y $1 -i yio
# python -m scripts.rais.step_7_growth -y $1 -i yo