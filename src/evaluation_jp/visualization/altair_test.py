# %%
import altair as alt
from vega_datasets import data
import ipywidgets
import altair_widgets
alt.renderers.enable("nteract")
source = data.gapminder_health_income.url

alt.Chart(source).mark_circle().encode(
    alt.X('income:Q', scale=alt.Scale(type='log')),
    alt.Y('health:Q', scale=alt.Scale(zero=False)),
    size='population:Q'
)


#%%
