# %%
import numpy as np
import pandas as pd
import altair as alt

alt.renderers.enable("notebook")

from bokeh.layouts import layout, widgetbox

from bokeh.io import show, output_notebook, export_png

from bokeh.models import (Text, Plot, Label, Slider, ColumnDataSource, SingleIntervalTicker, )
from bokeh.models.annotations import Label, LabelSet

from bokeh.palettes import Spectral7

from bokeh.plotting import figure, reset_output
from bokeh.resources import CDN
from bokeh.embed import file_html
from bokeh.transform import linear_cmap



# %%
df = pd.read_csv(
    "\\\\cskma0294\\F\\Evaluations\\JobPath\\Python\\Data\\Results\\Rdf.csv", 
)
drop_cols = [col for col in list(df) if "." in col]
drop_cols.append("w_earn_per_WIES_2017")
df = df.drop(drop_cols, axis="columns")
df["clust_size"] = df["clust_size"]/3
# %%
year_list = [x for x in range(2015, 2018)]
years = {}
years_t = {}
years_c = {}
shared_cols = ["Group", "cluster", "cluster_abb", "clust_size"]
col_list = list(df)
for year in year_list:
    year_cols = [col for col in col_list if str(year) in col]
    this_year_cols = shared_cols + year_cols
    years[year] = df[this_year_cols]
    year_col_map = {col: col.split("_")[1] for col in year_cols}
    years[year] = years[year].rename(columns=year_col_map)
    years_t[year] = years[year].loc[years[year]["Group"] == 1]
    years_c[year] = years[year].loc[years[year]["Group"] == 0]
# js_source_array = str(dict_of_sources).replace("'", "")

# %%
for year in year_list:
    print("------------------------------")
    print(year)
    print("------------------------------")
    print(years_t[year].T)
    print(years_c[year].T)
    reset_output()
    output_notebook()

    source_t = ColumnDataSource(years_t[year])
    if year == 2015:    
        source_c = source_t
    else:
        source_c = ColumnDataSource(years_c[year])
    print(source_t.data)
    print(source_c.data)

    plot = figure(x_range=(6000, 15000), y_range=(0, 9000), plot_height=600, plot_width=600)
    plot.xaxis.ticker = SingleIntervalTicker(interval=1000)
    plot.xaxis.axis_label = "Social Welfare income"
    plot.yaxis.ticker = SingleIntervalTicker(interval=1000)
    plot.yaxis.axis_label = "Employment earnings"

    label = Label(
        x=6050,
        y=10,
        text=str(year),
        text_font_size="70pt",
        text_color="#eeeeee",
        # level=0,
    )
    plot.add_layout(label)

    plot.circle(
        x="swpay",
        y="earn",
        source=source_c,
        radius="clust_size",
        fill_color=linear_cmap("cluster", palette=Spectral7, low=0, high=6),
        alpha=0.3,
    )
    
    plot.circle(
        x="swpay",
        y="earn",
        source=source_t,
        radius="clust_size",
        fill_color=linear_cmap("cluster", palette=Spectral7, low=0, high=6),
        # legend="cluster name",
        alpha=0.9,
    )

    labels_t = LabelSet(
        x="swpay",
        y="earn",
        source=source_t,
        text="cluster",
        level="glyph",
        text_align="center",
        y_offset=5,
        render_mode="canvas",
        text_font_size="8pt",
    )
    labels_c = LabelSet(
        x="swpay",
        y="earn",
        source=source_c,
        text="cluster",
        level="glyph",
        text_align="center",
        y_offset=5,
        render_mode="canvas",
        text_font_size="8pt",
        text_alpha=0.5,
    )
    plot.add_layout(labels_t)
    plot.add_layout(labels_c)

    
    show(plot)
    export_png(plot, f"earn_sw_{year}.png")

# #%%
# # Mark circle centres
# # Mark uncertainty perimeter?
# # 2013-2015 average, 2016, 2017


# #%%


#%%
