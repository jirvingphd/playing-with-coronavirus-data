# try:
#     from jupyter_dash import JupyterDash
# except:
#     import os
#     os.system("conda install -c conda-forge -c plotly jupyter-dash")
#     from jupyter_dash import JupyterDash
    
from functions import *
corona_data = CoronaData(verbose=False,run_workflow=True)
df = corona_data.df_us.copy()

## IMPORTS
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output


def make_options(menu_choices):
    """Returns list of dictionary with {'label':menu_choice,'value':menu_choice}"""
    options = []
    for choice in menu_choices:
        options.append({'label':choice,'value':choice})
    return options

## Make Plot Cols list for options
stat_cols = ['Confirmed','Deaths','Recovered']
plot_cols = []
for column in stat_cols:
    plot_cols.extend([col for col in df.columns if column in col])

new_options = [{'label':'New Cases Only','value':1},
{'label':'Cumulative Cases','value':0}]



# Build App
# app = JupyterDash()
app = dash.Dash()

app.layout = html.Div([
    html.H1("Coronavirus Analysis"),
    html.Div([
    html.Div(id="menu",children=[        
            dcc.Dropdown(id='choose_states',multi=True,placeholder='Select States',
                        options= make_options(df['state'].sort_values().unique( )),
                        value=['MD','NY','TX','CA','AZ'],style={'width':'90%','display':'block'}),
            
            html.Div(id='case_type_menu',children=[
                dcc.Dropdown(id='choose_cases',multi=False,
                            placeholder='Select Case Type', 
                            options=make_options(plot_cols),
                            value='Confirmed', 
                            style={'width':'80%','display':'inline-block'}
                            ),
            
            
            dcc.RadioItems(id='choose_new',#multi=False,
                        options=new_options, value=0,#make_options(['True','False']),
                        style={'display':'inline-block'}
                        )], 
            style={'display':'inline-block','padding-top':'2em'})
        ],
        style={'border':"1px solid gray",
        "display":"block-inline","width":'90%'
        })
,
        
    dcc.Graph(id='graph')
    ], )],
        style={'width':'100%','display':'block','margin-left':'auto',
        "border":"2px solid black"})
    #"display":'block'})border":"2px solid blue",

@app.callback(Output('graph','figure'),[Input('choose_states','value'),
                                       Input('choose_cases','value'),
                                       Input('choose_new','value')])
def update_output_div(states,cases,new_only):
    if isinstance(states,list)==False:
        states = [states]
    if isinstance(cases,list)==False:
        cases = [cases]
#     if new_only=='True':
#         new_only=True
#     else:
#         new_only=False
    pfig = plot_states(df,states,plot_cols=cases,new_only=new_only)
    return pfig

app.run_server(debug=True)

# FLASK_APP=app