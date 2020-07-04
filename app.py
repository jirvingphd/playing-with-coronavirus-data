# try:
#     from jupyter_dash import JupyterDash
# except:
#     import os
#     os.system("conda install -c conda-forge -c plotly jupyter-dash")
#     from jupyter_dash import JupyterDash

## PLOTLY IMPORTS/PARAMS
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
pio.templates.default = "plotly_dark"

## Acitvating Cufflinks
import cufflinks as cf
cf.go_offline()
cf.set_config_file(sharing='public',theme='solar',offline=True)

## Importing Dash
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output


## Load Functions and Data
from functions import *#CoronaData,plot_states,get_state_ts

corona_data = CoronaData(verbose=False,run_workflow=True)
df = corona_data.df_us.copy()





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
app = dash.Dash(__name__)
server = app.server




app.layout = html.Div(id='outerbox',children=[
    html.H1("Coronavirus Cases - By State"),
           
        html.Div(id='app',
        children=[
            html.Div(id="menu",
                    children=[
                        html.H2("Select Case Types and States"),

                        html.Div(id='case_type_menu', 
                                children=[
                                    dcc.RadioItems(id='choose_new',
                                                    options=new_options,
                                                    value=0),
                                    dcc.Dropdown(id='choose_cases',multi=False,
                                                placeholder='Select Case Type', 
                                                options=make_options(plot_cols),
                                                value='Confirmed'),#]),
                        dcc.Dropdown(id='choose_states',
                                    multi=True,
                                    placeholder='Select States', 
                                    options= make_options(df['state'].sort_values().unique( )),
                                    value=['MD','NY','TX','CA','AZ'])])
                    ]),
            dcc.Graph(id='graph')
        ])
        
        ])


@app.callback(Output('graph','figure'),[Input('choose_states','value'),
                                       Input('choose_cases','value'),
                                       Input('choose_new','value')])
def update_output_div(states,cases,new_only):
    if isinstance(states,list)==False:
        states = [states]
    if isinstance(cases,list)==False:
        cases = [cases]

    pfig = plot_states(df,states,plot_cols=cases,new_only=new_only)
    return pfig


FLASK_APP=app

# if __name__=='main':
# app.run_server(debug=True)
if __name__ == '__main__':
    app.run_server(debug=True)
    
