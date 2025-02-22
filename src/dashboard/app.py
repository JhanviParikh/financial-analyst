from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import os
from datetime import datetime, timedelta
import json
from src.config.settings import ANALYTICS_PATH

# Initialize the Dash app
app = Dash(__name__, title="Financial Risk Management Dashboard")

# Layout with modern styling
app.layout = html.Div([
    # Header
    html.Div([
        html.H1("Financial Risk Management Dashboard",
                style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': 20}),
        html.Div([
            html.Div(id='total-transactions', className='metric-card'),
            html.Div(id='suspicious-transactions', className='metric-card'),
            html.Div(id='average-amount', className='metric-card'),
        ], style={'display': 'flex', 'justifyContent': 'space-around', 'marginBottom': 30}),
    ], style={'padding': '20px', 'backgroundColor': '#f8f9fa'}),

    # Main content
    html.Div([
        # Left column
        html.Div([
            dcc.Graph(id='transactions-timeline'),
            dcc.Graph(id='risk-heatmap'),
        ], style={'width': '60%', 'display': 'inline-block', 'padding': '20px'}),

        # Right column
        html.Div([
            dcc.Graph(id='transaction-types'),
            dcc.Graph(id='location-scatter'),
        ], style={'width': '40%', 'display': 'inline-block', 'padding': '20px'}),
    ], style={'display': 'flex'}),

    # Update interval
    dcc.Interval(
        id='interval-component',
        interval=2*1000,  # Update every 2 seconds (was 5)
        n_intervals=0
    )
], style={'backgroundColor': '#ffffff', 'fontFamily': 'Arial'})

def load_latest_data():
    """Load and process the latest transaction data"""
    try:
        # Ensure analytics directory exists
        if not os.path.exists(ANALYTICS_PATH):
            print(f"Analytics directory does not exist: {ANALYTICS_PATH}")
            return pd.DataFrame()
            
        # Read the latest analytics data
        analytics_files = sorted([f for f in os.listdir(ANALYTICS_PATH) if f.endswith('.csv')])
        print(f"Found analytics files: {analytics_files}")
        
        if not analytics_files:
            print("No analytics files found")
            return pd.DataFrame()
        
        # Load the last 5 files to show more history
        latest_files = analytics_files[-5:]
        dfs = []
        
        for file in latest_files:
            try:
                file_path = os.path.join(ANALYTICS_PATH, file)
                if not os.path.exists(file_path):
                    print(f"File does not exist: {file_path}")
                    continue
                
                # Read CSV
                df = pd.read_csv(file_path, low_memory=False)
                
                # If risk_score is not present, calculate it from other columns
                if 'risk_score' not in df.columns:
                    df['risk_score'] = 0.2  # Base risk
                    # High risk for large amounts during night hours
                    df.loc[(df['amount'] > 5000) & (df['hour_of_day'].isin([0,1,2,3,4,5,23])), 'risk_score'] = 0.9
                    # High risk for very large amounts
                    df.loc[df['amount'] > 8000, 'risk_score'] = 0.8
                    # Medium-high risk for risky locations
                    df.loc[df['location_risk_score'] > 0.7, 'risk_score'] = 0.7
                    # Medium risk for suspicious timing
                    df.loc[df['time_risk_score'] > 0.7, 'risk_score'] = 0.6
                
                # Calculate suspicious flag
                df['is_suspicious'] = (df['risk_score'] > 0.7).astype(int)
                
                # Extract latitude and longitude if not present
                if 'latitude' not in df.columns and 'location' in df.columns:
                    try:
                        df[['latitude', 'longitude']] = df['location'].str.split(',', expand=True).astype(float)
                    except:
                        df['latitude'] = 0
                        df['longitude'] = 0
                
                print(f"Successfully loaded {file} with columns: {df.columns.tolist()}")
                dfs.append(df)
                
            except Exception as e:
                print(f"Error reading file {file}: {e}")
                continue
        
        if not dfs:
            print("No valid data files found")
            return pd.DataFrame()
            
        return pd.concat(dfs, ignore_index=True)
        
    except Exception as e:
        print(f"Error loading data: {e}")
        return pd.DataFrame()

@app.callback(
    [Output('total-transactions', 'children'),
     Output('suspicious-transactions', 'children'),
     Output('average-amount', 'children'),
     Output('transactions-timeline', 'figure'),
     Output('risk-heatmap', 'figure'),
     Output('transaction-types', 'figure'),
     Output('location-scatter', 'figure')],
    [Input('interval-component', 'n_intervals')]
)
def update_dashboard(n):
    df = load_latest_data()
    
    if df.empty:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="No data available",
            xaxis_title="",
            yaxis_title=""
        )
        return [
            html.Div([html.H4("Total Transactions"), html.P("0")]),
            html.Div([html.H4("Suspicious Transactions"), html.P("0")]),
            html.Div([html.H4("Average Amount"), html.P("$0.00")]),
            empty_fig,
            empty_fig,
            empty_fig,
            empty_fig
        ]

    # Calculate metrics
    total_trans = len(df)
    suspicious_trans = df['is_suspicious'].sum()  # Use the new column
    avg_amount = df['amount'].mean()

    # Create timeline
    df['minute'] = pd.to_datetime(df['timestamp']).dt.floor('min')
    timeline_data = df.groupby('minute').agg({
        'transaction_id': 'count',
        'is_suspicious': 'sum'
    }).reset_index()
    
    timeline = go.Figure()
    timeline.add_trace(go.Scatter(
        x=timeline_data['minute'],
        y=timeline_data['transaction_id'],
        name='All Transactions',
        line=dict(color='blue')
    ))
    timeline.add_trace(go.Scatter(
        x=timeline_data['minute'],
        y=timeline_data['is_suspicious'],
        name='Suspicious',
        line=dict(color='red')
    ))
    timeline.update_layout(
        title=f'Transaction Volume Over Time (Total: {total_trans:,})',
        yaxis_title="Number of Transactions",
        xaxis_title="Time"
    )

    # Create risk heatmap
    risk_columns = ['amount_threshold_flag', 'time_risk_score', 'location_risk_score', 'risk_score']
    available_risk_columns = [col for col in risk_columns if col in df.columns]
    if available_risk_columns:
        corr_matrix = df[available_risk_columns].corr()
        heatmap = px.imshow(
            corr_matrix,
            title='Risk Factor Correlation',
            labels=dict(color="Correlation"),
            color_continuous_scale='RdBu'
        )
    else:
        heatmap = go.Figure()
        heatmap.update_layout(title="No risk data available")

    # Create transaction type distribution
    if 'transaction_type' in df.columns:
        type_counts = df['transaction_type'].value_counts()
        types_pie = px.pie(
            values=type_counts.values,
            names=type_counts.index,
            title=f'Transaction Types Distribution (Last {len(df)} Transactions)'
        )
    else:
        types_pie = go.Figure()
        types_pie.update_layout(title="No transaction type data available")

    # Create location scatter plot
    if 'latitude' in df.columns and 'longitude' in df.columns:
        location_scatter = px.scatter(
            df,
            x='longitude',
            y='latitude',
            color='risk_score',
            size='amount',
            hover_data=['transaction_type', 'amount', 'risk_score'],
            title='Transaction Locations',
            color_continuous_scale='RdYlBu_r',  # Red for high risk, blue for low risk
            size_max=30
        )
        location_scatter.update_layout(
            coloraxis_colorbar_title="Risk Score"
        )
    else:
        location_scatter = go.Figure()
        location_scatter.update_layout(title="No location data available")

    return [
        html.Div([html.H4("Total Transactions"), html.P(f"{total_trans:,}")]),
        html.Div([html.H4("Suspicious Transactions"), html.P(f"{suspicious_trans:,}")]),
        html.Div([html.H4("Average Amount"), html.P(f"${avg_amount:,.2f}")]),
        timeline,
        heatmap,
        types_pie,
        location_scatter
    ]

# Add custom CSS
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            .metric-card {
                background-color: white;
                border-radius: 8px;
                padding: 20px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                text-align: center;
                min-width: 200px;
            }
            .metric-card h4 {
                margin: 0;
                color: #666;
                font-size: 14px;
            }
            .metric-card p {
                margin: 10px 0 0;
                color: #2c3e50;
                font-size: 24px;
                font-weight: bold;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

if __name__ == '__main__':
    app.run_server(debug=True) 