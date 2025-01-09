from flask import Flask, render_template, send_from_directory
import os
import pandas as pd
from data_visualize import create_pie_chart, create_bar_chart, create_toggleable_pie_charts, create_toggleable_bar_charts
from uk_exploration_update import prepare_grouped_data
from uk_wrangle import get_year, process_address, process_country

# Create Flask app
app = Flask(__name__)

# Route for main dashboard
@app.route('/')
def dashboard():
    # Generate data and visualizations dynamically
    # Example data processing and chart generation
    data = pd.DataFrame({
        'Category': ['A', 'B', 'C', 'D'],
        'Values': [10, 20, 30, 40]
    })

    pie_chart = create_pie_chart(data, 'Category', 'Values', 'Example Pie Chart')
    bar_chart = create_bar_chart(data, 'Category', 'Values', 'Example Bar Chart')

    pie_html = pie_chart.to_html(full_html=False, include_plotlyjs=False)
    bar_html = bar_chart.to_html(full_html=False, include_plotlyjs=False)

    html_template = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Company Analysis Dashboard</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body {{
                padding: 20px;
            }}
            .chart-container {{
                margin-bottom: 40px;
            }}
            .chart-container h3 {{
                margin-bottom: 20px;
            }}
            .chart {{
                width: 100%;
                max-width: 1000px;
                margin: 0 auto;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1 class="text-center mb-5">Company Analysis Dashboard</h1>
            <div class="chart-container">
                <h3 class="text-center">Pie Chart Example</h3>
                <div class="chart">{pie_html}</div>
            </div>
            <div class="chart-container">
                <h3 class="text-center">Bar Chart Example</h3>
                <div class="chart">{bar_html}</div>
            </div>
        </div>
    </body>
    </html>
    """

    # Write HTML to a file for static serving
    html_file_path = 'static/dashboard.html'
    with open(html_file_path, 'w') as file:
        file.write(html_template)

    return send_from_directory('static', 'dashboard.html')

# Route to serve static assets (if needed)
@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

# Main entry point for running the app
if __name__ == '__main__':
    # Ensure the static directory exists
    if not os.path.exists('static'):
        os.makedirs('static')

    # Run the Flask app
    app.run(debug=True)
