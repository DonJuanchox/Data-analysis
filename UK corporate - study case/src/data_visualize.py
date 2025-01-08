"""
Creating vizualizations with plotly library
Add *kwargs arguments to create_pie_chart and create_bar_chart
"""
import plotly.express as px
import plotly.graph_objects as go

# Function to create pie chart
def create_pie_chart(data, names_col, values_col, title, category_orders=None):
    return px.pie(
        data,
        names=names_col,
        values=values_col,
        title=title,
        width=1000,
        height=700,
        category_orders=category_orders
    )

# Function to create bar chart
def create_bar_chart(data, x_col, y_col, title):
    data['percentage'] = (data[y_col] / data[y_col].sum()) * 100
    fig = px.bar(
        data,
        x=x_col,
        y="percentage",
        title=title,
        labels={x_col: "Company Type", "percentage": "Percentage (%)"},
        text="percentage",
        width=1000,
        height=700
    )
    fig.update_traces(texttemplate='%{text:.2f}%',
                      textposition='outside', cliponaxis=False)
    return fig

# Function to prepare years bracket data

def label_top_rows(df, col_target, top_n=10):
    """
    Labels all occupations not in the top `top_n` as 'Other'.

    Parameters:
    - df: pandas DataFrame containing the columns 'occupation' and 'size'.
    - top_n: the number of top occupations to retain.

    Returns:
    - A modified DataFrame with an updated 'occupation' column.
    """
    # Get the top_n occupations by size
    top_occupations = df.nlargest(top_n, 'size')[col_target].tolist()

    # Create a new column with updated labels
    df[col_target] = df[col_target].apply(
        lambda x: x if x in top_occupations else 'Other')

    return df


def create_toggleable_pie_charts(dataframes, labels_columns, values_columns, titles, width=800, height=600):
    """
    Creates a Plotly figure with toggleable pie charts for multiple dataframes.

    Parameters:
        dataframes (list): List of DataFrames for pie charts.
        labels_columns (list): List of column names for labels in each DataFrame.
        values_columns (list): List of column names for values in each DataFrame.
        titles (list): List of titles for each pie chart.

    Returns:
        None: Displays the interactive chart.
    """
    if not (len(dataframes) == len(labels_columns) == len(values_columns) == len(titles)):
        raise ValueError("All input lists must have the same length.")

    # Create the figure
    fig = go.Figure()

    # Add traces for each DataFrame
    for i, df in enumerate(dataframes):
        pie = go.Pie(
            labels=df[labels_columns[i]],
            values=df[values_columns[i]],
            name=titles[i],
        )
        fig.add_trace(pie)

    # Generate buttons for toggling pie charts
    buttons = []
    for i, title in enumerate(titles):
        visibility = [False] * len(dataframes)
        visibility[i] = True  # Show only the current pie chart
        buttons.append({
            'label': title,
            'method': 'update',
            'args': [
                {'visible': visibility},
                {'title': title}  # Update the title dynamically
            ],
        })

    # Set initial layout with the first chart's title
    fig.update_layout(
        updatemenus=[{'buttons': buttons}],
        title=titles[0],  # Set the initial title
        width=width,  # Set figure width
        height=height  # Set figure height
    )

    # Set initial visibility
    for i, trace in enumerate(fig.data):
        trace.visible = (i == 0)  # Only the first chart is visible by default

    # Show the figure
    return fig


def create_toggleable_bar_charts(dataframes, x_columns, y_columns, titles, width=800, height=600):
    """
    Creates a Plotly figure with toggleable bar charts for multiple dataframes.

    Parameters:
        dataframes (list): List of DataFrames for bar charts.
        x_columns (list): List of column names for x-axis in each DataFrame.
        y_columns (list): List of column names for y-axis in each DataFrame.
        titles (list): List of titles for each bar chart.

    Returns:
        None: Displays the interactive chart.
    """
    if not (len(dataframes) == len(x_columns) == len(y_columns) == len(titles)):
        raise ValueError("All input lists must have the same length.")

    # Create the figure
    fig = go.Figure()

    # Add traces for each DataFrame
    for i, df in enumerate(dataframes):
        bar = go.Bar(
            x=df[x_columns[i]],
            y=df[y_columns[i]],
            name=titles[i],
        )
        fig.add_trace(bar)

    # Generate buttons for toggling bar charts
    buttons = []
    for i, title in enumerate(titles):
        visibility = [False] * len(dataframes)
        visibility[i] = True  # Show only the current bar chart
        buttons.append({
            'label': title,
            'method': 'update',
            'args': [
                {'visible': visibility},
                {'title': title}  # Update the title dynamically
            ],
        })

    # Set initial layout with the first chart's title
    fig.update_layout(
        updatemenus=[{'buttons': buttons}],
        title=titles[0],  # Set the initial title
        width=width,  # Set figure width
        height=height  # Set figure height
    )

    # Set initial visibility
    for i, trace in enumerate(fig.data):
        trace.visible = (i == 0)  # Only the first chart is visible by default

    # Show the figure
    return fig
