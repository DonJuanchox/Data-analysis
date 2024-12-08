import plotly.express as px

# Function to prepare city sizes dataset
def prepare_city_data(data, top_n=50):
    return (
        data.groupby('city', as_index=False)
        .size()
        .sort_values('size', ascending=False)
        .reset_index(drop=True)
        .head(top_n)
    )

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
    fig.update_traces(texttemplate='%{text:.2f}%', textposition='outside', cliponaxis=False)
    return fig

# Function to prepare years bracket data
def prepare_years_bracket_data(data):
    return (
        data.groupby('Years bracket', as_index=False)
        .size()
        .sort_values('size', ascending=False)
        .reset_index(drop=True)
    )
