"""
Visualization Module for Data Analysis.

This module provides a collection of utility functions for creating interactive
visualizations using the Plotly library. The visualizations include pie charts,
bar charts, and toggleable charts for comparing multiple datasets.

This module is designed for use in data analysis pipelines where
visual exploration and presentation of results are essential.
"""
import plotly.express as px
import plotly.graph_objects as go
from pandas import DataFrame
from typing import List


def create_pie_chart(df: DataFrame,
                     names_col: str,
                     values_col: str,
                     title: str,
                     category_orders=None,
                     width: int = 1000,
                     height: int = 700
                     ) -> go.Figure:
    """
    Create an interactive pie chart using Plotly.

    Parameters
    ----------
    df : DataFrame
        The DataFrame containing the data for the pie chart.
    names_col : str
        The column name to use for the labels of the pie slices.
    values_col : str
        The column name to use for the values of the pie slices.
    title : str
        The title of the pie chart.
    category_orders : dict, optional
        A dictionary specifying the ordering of categories for the pie chart.
    width : int, optional
        Width of the figure (default is 1000).
    height : int, optional
        Height of the figure (default is 700).

    Returns
    -------
    go.Figure
        The Plotly figure object representing the pie chart.
    """
    return px.pie(
        df,
        names=names_col,
        values=values_col,
        title=title,
        width=width,
        height=height,
        category_orders=category_orders
    )


def create_bar_chart(df: DataFrame,
                     x_col: str,
                     y_col: str,
                     title: str,
                     width: int = 1000,
                     height: int = 700
                     ) -> go.Figure:
    """
    Create an interactive bar chart using Plotly.

    Parameters
    ----------
    df : DataFrame
        The DataFrame containing the data for the bar chart.
    x_col : str
        The column name to use for the x-axis.
    y_col : str
        The column name to use for the y-axis.
    title : str
        The title of the bar chart.
    width : int, optional
        Width of the figure (default is 1000).
    height : int, optional
        Height of the figure (default is 700).

    Returns
    -------
    go.Figure
        The Plotly figure object representing the bar chart.
    """
    df['percentage'] = (df[y_col] / df[y_col].sum()) * 100
    fig = px.bar(
        df,
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


def label_top_rows(df: DataFrame,
                   col_target: str,
                   top_n: int = 10
                   ) -> DataFrame:
    """
    Labels rows in a DataFrame that are not in the top N as 'Other'.

    Parameters
    ----------
    df : DataFrame
        The DataFrame containing the data to process.
    col_target : str
        The column to label the top N rows.
    top_n : int, optional
        The number of top rows to retain (default is 10).

    Returns
    -------
    DataFrame
        The modified DataFrame with an updated column.
    """
    top_occupations = df.nlargest(top_n, 'size')[col_target].tolist()
    df[col_target] = df[col_target].apply(lambda x: x if x in top_occupations else 'Other')
    return df


def create_toggleable_pie_charts(dataframes: List[DataFrame],
                                 labels_columns: List[str],
                                 values_columns: List[str],
                                 titles: List[str],
                                 width: int = 800,
                                 height: int = 600
                                 ) -> go.Figure:
    """
    Creates a Plotly figure with toggleable pie charts for multiple DataFrames.

    Parameters
    ----------
    dataframes : list of DataFrame
        List of DataFrames for pie charts.
    labels_columns : list of str
        List of column names for labels in each DataFrame.
    values_columns : list of str
        List of column names for values in each DataFrame.
    titles : list of str
        List of titles for each pie chart.
    width : int, optional
        Width of the figure (default is 800).
    height : int, optional
        Height of the figure (default is 600).

    Returns
    -------
    go.Figure
        The Plotly figure object with toggleable pie charts.
    """
    if not (len(dataframes) == len(labels_columns) == len(values_columns) == len(titles)):
        raise ValueError("All input lists must have the same length.")

    fig = go.Figure()

    for i, df in enumerate(dataframes):
        pie = go.Pie(labels=df[labels_columns[i]], values=df[values_columns[i]], name=titles[i])
        fig.add_trace(pie)

    buttons = [
        {'label': title,
         'method': 'update',
         'args': [{'visible': [i == j for j in range(len(dataframes))]},
                  {'title': title}]}
        for i, title in enumerate(titles)
    ]

    fig.update_layout(
        updatemenus=[{'buttons': buttons}],
        title=titles[0],
        width=width,
        height=height
    )

    for i, trace in enumerate(fig.data):
        trace.visible = (i == 0)

    return fig


def create_toggleable_bar_charts(dataframes: List[DataFrame],
                                 x_columns: List[str],
                                 y_columns: List[str],
                                 titles: List[str],
                                 width: int = 800,
                                 height: int = 600
                                 ) -> go.Figure:
    """
    Creates a Plotly figure with toggleable bar charts for multiple DataFrames.

    Parameters
    ----------
    dataframes : list of DataFrame
        List of DataFrames for bar charts.
    x_columns : list of str
        List of column names for x-axis in each DataFrame.
    y_columns : list of str
        List of column names for y-axis in each DataFrame.
    titles : list of str
        List of titles for each bar chart.
    width : int, optional
        Width of the figure (default is 800).
    height : int, optional
        Height of the figure (default is 600).

    Returns
    -------
    go.Figure
        The Plotly figure object with toggleable bar charts.
    """
    if not (len(dataframes) == len(x_columns) == len(y_columns) == len(titles)):
        raise ValueError("All input lists must have the same length.")

    fig = go.Figure()

    for i, df in enumerate(dataframes):
        bar = go.Bar(x=df[x_columns[i]], y=df[y_columns[i]], name=titles[i])
        fig.add_trace(bar)

    buttons = [
        {'label': title,
         'method': 'update',
         'args': [{'visible': [i == j for j in range(len(dataframes))]},
                  {'title': title}]}
        for i, title in enumerate(titles)
    ]

    fig.update_layout(
        updatemenus=[{'buttons': buttons}],
        title=titles[0],
        width=width,
        height=height
    )

    for i, trace in enumerate(fig.data):
        trace.visible = (i == 0)

    return fig
