import pandas as pd
import altair as alt
import altair_viewer as viewer

'''
NOTE: 
This code aims at preparing and printing the visualization of the sentiment analysis grouped by the different categories.
'''

#Read dataset
df = pd.read_csv("/Users/daniel/LocalFiles for TFM/sentiment-analysis-analytics.csv")
print(df.head())

#Command to drop the limitation of 5000 rows allowed
alt.data_transformers.disable_max_rows()

chart1 = alt.Chart(df).mark_bar(
    cornerRadiusTopLeft=3,
    cornerRadiusTopRight=3
).encode(
    x='category:N',
    y=alt.Y('sum(positive_comments):Q', title='Sum of Positive comments',axis=alt.Axis(labelOverlap=True)),
    color='category:N'
).properties(title='Distribution of Positive Comments',width=300)

chart2 = alt.Chart(df).mark_bar(
    cornerRadiusTopLeft=3,
    cornerRadiusTopRight=3
).encode(
    x='category:N',
    y=alt.Y('sum(negative_comments):Q', title='Sum of Negative comments',axis=alt.Axis(labelOverlap=True)),
    color='category:N'
).properties(title='Distribution of Negative Comments',width=300)

chart3 = alt.Chart(df).mark_bar(
    cornerRadiusTopLeft=3,
    cornerRadiusTopRight=3
).encode(
    x='category:N',
    y=alt.Y('sum(neutral_comments):Q', title='Sum of Neutral comments',axis=alt.Axis(labelOverlap=True)),
    color='category:N'
).properties(title='Distribution of Neutral Comments',width=300)

concat = alt.hconcat(chart1,chart2,chart3).resolve_scale(y='shared').configure_title(fontSize = 15, offset=5, orient='top', anchor='middle')
viewer.show(concat)