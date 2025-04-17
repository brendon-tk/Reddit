import streamlit as st
from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark
spark = SparkSession.builder.appName('Reddit Post Analysis').getOrCreate()

# Title
st.title("📊 Reddit Post Analysis App")

# Upload CSV
uploaded_file = st.file_uploader("Upload your reddit_posts.csv", type=["csv"])

if uploaded_file:
    # Load CSV into Spark
    df = spark.read.option("header", True) \
        .option("inferSchema", True) \
        .option("multiLine", True) \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("mode", "DROPMALFORMED") \
        .csv(uploaded_file)

    # Clean the data
    df_clean = df.filter("id IS NOT NULL AND subreddit IS NOT NULL AND title IS NOT NULL AND id LIKE '1%'")
    df_clean.createOrReplaceTempView("posts")

    # Show raw data
    st.subheader("🔍 Preview of Reddit Posts")
    st.dataframe(df_clean.limit(10).toPandas())

    # Subreddit post count
    st.subheader("📌 Post Count by Subreddit")
    subreddit_counts = spark.sql("""
        SELECT subreddit, COUNT(*) as post_count
        FROM posts
        GROUP BY subreddit
        ORDER BY post_count DESC
    """)
    st.dataframe(subreddit_counts.toPandas())

    # Top 10 fastest growing posts
    st.subheader("🚀 Top 10 Fastest Growing Posts (within 3 hours)")
    top_growth = spark.sql("""
        SELECT title, subreddit, score_immediate, score_3hr,
               (score_3hr - score_immediate) AS growth_3hr
        FROM posts
        WHERE score_3hr IS NOT NULL
        ORDER BY growth_3hr DESC
        LIMIT 10
    """)
    st.dataframe(top_growth.toPandas())

    st.success("✅ Analysis complete!")

else:
    st.info("👈 Please upload a CSV file to get started.")

