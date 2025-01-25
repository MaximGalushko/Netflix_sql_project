# Netflix Movies and TV Shows Data Analysis using SQL

![](https://github.com/najirh/netflix_sql_project/blob/main/logo.png)

## Overview
This project involves a comprehensive analysis of Netflix's movies and TV shows data using SQL. The goal is to extract valuable insights and answer various business questions based on the dataset. The following README provides a detailed account of the project's objectives, business problems, solutions, findings, and conclusions.

## Objectives

- Analyze the distribution of content types (movies vs TV shows).
- Identify the most common ratings for movies and TV shows.
- List and analyze content based on release years, countries, and durations.
- Explore and categorize content based on specific criteria and keywords.

## Dataset

The data for this project is sourced from the Kaggle dataset:

- **Dataset Link:** [Movies Dataset](https://www.kaggle.com/datasets/shivamb/netflix-shows?resource=download)

## Schema

```sql
DROP TABLE IF EXISTS netflix;
CREATE TABLE netflix
(
    show_id      VARCHAR(5),
    type         VARCHAR(10),
    title        VARCHAR(250),
    director     VARCHAR(550),
    casts        VARCHAR(1050),
    country      VARCHAR(550),
    date_added   VARCHAR(55),
    release_year INT,
    rating       VARCHAR(15),
    duration     VARCHAR(15),
    listed_in    VARCHAR(250),
    description  VARCHAR(550)
);
CREATE INDEX idx_netflix_type ON netflix (type);
```

## Business Problems and Solutions

### Query 1: Count the number of Movies vs TV Shows (with index check)

```sql
EXPLAIN ANALYZE 
SELECT 
	type
	, COUNT(*) AS Total_count_b
FROM netflix
GROUP BY type;
```

**Objective:** Determine the distribution of content types on Netflix.

### Query 2: Find the most common rating for movies and TV shows

```sql
WITH cte1 AS (
	SELECT 
		type
  		,rating
  		,RANK() OVER (PARTITION BY type ORDER BY COUNT(*) DESC) AS ranking
 	FROM netflix
 	GROUP BY 1,2
)
SELECT 
 	type
 	,rating
FROM cte1
WHERE ranking = 1;
```

**Objective:** Identify the most frequently occurring rating for each type of content.

### Query 3: Determine the dynamics of content addition by year for each genre and display the increase/decrease compared to the previous year

```sql
WITH new_listen_in_table AS (
	SELECT 
		*
		,RIGHT(date_added, 4)::int AS year_added
		,TRIM ( UNNEST (STRING_TO_ARRAY (listen_in, ','))) AS new_listen_in
	FROM netflix 
	WHERE date_added IS NOT NULL
),
content_by_year_genre_table AS (
	SELECT 
		new_listen_in
		, year_added
		, COUNT(show_id) AS total_content_by_year_genre
	FROM new_listen_in_table
	GROUP BY 1,2
	ORDER BY 1,2

)
SELECT 
	*
	,CONCAT(
		ROUND(
			(total_content_by_year_genre * 100.0 / 
				LAG(total_content_by_year_genre) OVER(PARTITION BY new_listen_in ORDER BY total_content_by_year_genre)) - 100
		,2)
	,' %')  AS increase_previous_year
FROM content_by_year_genre_table
```

**Objective:** Retrieve all movies released in a specific year.

### Query 4: Find countries that have added content consistently every year for the past 5 years

```sql
WITH max_date_cte AS (
	SELECT 
		MAX(TO_DATE(date_added, 'Month DD, YYYY')) AS max_date
	FROM netflix
)
SELECT 
	TRIM ( UNNEST ( STRING_TO_ARRAY(country, ','))) AS country_name  
FROM netflix, max_date_cte
WHERE 
	TO_DATE(date_added, 'Month DD, YYYY') >= max_date_cte.max_date - INTERVAL '5 years'
GROUP BY 1
HAVING 
	COUNT(DISTINCT RIGHT(date_added, 4)::int) >= 5
ORDER BY 1;

```

**Objective:** Identify the top 5 countries with the highest number of content items.

### Query 5: Divide the content into groups by duration, for example: short (<30 min), medium (30-90 min), long (>90 min)

```sql
SELECT 
	type
	,title
	,duration
	,CASE 
		WHEN LEFT(duration, POSITION('m' IN duration) - 1)::int < 30 THEN 'short'
		WHEN LEFT(duration, POSITION('m' IN duration) - 1)::int < 60 THEN 'medium'
		ELSE 'long'
	END AS groups_by_duration_movie
FROM netflix
WHERE 
	type = 'Movie'
	AND 
	duration IS NOT NULL;
```

**Objective:** Find the movie with the longest duration.

### Query 6: Identify the genres that have the greatest growth in content over the last 3 years (year over year).

```sql
WITH max_year_cte AS (
	-- Step 1: Determine the most recent year in the dataset and extract `year_added`
    SELECT 
		*
        ,EXTRACT(YEAR FROM TO_DATE(date_added, 'Month DD, YYYY')) AS year_added
        ,MAX(EXTRACT(YEAR FROM TO_DATE(date_added, 'Month DD, YYYY'))) OVER () AS max_year
    FROM netflix
),
content_last_3_years AS (
	-- Step 2: Filter for content added within the last 3 years
    SELECT 
        *
        ,TRIM(UNNEST(STRING_TO_ARRAY(listen_in, ','))) AS all_genre
    FROM max_year_cte
    WHERE year_added BETWEEN max_year - 2 AND max_year
),
group_by_genre_year AS (
	-- Step 3: Group by genre and year, calculate the total content for each genre per year
	SELECT
		all_genre
		,year_added
		,COUNT(show_id) AS content_by_genre
		,LAG(COUNT(*)) OVER (PARTITION BY all_genre ORDER BY year_added) AS content_by_previous_year
	FROM content_last_3_years
	GROUP BY all_genre, year_added
	ORDER BY 1,2
),
growth_content_cte AS (
	-- Step 4: Calculate the year-over-year growth for each genre
	SELECT 
		*
		,content_by_genre * 100 / content_by_previous_year - 100 AS growth_content
	FROM group_by_genre_year
),
avarage_growth_content_cte AS (
	-- Step 5: Calculate the average growth rate for each genre over the last 3 years
	SELECT 
		all_genre
		,AVG(growth_content) AS avarage_growth_content
	FROM growth_content_cte
	WHERE 
		growth_content IS NOT NULL
	GROUP BY all_genre
	ORDER BY 2 DESC
)
-- Step 6: Display the genres with their average growth rates
SELECT 
	all_genre
	,CONCAT(
		ROUND(
			avarage_growth_content
		,2)
	,' %') AS avarage_growth_content
FROM avarage_growth_content_cte;
```

**Objective:** Retrieve content added to Netflix in the last 5 years.

### Query 7: Find the top 5 countries with the most content on Netflix

```sql
SELECT 
 	TRIM ( UNNEST ( STRING_TO_ARRAY( country, ',' )))  AS new_country
 	,COUNT(*)                                          AS total_content
FROM netflix
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;
```

**Objective:** List all content directed by 'Rajiv Chilaka'.

### Query 8: Identify the longest movie or TV show duration

```sql
SELECT 
 	title
 	, SUBSTRING(duration, 1, POSITION('m' IN duration) - 1)::int  AS duration
FROM netflix
WHERE 
 	type = 'Movie' 
 	AND 
 	duration IS NOT NULL
ORDER BY 2 DESC
LIMIT 1;

```

**Objective:** Identify TV shows with more than 5 seasons.

### Query 9: Find content added in the last 5 years

```sql
ELECT 
 	title
 	, date_added
FROM netflix 
WHERE TO_DATE(date_added, 'Month DD, YYYY') >= CURRENT_DATE - INTERVAL '5 years';
```

**Objective:** Count the number of content items in each genre.

### Query 10: Find all the movies/TV shows by director 'Rajiv Chilaka'

```sql
SELECT
 	* 
FROM netflix 
WHERE director ILIKE '%Rajiv Chilaka%';
```

**Objective:** Calculate and rank years by the average number of content releases by India.

### Query 11: List all TV shows with more than 5 seasons

```sql
SELECT 
	 title
	 , duration
FROM netflix
WHERE 
	 type = 'TV Show' 
	 AND
	 LEFT(duration, POSITION('S' IN duration) - 1)::int > 5;
```

**Objective:** Retrieve all movies classified as documentaries.

### Query 12: Count the number of content items in each genre

```sql
SELECT 
	 TRIM ( UNNEST ( STRING_TO_ARRAY(listen_in, ','))) AS genre
	 , COUNT(show_id)                                  AS Total_content
FROM netflix
GROUP BY 1
ORDER BY 2 DESC;
```

**Objective:** List content that does not have a director.

### Query 13: Find the year and average amount of content released in India on Netflix, and return the 5 years with the highest averages

```sql
SELECT 
    EXTRACT(YEAR FROM TO_DATE(date_added, 'Month DD, YYYY')) AS year
    , COUNT(*)                                               AS total_content
    , ROUND( 
  		COUNT(*)::numeric /   
    		(SELECT COUNT(*) FROM netflix WHERE country = 'India')::numeric * 100
 		,2) AS avg_content_per_year    
FROM netflix
WHERE 
	country = 'India'
GROUP BY year
ORDER BY avg_content_per_year DESC;
```

**Objective:** Count the number of movies featuring 'Salman Khan' in the last 10 years.

### Query 14: Categorize the content based on the presence of the keywords 'kill' and 'violence' in the description field. 
### Label content containing these keywords as 'Bad' and all other content as 'Good'. Count how many items fall into each category.

```sql
WITH Good_or_Bad AS (
	SELECT *,
 		CASE 
  			WHEN description ILIKE '% kill %' OR description ILIKE '% kill, %' OR description ILIKE '%Kill %' 
   				OR description ILIKE ' violence ' OR description ILIKE ' violence, ' OR description ILIKE 'violence %' THEN 'Bad'
  			ELSE 'Good' 
 		END AS Categorize
	FROM netflix
	ORDER BY Categorize
)

SELECT Categorize, COUNT(*) AS Count_content
FROM Good_or_Bad
GROUP BY 1;
```

**Objective:** Identify the top 10 actors with the most appearances in Indian-produced movies.


## Findings and Conclusion

- **Content Distribution:** The dataset contains a diverse range of movies and TV shows with varying ratings and genres.
- **Common Ratings:** Insights into the most common ratings provide an understanding of the content's target audience.
- **Geographical Insights:** The top countries and the average content releases by India highlight regional content distribution.
- **Content Categorization:** Categorizing content based on specific keywords helps in understanding the nature of content available on Netflix.

This analysis provides a comprehensive view of Netflix's content and can help inform content strategy and decision-making.



## Author - Zero Analyst

This project is part of my portfolio, showcasing the SQL skills essential for data analyst roles. If you have any questions, feedback, or would like to collaborate, feel free to get in touch!

### Stay Updated and Join the Community

For more content on SQL, data analysis, and other data-related topics, make sure to follow me on social media and join our community:

- **YouTube**: [Subscribe to my channel for tutorials and insights](https://www.youtube.com/@zero_analyst)
- **Instagram**: [Follow me for daily tips and updates](https://www.instagram.com/zero_analyst/)
- **LinkedIn**: [Connect with me professionally](https://www.linkedin.com/in/najirr)
- **Discord**: [Join our community to learn and grow together](https://discord.gg/36h5f2Z5PK)

Thank you for your support, and I look forward to connecting with you!
