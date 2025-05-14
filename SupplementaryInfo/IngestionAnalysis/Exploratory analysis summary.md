# Exploratory Dataset Analysis Summary

## Gas prices

This data set is rather small, having only about 8000 observations. There are some columns that have constant value or that are essentially redundant with other columns that will need to be removed during cleaning.

The types of the columns are mostly categorical, with one date and 2 numeric. Work will need to be put into understanding these columns, particularly the location-related ones.

The gas price values range from .919 to 6.416, with a mean of 2.71. Not really important, but it's cool to already have a little bit of analysis relevant to my overall goal.

This data set has very few missing entries.

## Gas sales

This data set is surprisingly large, with about 120000 entries.

The distribution of gas sales values appears to be hugely skewed, and there are a *significant* number of missing elements (just over 50%).

## Oil imports

With about 70000 observations, this data set is medium sized for my project.

The import quantity is the only numeric column here.

There are *zero* missing entries.

## Climate normals

With about 88000 observations, this data set is on the larger side for my project.

This data set has many numeric columns. During transformation, I will remove most of these, because I only need the average temperature and a few other columns.

There are zero missing entries here, too.