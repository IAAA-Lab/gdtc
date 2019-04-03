# Design guidelines
This document will hold the main design guidelines of the project. Generally speaking design decisions should be aligned with this high-level constraints and objectives.

For now, just some general guidelines, we will reconsider them as we go on.

In general, we will adhere to the "make it work; make it right; make it fast" motto, and to the Unix philosophy of simple tools that can be easily chained.

## GDTC will be opinionated and will provide sensible defaults
Making the design of geopipelines as simple as possible requires this. If we providing many different options, parameters and ways to do things, this project will end up being just a collection of loosely integrated geographic tools, and that is not worth the effort.

This may require sacrificing efficiency or some flexibility. Nothing worth having is free.

## Smart users
GDTC is intended for users that are fluent in Python and know their geo stuff, but are not particularly interested in remembering the syntax of PostGIS SQL, or the parameters of GDAL, or which version of this can be used with that etc.

## No GUI
We are not against GUI in general. We just want to focus on pipelines scripted in Python. 

## Simple filters are the basic stuff
Each basic filter should do on one thing. Complex processes will then involve a number of chained simple filters. 

## Complex filters are just simple filters chained
Some of the processes may in turn be wrapped in their own filters as we see the necessity.

## Factories to create filters
Although filters can be created directly, factories to create them will give us a place to hide certain complexity, to enforce opinions and defaults and, maybe, to provide some performance optimizations. Factories should be the most common way to create filters.

## Simple before fast
Computer time is far less valuable than people time. Providing the simpler way to design the pipelines comes first, and making those run fast comes second (or third...).

## Automatic performance improvements
Making some constraints on the tools used, will make it easier to improve the performance of pipelines in the future. E.g., if we enforce the use of a certain version of PostgreSQL/PostGIS for the DB2DB filters, it may be possible to analyze a filter chain desiged by a user and to produce a more efficient alternative (e.g. by combining some SQL queries, or ordering them in a different way).

## Deployment based on Docker
Using GDTC should not require a complex configuration of the user environment or installing tools and libraries by hand. Docker will make this possible.
