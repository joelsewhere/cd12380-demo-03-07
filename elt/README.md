# Join and Aggregate Data

## `author_geo_stats`
- Isolate page views
- Join `authors`, `quotes`, `pageviews`, and `countries`
- Aggregate metrics
  - Number of quotes
  - Total page views

## `tag_engagement`
- Isolate page views
- Unnest tags
- Join unnested tags to pageviews
- Aggregate metrics
    - Number of quotes
    - Distinct authors
    - Total pageviews
    - Pageviews weighted by number of quotes
