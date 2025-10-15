CREATE FUNCTION GEO_DISTANCE
AS
'io.confluent.udf.GeoDistanceFunction'
USING JAR 'confluent-artifact://cfa-...';