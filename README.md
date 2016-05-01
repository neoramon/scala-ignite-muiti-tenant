# scala-ignite-muiti-tenant
Ignite scala POC to test performance of 2 multi-tenant approaches:

http://stackoverflow.com/questions/36950370/apache-ignite-performance-of-multi-tenant-approaches

The conclusio is: The best approach depends on the number of records to put in the cache.
With few records there is no significant difference in the times.
One cache tenant concatenated with the key (product) shows up to 1 second faster.
With over 100k records: Tenants separate be more efficient.