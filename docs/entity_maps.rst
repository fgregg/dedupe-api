Tke main table for entitity matching is the entity mapping table, which maps records to entities.

If we only had one source of records than `entity_map` could be a two column table:

+-----------+-----------+
| record_id | entity_id |
+===========+===========+
|123        |         1 |
+-----------+-----------+


+------------+------------+-----------+
| Header 1   | Header 2   | Header 3  |
+============+============+===========+
| body row 1 | column 2   | column 3  |
+------------+------------+-----------+
| body row 2 | Cells may span columns.|
+------------+------------+-----------+
| body row 3 | Cells may  | - Cells   |
+------------+ span rows. | - contain |
| body row 4 |            | - blocks. |
+------------+------------+-----------+


1. links unique IDs to ISBE candidate or officer IDs, I propose we try
to use opencivicdata ids
2. gives enough information about the entity that the consumer of this
linkage can evaluate this linkage (name, address, party, office, etc)
3. gives the rational for this linkage, exact match, near match, and
human review. If it's a near match or exact match then give a
confidence score for the match. If there's been a clerical review,
give an identifier of the reviewer

Something that looks like this:

https://github.com/influence-usa/campaign-finance_state_IL/blob/master/entity_map.csv
