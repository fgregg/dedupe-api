Tke main table for entitity matching is the entity mapping table, which maps records to entities.

If we had two records, 123 and 124, which referred to separate entities, entity_map could look like this:

+-------------+-----------+
| entity_id   | record_id |
+=============+===========+
|1            | 123       |
+-------------+-----------+
|2             | 124      |
+-------------+-----------+

If we changed our mind, and decided that the records referred to the same entity the table could look like this:

+-------------+-----------+
| entity_id   | record_id |
+=============+===========+
|1            | 123       |
+-------------+-----------+
|1            | 124       |
+-------------+-----------+


The entity map represents our best current beliefs about the relations between records and entities. We should attach
the reasons for these beliefs. The possible rationales include:

1. Original clustering
2. Exact match to target record
3. Near match to target record
4. Confirmed match to target record

+-------------+-----------+------------+---------------+-------+
| entity_id   | record_id | match_type | target_record | score |
+=============+===========+============+===============+=======+
| 1           |123        | Cluster    | NULL          | .8    |
+-------------+-----------+------------+---------------+-------+
| 1           |124        | Exact      | 123           | 1.0   |
+-------------+-----------+------------+---------------+-------+
| 1           |125        | Cluster    | NULL          | .6    |
+-------------+-----------+------------+---------------+-------+
| 1           |126        | Near       | 125           | .7    |
+-------------+-----------+------------+---------------+-------+


+----------+------------+-----------+-------+-------------+-----------+----------+
| match_id | match_type | target_id | score | reviewer_id | timestamp | current? |
+==========+============+===========+=======+=============+===========+==========+
| 2        | Exact      | 1         | 1.00  | Null        | 2014...   | True     |
+----------+------------+-----------+-------+-------------+-----------+----------+
| 3        | Near       | Null      | 0.32  | Null        | 2014..    | True     |
+----------+------------+-----------+-------+-------------+-----------+----------+
| 4        | Near       | 1         | 0.90  | ftg         | 2014...   | True     |
+----------+------------+-----------+-------+-------------+-----------+----------+
| 4        | Near       | 3         | 0.91  | ftg         | 2014...   | False    |
+----------+------------+-----------+-------+-------------+-----------+----------+

From the match table we can reproduced the chain of reasoning why we believe some record refers to some entitity. 

(we may want to normalize reviewer_id out).

When we match a new record, we match the record against all the records that have a record_guid in `entity_map` 
If we decide there's a match we add a new row to entity_map and to the match table.

If we have data from more than one source, then we need to make sure we can unambiguously refer to each record.
We could either make sure that each record has an globably unique id or add a source field to our entity map (assuming 
that records within each source are locally unique). We'll choose the second strategy. 

+-------------+-----------+--------+-----------+
| record_guid | record_id | source | entity_id |
+=============+===========+========+===========+
| 1           |123        | A      | 1         |
+-------------+-----------+--------+-----------+
| 2           |124        | A      | 1         |
+-------------+-----------+--------+-----------+
| 3           |123        | B      | 3         |
+-------------+-----------+--------+-----------+

The combination of (record_id, source) must be unique in the `entity_map` table.
