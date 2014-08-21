Currently, this service depends upon their be an existing canonical set. Often the client will not have that set, that's why they need dedupe in the first place.

Here's how we can accommodate that.

1. The administrative user upload and trains a dedupe session (like in spreadsheet deduper)
2. From that output, (scored clusters) we create review tasks. The task is to confirm or deny that a cluster of records all refer to the same entity.
3. It's possible that multiple clusters all refer to the same entity. We will create canonical representations of each cluster and the run a dedupe session on these representations.

This creates our master list. It is very likely to be a subset of the original data. We can then run a normal 'match' task for the unmatched records against the master file.
