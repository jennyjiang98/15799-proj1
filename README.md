# 15799-proj1
### PostgreSQL Auto Index Tuner

#### Support: selecting btree / clustering on btree / hash indexes for columns based on HypoPG's cost estimations. 

Considers candidates of all co-appeard columns in 'where', 'order_by', 'group_by' and 'join' clauses and their subsets in a brute-force fashion. 
- 'Where' and 'join' uses all permutation. 
- 'Order_by', 'group_by' only considered the first column + up to 1 corresponding column in 'where'.
- Clustering based on simple heuristics from db2adviser paper's thought: benefits of an index can be totally covered by a longer index with the same prefix.


Credit: Preprocessing & db_connector code partially adapted from [NoisePage](https://github.com/cmu-db/noisepage-pilot/) and [Hyrise](https://github.com/hyrise/index_selection_evaluation).
