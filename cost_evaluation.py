# adapted from hyrise https://github.com/hyrise/index_selection_evaluation
# support current built indexes, type, and get workload query indexes usage
import logging
import pandas as pd
from what_if_index_creation import WhatIfIndexCreation


class CostEvaluation:
    def __init__(self, db_connector, curr_real_indexes2index_obj, wkld_df, cost_estimation="whatif"):
        logging.debug("Init cost evaluation")
        self.db_connector = db_connector
        self.cost_estimation = cost_estimation
        logging.info("Cost estimation with " + self.cost_estimation)
        self.what_if = WhatIfIndexCreation(db_connector)
        self.current_hypo_indexes = set()  # triples
        self.curr_real_indexes = set([triple for triple, i in
                                      curr_real_indexes2index_obj.items()])  # set reference, will modify the real set at the same time (if drop any)
        print("curr real indexes", self.curr_real_indexes)
        self.curr_real_indexes2index_obj = curr_real_indexes2index_obj
        self.cache_hits = 0
        self.wkld_df = wkld_df
        # Cache structure:
        # {(query_object, relevant_indexes): cost}
        self.cache = {}
        self.relevant_indexes_cache = {}

    def get_query_indexes_and_cost(self, query, triple_list):
        # must call self._prepare_cost_calculation(indexes_table_tup_list) before using
        plan = self.db_connector.get_plan(query)
        cost = plan["Total Cost"]
        plan_str = str(plan)

        used_indexes = set()

        for index in triple_list:
            cols, table_name, type = index
            index_name_substr = type + '_' + table_name + '_' + '_'.join(cols) + '\''
            if index_name_substr not in plan_str:  # if no '\'', will get all prefixes
                if not index in self.curr_real_indexes2index_obj.keys():
                    continue  # the hypo index is truely not used
                if self.curr_real_indexes2index_obj[index].index_name not in plan_str:
                    continue  # the real index is truely not used
            used_indexes.add(index)

        return used_indexes, cost

    def get_wkld_utilized_indexes_improvement(self, indexes_table_tup_list):
        utilized_indexes_benefits = {}
        query_details = {}
        utilized_indexes_old = set()

        self._prepare_cost_calculation(self.curr_real_indexes)
        total_curr_cost = 0
        for df_index, row in self.wkld_df.iterrows():
            query, ref_cols = row["query_subst"], row["where_cols"]
            # single_cost = self._request_cache(query, ref_cols, self.curr_real_indexes)
            utilized_indexes_query, single_cost = self.get_query_indexes_and_cost(query, self.curr_real_indexes)
            query_details[query] = {
                "cost_prev": single_cost,
                "cost_new": -1,
                "utilized_indexes_prev": utilized_indexes_query,
                "utilized_indexes_new": -1,
            }
            utilized_indexes_old |= utilized_indexes_query
            total_curr_cost += single_cost

        all_types_candidates = self.gen_all_types_candidates(indexes_table_tup_list)
        print("all_types_candidates", all_types_candidates)
        self._prepare_cost_calculation(all_types_candidates)
        total_potential_cost = 0
        for df_index, row in self.wkld_df.iterrows():
            query, ref_cols = row["query_subst"], row["where_cols"]
            utilized_indexes_query, cost_new = self.get_query_indexes_and_cost(query, all_types_candidates)
            query_details[query]["cost_new"] = cost_new;
            query_details[query]["utilized_indexes_new"] = utilized_indexes_query;
            total_potential_cost += cost_new
            for index_tup in utilized_indexes_query:  # assumer here is one
                benefit = query_details[query]["cost_prev"] - cost_new
                utilized_indexes_benefits[index_tup] = benefit if index_tup not in utilized_indexes_benefits.keys() else \
                    utilized_indexes_benefits[index_tup] + benefit

        return utilized_indexes_benefits, utilized_indexes_old, query_details, total_curr_cost, total_potential_cost

    def gen_all_types_candidates(self, indexes_table_tup_list):
        ret = set()
        ret.update([(cols, tab, 'btree') for cols, tab in indexes_table_tup_list])
        ret.update([(cols, tab, 'hash') for cols, tab in indexes_table_tup_list if len(cols) == 1])
        return ret

    def calculate_cost(self, triple_list,
                       keep_real=False):  # if keep_real = True, will not drop real index inconsistent with indexes_table_tup_list
        calculate_index_list = set(triple_list) | self.curr_real_indexes if keep_real else set(triple_list)
        self._prepare_cost_calculation(calculate_index_list)
        total_cost = 0
        for df_index, row in self.wkld_df.iterrows():
            query, ref_cols = row["query_subst"], row["where_cols"]
            self.cost_requests += 1
            total_cost += self._request_cache(query, ref_cols, calculate_index_list)
        return total_cost

    # Creates the current index combination by simulating/creating
    # missing indexes and unsimulating/dropping indexes
    # that exist but are not in the combination.
    def _prepare_cost_calculation(self, triple_list, store_size=False):
        for index in set(triple_list) - self.current_hypo_indexes - self.curr_real_indexes:
            self._simulate_or_create_index(index, store_size=store_size)
        for index in self.curr_real_indexes - set(triple_list):
            self._drop_real_index(index)
        for index in self.current_hypo_indexes - set(triple_list):
            self._unsimulate_hypo_index(index)

        assert (self.current_hypo_indexes | self.curr_real_indexes) == set(triple_list)

    def _simulate_or_create_index(self, triple, store_size=False):
        if triple in self.curr_real_indexes:
            return
        if self.cost_estimation == "whatif":
            self.what_if.simulate_index(triple)
        self.current_hypo_indexes.add(triple)

    def _unsimulate_hypo_index(self, triple):
        if self.cost_estimation == "whatif":
            self.what_if.drop_simulated_index(triple)
        self.current_hypo_indexes.remove(triple)

    def _drop_real_index(self, triple):
        if triple in self.curr_real_indexes2index_obj.keys():
            self.db_connector.drop_index(self.curr_real_indexes2index_obj[triple].index_name)
            del self.curr_real_indexes2index_obj[triple]
        else:
            logging.error(triple, "not in curr_real_indexes2index_obj")

        self.curr_real_indexes.remove(triple)

    def _get_cost(self, query):
        if self.cost_estimation == "whatif":
            return self.db_connector.get_cost(query)
        elif self.cost_estimation == "actual_runtimes":
            runtime = self.db_connector.exec_query(query)[0]
            return runtime

    def _request_cache(self, query, ref_cols, indexes):
        q_i_hash = (query, frozenset(indexes))
        if q_i_hash in self.relevant_indexes_cache:
            relevant_indexes = self.relevant_indexes_cache[q_i_hash]
        else:
            relevant_indexes = self._relevant_indexes(query, ref_cols, indexes)
            self.relevant_indexes_cache[q_i_hash] = relevant_indexes
        # Check if query and corresponding relevant indexes in cache
        if (query, relevant_indexes) in self.cache:
            self.cache_hits += 1
            return self.cache[(query, relevant_indexes)]
        # If no cache hit request cost from database system
        else:
            cost = self._get_cost(query)
            self.cache[(query, relevant_indexes)] = cost
            return cost

    def _relevant_indexes(self, query, ref_cols, indexes):
        relevant_indexes = [
            # check if "table.col" in ref_cols
            x for x in indexes if any(x[1] + "." + col in ref_cols for col in x[0])
        ]
        return frozenset(relevant_indexes)
