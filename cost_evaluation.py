# adapted from hyrise https://github.com/hyrise/index_selection_evaluation
# support current built indexes and get workload query indexes usage
import logging
import pandas as pd
from what_if_index_creation import WhatIfIndexCreation


class CostEvaluation:
    def __init__(self, db_connector, curr_real_indexes, curr_real_indexes2index_name, wkld_df, cost_estimation="whatif"):
        logging.debug("Init cost evaluation")
        self.db_connector = db_connector
        self.cost_estimation = cost_estimation
        logging.info("Cost estimation with " + self.cost_estimation)
        self.what_if = WhatIfIndexCreation(db_connector)
        self.current_hypo_indexes = set()
        self.curr_real_indexes = curr_real_indexes # set, 同时也修改原本的set
        self.curr_real_indexes2index_name = curr_real_indexes2index_name
        self.cost_requests = 0
        self.cache_hits = 0
        self.wkld_df = wkld_df
        # Cache structure:
        # {(query_object, relevant_indexes): cost}
        self.cache = {}
        self.relevant_indexes_cache = {}


    def get_query_indexes_and_cost(self, query, indexes_table_tup_list):
        # must call self._prepare_cost_calculation(indexes_table_tup_list) before using
        plan = self.db_connector.get_plan(query)
        cost = plan["Total Cost"]
        plan_str = str(plan)

        used_indexes = set()

        for index in indexes_table_tup_list:
            cols, table_name = index
            # print(plan_str)
            index_name_substr=table_name +'_'+ '_'.join(cols)+'\''
            if index_name_substr not in plan_str: # if no '\'', will get all prefixes
                if not index in self.curr_real_indexes2index_name.keys():
                    continue
                if self.curr_real_indexes2index_name[index] not in plan_str:
                    continue
            # print(query, "used index ", table_name +'_'+ '_'.join(cols)+'\'')
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
            # self.cost_requests += 1
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

        self._prepare_cost_calculation(indexes_table_tup_list)
        total_potential_cost = 0
        for df_index, row in self.wkld_df.iterrows():
            query, ref_cols = row["query_subst"], row["where_cols"] # 每个q拿到本q用到的所有index
            utilized_indexes_query, cost_new = self.get_query_indexes_and_cost(query, indexes_table_tup_list)
            query_details[query]["cost_new"] = cost_new;
            query_details[query]["utilized_indexes_new"] = utilized_indexes_query;
            total_potential_cost += cost_new
            for index_tup in utilized_indexes_query: # assumer here is one
                benefit = query_details[query]["cost_prev"] - cost_new
                utilized_indexes_benefits[index_tup] = benefit if index_tup not in utilized_indexes_benefits.keys() else utilized_indexes_benefits[index_tup] + benefit

        return utilized_indexes_benefits, utilized_indexes_old, query_details, total_curr_cost, total_potential_cost

    def calculate_cost(self, indexes_table_tup_list, keep_real = False): # if keep_real = True, will not drop real index inconsistent with indexes_table_tup_list
        calculate_index_list = set(indexes_table_tup_list) | self.curr_real_indexes if keep_real else set(indexes_table_tup_list)
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
    def _prepare_cost_calculation(self, indexes_table_tup_list, store_size=False):
        # print("inside prepare, current indexes: ", self.current_hypo_indexes, "need indexes:", indexes_table_tup_list, "real: ", self.curr_real_indexes)
        for index in set(indexes_table_tup_list) - self.current_hypo_indexes - self.curr_real_indexes:
            # print("building hypo: ", index)
            self._simulate_or_create_index(index, store_size=store_size)
        for index in self.curr_real_indexes - set(indexes_table_tup_list):
            # print("dropping real: ", index)
            self._drop_real_index(index)
        for index in self.current_hypo_indexes - set(indexes_table_tup_list):
            # print("dropping hypo: ", index)
            self._unsimulate_hypo_index(index)

        assert (self.current_hypo_indexes | self.curr_real_indexes) == set(indexes_table_tup_list)

    def _simulate_or_create_index(self, indexes_table_tup, store_size=False):
        if indexes_table_tup in self.curr_real_indexes:
            return
        if self.cost_estimation == "whatif":
            self.what_if.simulate_index(indexes_table_tup)
        # elif self.cost_estimation == "actual_runtimes":
        #     self.db_connector.create_index(index)
        self.current_hypo_indexes.add(indexes_table_tup)

    def _unsimulate_hypo_index(self, indexes_table_tup):
        if self.cost_estimation == "whatif":
            self.what_if.drop_simulated_index(indexes_table_tup)
        # elif self.cost_estimation == "actual_runtimes":
        self.current_hypo_indexes.remove(indexes_table_tup)

    def _drop_real_index(self, indexes_table_tup):
        # elif self.cost_estimation == "actual_runtimes":
        if indexes_table_tup in self.curr_real_indexes2index_name.keys():
            self.db_connector.drop_index(self.curr_real_indexes2index_name[indexes_table_tup])
            del self.curr_real_indexes2index_name[indexes_table_tup]
        else:
            self.db_connector.drop_index(indexes_table_tup[0], indexes_table_tup[1])

        self.curr_real_indexes.remove(indexes_table_tup)



    def _get_cost(self, query):
        if self.cost_estimation == "whatif":
            return self.db_connector.get_cost(query)
        elif self.cost_estimation == "actual_runtimes":
            runtime = self.db_connector.exec_query(query)[0]
            return runtime

    def _request_cache(self, query, ref_cols, indexes):
        q_i_hash = (query, frozenset(indexes))
        if q_i_hash in self.relevant_indexes_cache:
            # print("compute relevant in cache: ", q_i_hash)
            relevant_indexes = self.relevant_indexes_cache[q_i_hash]
        else:
            relevant_indexes = self._relevant_indexes(query, ref_cols, indexes)
            self.relevant_indexes_cache[q_i_hash] = relevant_indexes
            # print("compute relevant not seen:", q_i_hash, "result:",relevant_indexes)
        # Check if query and corresponding relevant indexes in cache
        if (query, relevant_indexes) in self.cache:
            self.cache_hits += 1
            # print("db query in cache:", query, relevant_indexes)
            return self.cache[(query, relevant_indexes)]
        # If no cache hit request cost from database system
        else:
            cost = self._get_cost(query)
            self.cache[(query, relevant_indexes)] = cost
            # print("db query returned:", cost)
            return cost

    def _relevant_indexes(self, query, ref_cols, indexes):
        relevant_indexes = [
            # check if "table.col" in ref_cols
            x for x in indexes if any(x[1]+ "." + col in ref_cols for col in x[0])
        ]
        return frozenset(relevant_indexes)
