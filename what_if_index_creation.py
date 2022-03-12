# adapted from hyrise: https://github.com/hyrise/index_selection_evaluation
import logging

# Class that encapsulates simulated/WhatIf-Indexes.
# This is usually used by the CostEvaluation class and there should be no need
# to use it manually.
# Uses hypopg for postgreSQL
class WhatIfIndexCreation:
    def __init__(self, db_connector):
        logging.debug("Init WhatIfIndexCreation")

        self.tup2hypo_name = {}
        self.tup2hypo_oid = {}
        self.db_connector = db_connector

    def get_hypo_name(self, index_table_tup):
        return self.tup2hypo_name[index_table_tup]

    def simulate_index(self, index_table_tup):
        result = self.db_connector.simulate_index(index_table_tup[0], index_table_tup[1])
        index_oid = result[0]
        index_name = result[1]
        # print("inside simulate return: ", index_name)
        self.tup2hypo_name[index_table_tup] = index_name
        self.tup2hypo_oid[index_table_tup] = index_oid
        # if store_size:
        #     potential_index.estimated_size = self.estimate_index_size(index_oid)

    def drop_simulated_index(self, index_table_tup):
        oid = self.tup2hypo_oid[index_table_tup]
        self.db_connector.drop_simulated_index(oid)
        del self.tup2hypo_oid[index_table_tup]
        del self.tup2hypo_name[index_table_tup]

    def all_simulated_indexes(self):
        statement = "select * from hypopg_list_indexes"
        indexes = self.db_connector.exec_fetch(statement, one=False)
        return indexes

    def estimate_index_size(self, index_oid):
        statement = f"select hypopg_relation_size({index_oid})"
        result = self.db_connector.exec_fetch(statement)[0]
        assert result > 0, "Hypothetical index does not exist."
        return result

