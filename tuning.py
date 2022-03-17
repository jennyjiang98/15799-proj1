# main process of tuning
# parameters
min_cost_improvement = 1.003
max_dropping_cost_degrade = 1.003

revert_threshold = 0.80
drop_prefix_no_test = False
drop_prefix_test = True
first_round_benefit_thresh = 5
wkld_sample_size = 1000


def generate(workload_csv, timeout):
    import pickle

    from preprocessor import Preprocessor
    from postgres_dbms import PostgresDatabaseConnector
    from copy import deepcopy
    from cost_evaluation import CostEvaluation
    from utils import Index, get_appeared_candidate_with_cnt_dict, permutate_candidate_dict_all_lengths_prefix_cnt, \
        merge_permutate_candidate_dict_all_lengths_prefix_cnt, merge_no_permutate_candidate_dict_prefix_cnt, \
        dict_to_actions_sql
    import random

    db_connector = PostgresDatabaseConnector("project1db")
    # db_connector.drop_all_indexes()

    # get current tables and indexes info
    table_names_dict, table_column_dict, table_index_dict = db_connector.get_table_info_dicts()
    print("curr table indexes:", table_index_dict)

    # preprocess data, parse tables using the fetched table_column_dict
    preprocessor = Preprocessor(csvlogs=[workload_csv],
                                table_column_dict=table_column_dict)
    merged_candidate = get_column_candidates(preprocessor, table_names_dict)

    seen_tables = set(merged_candidate.keys())

    # sort merged_candidate by cnt and len of cols
    # related current table possible columns for generating hypoindexes: we only consider tables used in this workload.csv
    possible_permute_cand_sort_list = []
    for tab, cands in merged_candidate.items():
        for cand, cnt in cands.items():
            possible_permute_cand_sort_list.append((cnt, cand, tab))

    possible_permute_cand_sort_list.sort(key=lambda x: (-x[0], len(x[1])))
    print("possible_permute_cand_sort_list: ", possible_permute_cand_sort_list)

    # searched_candidate: to ensure same index candidate is not build twice
    searched_candidate = set()

    # real triples on the current DB
    related_curr_table_triples2index_obj = {}
    for tab in merged_candidate.keys():
        searched_candidate.update([(cols, tab) for cols in table_index_dict[tab].keys()])
        for cols, index_objs in table_index_dict[tab].items():
            related_curr_table_triples2index_obj.update([(v.key, v) for v in index_objs])

    print("related_curr_table_triples2index_obj:", related_curr_table_triples2index_obj)

    sampled_workload = get_sampled_workload(preprocessor)

    wlkd_size = len(sampled_workload.index)
    print("sampled workload size: ", wlkd_size)

    # the final product from queries: cols_table_tuple candidates!
    possible_permute_cand_sorted_set = set([(cand[1], cand[2]) for cand in possible_permute_cand_sort_list])
    is_firstround, dump_vars = is_first_round(possible_permute_cand_sorted_set)
    if is_firstround:
        db_connector.create_statistics()
        db_connector.commit()

    # Get utilized indexes
    cost_eval = CostEvaluation(db_connector, related_curr_table_triples2index_obj,
                               sampled_workload)
    utilized_indexes_benefits, utilized_indexes_old, query_details, current_indexes_cost, potential_better_cost = cost_eval.get_wkld_utilized_indexes_improvement(
        possible_permute_cand_sorted_set)
    print("utilized_indexes_benefits", utilized_indexes_benefits)
    # only keep new indexes
    curr_real_index_set = set(related_curr_table_triples2index_obj.keys())
    utilized_new_hypo_indexes_benefits = {k: v for k, v in utilized_indexes_benefits.items() if
                                          k not in curr_real_index_set}
    print("utilized_new_hypo_indexes_benefits", utilized_new_hypo_indexes_benefits)
    # no longer used indexes in the hypothetical new setting
    new_setting_not_utilized_real_indexes = set(
        [k for k in curr_real_index_set if k not in utilized_indexes_benefits.keys()])
    print("new_setting_not_utilized_real_indexes", new_setting_not_utilized_real_indexes)
    # old setting not utilized indexes
    old_setting_not_utilized_real_indexes = curr_real_index_set - utilized_indexes_old
    print("old_setting_not_utilized_real_indexes", old_setting_not_utilized_real_indexes)

    sorted_benefits = [(k, v) for k, v in utilized_new_hypo_indexes_benefits.items()]
    sorted_benefits.sort(key=lambda x: (-x[1], len(x[0][0])))  # least cols first

    col_tab2new_benefits = {}
    for k, v in utilized_new_hypo_indexes_benefits.items():
        col_tup = (k[0], k[1])
        col_tab2new_benefits[col_tup] = col_tab2new_benefits[
                                            col_tup] + v if col_tup in col_tab2new_benefits.keys() else v
    sorted_benefits_no_type = [(k, v) for k, v in col_tab2new_benefits.items()]
    sorted_benefits_no_type.sort(key=lambda x: (-x[1], len(x[0][0])))

    to_drop_list = set()  # set of triples
    to_build_list = set()
    to_cluster_list = set()
    round_number = 1

    # Selection logic. The first round generates all candidates from utilized hypo indexes, and following rounds do adjustments

    # Determine whether this is the first round for a benchmark
    if is_firstround:
        dump_vars = {}  # new for 1st round
        best_col_combination_on_table_cost = {}  # cost, for btrees only, for selecting clustered_index cols. format: {"table": (benefit, (cols))}
        best_subsumed_cost_on_table = {}
        clustered_on_table = {}

        print("round", round_number)
        # pick all indexes over first_round_benefit_thresh
        print("sorted_benefits", sorted_benefits)
        for triple_tup, benefit in sorted_benefits:
            cols, tab, type = triple_tup
            if benefit > first_round_benefit_thresh * wlkd_size:
                to_build_list.add(triple_tup)
                print("adding:", triple_tup)
            else:
                print("not enough:", triple_tup)

        # All following are clustering logic：simple heuristic
        for col_tup, benefit in sorted_benefits_no_type:
            cols, tab = col_tup
            if tab not in best_col_combination_on_table_cost.keys():
                print("found best single col combination:", col_tup)
                best_col_combination_on_table_cost[tab] = (benefit, cols)

        # All subsuming cost (from db2advisor paper) get merged makes sense here.
        subsumed_benefits = deepcopy(col_tab2new_benefits)
        index_benefits_to_remove = set()
        for high_ratio_pos, index_benefit_high_ratio in enumerate(sorted_benefits_no_type):
            if index_benefit_high_ratio in index_benefits_to_remove:
                continue
            # Check all following elements (with lower ratios) in the list whether they're included by the current cols
            iteration_pos = high_ratio_pos + 1
            cols, tab = index_benefit_high_ratio[0]
            for index_benefit_lower_ratio in sorted_benefits_no_type[iteration_pos:]:
                if index_benefit_lower_ratio[0][1] != tab:
                    continue
                if index_benefit_lower_ratio in index_benefits_to_remove:
                    continue
                prefix = index_benefit_lower_ratio[0][0]
                if tuple(cols[:len(prefix)]) == prefix:  # is_prefix
                    subsumed_benefits[index_benefit_high_ratio[0]] += subsumed_benefits[index_benefit_lower_ratio[0]]
                    index_benefits_to_remove.add(index_benefit_lower_ratio[0])
        print("index_benefits_to_remove", index_benefits_to_remove)
        subsumed_cols_and_benefits = {k: v for k, v in subsumed_benefits.items() if k not in index_benefits_to_remove}
        for col_tup, benefit in subsumed_cols_and_benefits.items():
            cols, tab = col_tup
            if tab not in best_subsumed_cost_on_table.keys() or benefit > best_subsumed_cost_on_table[tab][0]:
                best_subsumed_cost_on_table[tab] = (benefit, cols)
        # at least one index is in both best_col_combination_on_table_cost and to_build, so cluster_list must be an index already
        # Now, extend the clustered index to be the longest possible prefix
        print("best_subsumed_cost_on_table: ", best_subsumed_cost_on_table)
        sorted_possible_clusters = [(k, v) for k, v in subsumed_cols_and_benefits.items()]
        sorted_possible_clusters.sort(key=lambda x: (-x[1], len(x[0][0])))  # least cols first
        for col_tup, benefit in sorted_possible_clusters:  # extend all prefixes by the earliest appeared index
            cols, tab = col_tup
            if tab in clustered_on_table.keys():
                prefix = clustered_on_table[tab][1]  # previously seen a better one
            else:
                prefix = best_col_combination_on_table_cost[tab][1]
                # here we use single to avoid random but large costs for multi col indexes. such as source id shoud be better than target id, but (s, t) can appear in either way with great cost
            if tuple(cols[:len(prefix)]) == prefix:
                print("better or curr cluster index for table: ", tab, cols)
                clustered_on_table[tab] = (benefit, cols)

        to_cluster_list.update([(benefit_cols[1], tab, 'btree') for tab, benefit_cols in clustered_on_table.items()])
        # Finally, add some btree indexes. in case the corresponding cluster's btree is not built, build them
        to_build_list.update([(benefit_cols[1], tab, 'btree') for tab, benefit_cols in clustered_on_table.items() if
                              (benefit_cols[1], tab, 'btree') not in curr_real_index_set])

        # hash are good for strings but often benefit not great enough from hypopg
        # first round I hack: re-check if strings are added hash indexes, if not used will drop later anyway.
        for cols, tab, type in utilized_indexes_benefits.keys():
            if len(cols) == 1 and db_connector.is_col_varchar(cols[0], tab):
                print("to hash adding: ", cols, tab)
                to_build_list.add((cols, tab, 'hash'))

        current_best_reals_hypo_cost = -1
        current_best_built_cols = to_build_list
        current_best_real_result = -1
        to_drop_list = old_setting_not_utilized_real_indexes

    else:
        # is not first round, do reverts / minor adjustments
        searched_candidate.update(dump_vars["searched_candidate"])  # set of (cols, tab)
        round_number = dump_vars["round_number"] + 1
        best_col_combination_on_table_cost = dump_vars["best_col_combination_on_table_cost"]
        best_subsumed_cost_on_table = dump_vars["best_subsumed_cost_on_table"]
        clustered_on_table = dump_vars["clustered_on_table"]
        current_best_reals_hypo_cost = dump_vars["current_best_reals_hypo_cost"]
        current_best_built_cols = dump_vars["current_best_built_cols"]
        current_best_real_result = dump_vars["current_best_real_result"]
        print("round", round_number)
        revert = False

        # might be buggy: sometimes there's variance. Also how to match bench with current tesing bench??
        # if round_number > 2:
        #     revert = check_revert(round_number, dump_vars, to_drop_list, to_build_list, curr_real_index_set)

        if revert == False:  # can do some exploring
            print("sorted_benefits_no_type", sorted_benefits_no_type)
            print("sorted_benefits", sorted_benefits)
            # check for new building
            for triple_tup, benefit in sorted_benefits:
                cols, tab, type = triple_tup
                if benefit > first_round_benefit_thresh * wlkd_size:
                    to_build_list.add(triple_tup)
                    print("over threshold, adding:", triple_tup)
                else:
                    print("not enough:", triple_tup)

            # check for new clustering
            for col_tup, benefit in sorted_benefits_no_type:
                # if actions.sql failed is lost then this would be a problem?
                if col_tup in searched_candidate:
                    print("searched, ignoring:", col_tup)
                    continue
                cols, tab = col_tup
                if tab not in best_col_combination_on_table_cost.keys() or benefit > \
                        best_col_combination_on_table_cost[tab][0]:
                    print("new single best, adding and clustering on:", col_tup)
                    if (cols, tab, 'btree') not in curr_real_index_set:
                        to_build_list.add((cols, tab, 'btree'))
                    to_cluster_list = set([k for k in to_cluster_list if k[1] != tab])  # remove old one chosen
                    to_cluster_list.add((cols, tab, 'btree'))
                    best_col_combination_on_table_cost[tab] = (benefit, cols)
                    clustered_on_table[tab] = (benefit, cols)  # changing prefix, the following rounds will extend it
                else:  # seen and not best
                    prefix = clustered_on_table[tab][1]
                    if cols[:len(prefix)] == prefix:
                        print("clustered is prefix, consider adding:", col_tup)
                        if (cols, tab, 'btree') not in curr_real_index_set:
                            to_build_list.add((cols, tab, 'btree'))
                        to_cluster_list = set([k for k in to_cluster_list if k[1] != tab])  # remove old one
                        to_cluster_list.add((cols, tab, 'btree'))
                        clustered_on_table[tab] = (benefit + clustered_on_table[tab][0], cols)

            # Finally, if no building action, we try drop
            if len(to_build_list) == 0:
                for triple in old_setting_not_utilized_real_indexes:  # does happen, especially hypopg made a lot of mistake at start
                    cols, tab, type = triple
                    if type == 'btree':  # check if clustering has to be changed
                        if tab not in clustered_on_table.keys():
                            continue  # let's assume this will not happen....
                        # if dropping a clustered index, consider prefix
                        if cols == clustered_on_table[tab][1]:
                            if cols == best_col_combination_on_table_cost[tab][1]:
                                print("best combination, probably shadowed by hash index", triple)
                                continue
                            if len(cols) > 1:
                                print(cols, "clustered is not used, go back to prefix")
                                prefix = cols[:-1]  # back to prefix. this is always derived from best single col
                                if (prefix, tab, 'btree') not in curr_real_index_set:
                                    to_build_list.add((prefix, tab, 'btree'))
                                clustered_on_table[tab] = (best_col_combination_on_table_cost[tab][0], prefix)
                                to_cluster_list.add((prefix, tab, 'btree'))
                            else:  # if len==1, probably due to using the hash index, but we want to keep the clustering
                                print("won't drop since it's a single clustered index", triple)
                                continue
                    # a hash or cluster resolved, we can drop
                    # prefix can be extended next time
                    to_drop_list.add(triple)

    searched_candidate.update(to_build_list)
    searched_candidate.update(to_drop_list)
    searched_candidate.update(to_cluster_list)

    to_build_list -= curr_real_index_set
    both = to_build_list.intersection(to_drop_list)
    to_build_list -= both
    to_drop_list -= both
    to_cluster_list -= both

    print("to build list: ", to_build_list)
    print("to drop list: ", to_drop_list)
    print("to cluster list: ", to_cluster_list)

    actions_sql_list = dict_to_actions_sql(to_build_list, to_drop_list, to_cluster_list,
                                           related_curr_table_triples2index_obj, db_connector)
    with open("actions.sql", 'w') as f:
        f.writelines('\n'.join(actions_sql_list))

    with open("config.json", 'w') as f:
        if is_firstround or (len(preprocessor.get_banned_set()) > 0 and round_number % 3 == 0):
            f.writelines('{"VACUUM": true, "RESTART": true}')
        else:
            f.writelines('{"VACUUM": false, "RESTART": true}')

    dump_vars.update({"round_number": round_number, "searched_candidate": searched_candidate,
                      "seen_tables": seen_tables,
                      "best_col_combination_on_table_cost": best_col_combination_on_table_cost,
                      "best_subsumed_cost_on_table": best_subsumed_cost_on_table,
                      "to_build_list_" + str(round_number): to_build_list,
                      "to_drop_list_" + str(round_number): to_drop_list,
                      "to_cluster_list_" + str(round_number): to_cluster_list,
                      "clustered_on_table": clustered_on_table,
                      "current_best_built_cols": current_best_built_cols,
                      "current_best_reals_hypo_cost": current_best_reals_hypo_cost,
                      "current_best_real_result": current_best_real_result,
                      })
    print(dump_vars)

    with open('mystate.pkl', 'wb') as f:
        pickle.dump(dump_vars, f)


def get_column_candidates(preprocessor, table_names_dict):
    from utils import Index, get_appeared_candidate_with_cnt_dict, permutate_candidate_dict_all_lengths_prefix_cnt, \
        merge_permutate_candidate_dict_all_lengths_prefix_cnt, merge_no_permutate_candidate_dict_prefix_cnt, \
        dict_to_actions_sql
    # get column candidates which appeared together. Here I considered where/orderby/join/groupby
    print('whole csv len:', len(preprocessor.get_dataframe()))
    dfw = preprocessor.get_grouped_where_cnt()
    dfo = preprocessor.get_grouped_order_by_cnt()
    dfj = preprocessor.get_grouped_join_cnt()
    dfg = preprocessor.get_grouped_group_by_cnt()
    banned_set = preprocessor.get_banned_set()
    where_candidate_no_permutation = get_appeared_candidate_with_cnt_dict(table_names_dict, dfw, banned_set)
    where_candidate_permutation = permutate_candidate_dict_all_lengths_prefix_cnt(where_candidate_no_permutation)
    join_candidate_no_permutation = get_appeared_candidate_with_cnt_dict(table_names_dict, dfj, banned_set)
    order_candidate_no_permutation = get_appeared_candidate_with_cnt_dict(table_names_dict, dfo, banned_set)
    groupby_candidate_no_permutation = get_appeared_candidate_with_cnt_dict(table_names_dict, dfg, banned_set)

    merged_candidate = merge_permutate_candidate_dict_all_lengths_prefix_cnt(where_candidate_permutation,
                                                                             join_candidate_no_permutation)
    merged_candidate = merge_no_permutate_candidate_dict_prefix_cnt(merged_candidate, order_candidate_no_permutation)
    merged_candidate = merge_no_permutate_candidate_dict_prefix_cnt(merged_candidate, groupby_candidate_no_permutation)
    return merged_candidate


def get_sampled_workload(preprocessor):
    # Get workloads sampled by template
    ratio = min(1, wkld_sample_size / len(preprocessor.get_dataframe().index))
    return preprocessor.get_sampled_rows_by_template(ratio)


def is_first_round(possible_permute_cand_sorted_set):
    import pickle
    is_firstround = False
    try:
        with open('mystate.pkl', 'rb') as f:
            dump_vars = pickle.load(f)
        # use new parsed tables to check if appeared before
        print("load success")
        for cols, tab in possible_permute_cand_sorted_set:  # a index tup list
            if tab not in dump_vars["seen_tables"]:  # new combination of tab and cols
                is_firstround = True
                print("is first round! seeing not seen table: ", tab)
                break
    except IOError:
        is_firstround = True

    if not is_firstround:
        print("is not first round")
        return is_firstround, dump_vars
    else:
        print("is first round")
        return is_firstround, {}


def check_revert(round_number, dump_vars, to_drop_list, to_build_list, curr_real_index_set):
    import glob
    import json
    import os

    revert = False
    try:
        print("checking results", round_number)
        summary_path_format = './grading/iteration_{}/*summary.json'.format(round_number - 1)
        files = glob.glob(summary_path_format)
        max_file = max(files, key=os.path.getctime)
        with open(max_file, 'r') as f:
            dict_new = json.load(f)
        summary_path_format = './grading/iteration_{}/*summary.json'.format(round_number - 2)
        files = glob.glob(summary_path_format)
        max_file_prev = max(files, key=os.path.getctime)
        with open(max_file_prev, 'r') as f:
            dict_old = json.load(f)
        # same benchmark
        bench1 = max_file.split('_')[1].split('/')[1]
        bench2 = max_file_prev.split('_')[1].split('/')[1]
        print(bench1, bench2)
        if bench1 == bench2:
            print("new and old throughput: ", dict_new["Goodput (requests/second)"],
                  dict_old["Goodput (requests/second)"])
            if dict_new["Goodput (requests/second)"] < revert_threshold * dict_old["Goodput (requests/second)"]:
                print("big degrade! ")
                to_drop_list.update(dump_vars["to_build_list_" + str(round_number - 1)])
                to_build_list.update([k for k in dump_vars["to_drop_list_" + str(round_number - 1)] if
                                      k not in curr_real_index_set])
                if len(to_drop_list) != 0 or len(to_build_list) != 0:
                    print("not empty, do revert")
                    revert = True
                else:
                    print("empty, return to regular stuff")
                    # if no op, revert = false
            else:
                print("no big degrade")
        else:
            print("benchmark not same")
    except Exception as e:
        print("open files failed")

    return revert
