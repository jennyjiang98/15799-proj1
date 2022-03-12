import csv
import glob
import re
import itertools
import pickle
import glob
import os
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sql_metadata import Parser
from preprocessor import Preprocessor
from postgres_dbms import PostgresDatabaseConnector
from doit.action import CmdAction
from copy import deepcopy
from cost_evaluation import CostEvaluation
import random


# def task_project1():
#     # --workload_csv="${workload_csv}" --timeout="${TIME_ACTION_GENERATION}"
#     timeout?
#     file path?
#
#     return {
#         # A list of actions. This can be bash or Python callables.
#         "actions": sql_list,
#         # Always rerun this task.
#         "uptodate": [False],
#     }

def task_project1():
    """
    Generate actions.
    """
    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            'echo "Starting action generation."',
            generate,
            'echo \'{"VACUUM": false}\' > config.json',
        ],
        # Always rerun this task.
        "uptodate": [False],
        "verbosity": 2,
        "params": [
            {
                "name": "workload_csv",
                "long": "workload_csv",
                "help": "The PostgreSQL workload to optimize for.",
                "default": None,
            },
            {
                "name": "timeout",
                "long": "timeout",
                "help": "The time allowed for execution before this dodo task will be killed.",
                "default": None,
            },
        ],
    }


def task_project1_setup():
    return {
        "actions": [
            'sudo apt-get update',
            'sudo apt-get -y install python3-pip postgresql-14-hypopg',
            'sudo pip3 install psycopg2 sql_metadata pglast pandas pandarallel tqdm',
        ],
        "uptodate": [False],
    }


def generate(workload_csv, timeout):
    _PG_LOG_COLUMNS = [
        "log_time",
        "user_name",
        "database_name",
        "process_id",
        "connection_from",
        "session_id",
        "session_line_num",
        "command_tag",
        "session_start_time",
        "virtual_transaction_id",
        "transaction_id",
        "error_severity",
        "sql_state_code",
        "message",  # cols
        "detail",
        "hint",
        "internal_query",
        "internal_query_pos",
        "context",
        "query",
        "query_pos",
        "location",
        "application_name",
        "backend_type",
    ]
    # 参数
    cand_max_len = 3
    min_cost_improvement = 1.003
    max_dropping_cost_degrade = 1.003

    revert_threshold = 0.80
    drop_prefix_no_test = False
    drop_prefix_test = True
    first_round_benefit_thresh = 5

    db_connector = PostgresDatabaseConnector("project1db")
    # db_connector.drop_all_indexes() # drop 所有index, 不管是不是hypo "select indexname from pg_indexes where schemaname='public'"
    # select * from hypopg_reset() drop hypo，新链接没必要做

    # Set the random seed to obtain deterministic statistics (and cost estimations)
    # because ANALYZE (and alike) use sampling for large tables
    db_connector.create_statistics()
    db_connector.commit()

    result = db_connector.exec_fetch(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'",
        one=False)
    # print(result)
    table_names_dict = {k[0]: {} for k in result}
    table_column_dict = table_names_dict.copy()
    table_index_dict = table_names_dict.copy()
    # print("table_names_dict", result)

    for tab in table_column_dict.keys():
        cols = db_connector.exec_fetch(
            "SELECT column_name FROM information_schema.columns where table_name = '{}'".format(tab), one=False)
        table_column_dict[tab] = {k[0] for k in cols}

        indexes = {}
        curr_index_def = db_connector.exec_fetch(
            "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{}'".format(tab),
            one=False)  # schemaname = 'public'
        p1 = re.compile(r'[(](.*?)[)]', re.S)  # 最小匹配括号
        for row in curr_index_def:
            res = re.findall(p1, row[1])
            assert (len(res) == 1)
            cols = tuple([a.strip() for a in res[0].split(',')])
            indexes[cols] = row[0]
        # print(indexes)
        table_index_dict[tab] = indexes

    # print("curr table cols:", table_column_dict) # {'useracct': {'name', 'creation_date', 'u_id', 'email'}, 'item': {'i_id', 'creation_date', 'description', 'title'}, 'review': {'rank', 'u_id', 'i_id', 'rating', 'a_id', 'creation_date', 'comment'}, 'review_rating': {'last_mod_date', 'u_id', 'status', 'rating', 'creation_date', 'a_id', 'type', 'vertical_id'}, 'trust': {'creation_date', 'source_u_id', 'trust', 'target_u_id'}}
    print("curr table indexes:",
          table_index_dict)  # {'jungle': {('uuid_field',): 'jungle_pkey', ('int_field9', ' float_field6'): 'index_jungle_intfield9_floatfield6'}, 'sources': {('id',): 'sources_pkey', ('name',): 'sources_name_key'}, 'types': {('id',): 'types_pkey', ('category', ' name'): 'types_category_name_key'}, 'sessions': {('id',): 'sessions_pkey'}, 'observations': {}}

    preprocessor = Preprocessor(csvlogs=[workload_csv], log_columns=_PG_LOG_COLUMNS,
                                table_column_dict=table_column_dict)
    # df0 = preprocessor.get_dataframe()
    # print(df0.head(n=5).to_string())

    dfw = preprocessor.get_grouped_where_cnt()
    # print(dfw.to_string())

    dfo = preprocessor.get_grouped_order_by_cnt()
    # print(dfo.to_string())

    dfj = preprocessor.get_grouped_join_cnt()

    # print(dfj.to_string())

    def get_appeared_candidate_with_cnt_dict(names_dict, df, banned_set):
        # df给出的组合可能不来自同一table
        # 提取出现过的所有table，转化为同一table上的所有可能列组合
        # output format: {'useracct': {('u_id',): 61}, 'item': {('i_id',): 62}, 'review': {('i_id',): 99, ('i_id', 'u_id'): 58, ('u_id',): 30}, 'trust': {('source_u_id', 'target_u_id'): 88, ('source_u_id',): 37}}
        appeared_table_candidate_combination_dict = deepcopy(names_dict)
        for tup in df.index: # (col col col) appears together in a query
            tmp_tab_col_dict = {}
            cnt = df['count'][tup]
            for tab_col in tup:
                if tab_col in banned_set:
                    # ignore this col
                    continue
                lst = tab_col.split('.')
                tab = lst[0]
                col = lst[1]
                if tab in tmp_tab_col_dict.keys():
                    tmp_tab_col_dict[tab].append(col)  # sorted for groupby and join, not sorted for order by
                else:
                    tmp_tab_col_dict[tab] = [col]

            for table, cols in tmp_tab_col_dict.items():
                key = tuple(cols)
                appeared_table_candidate_combination_dict[table][key] = \
                    appeared_table_candidate_combination_dict[table][key] + cnt if key in \
                                                                                   appeared_table_candidate_combination_dict[
                                                                                       table].keys() else cnt
        # 删除appeared_table_candidate_combination_dict 的dict大小为0的
        return {a: b for a, b in appeared_table_candidate_combination_dict.items() if len(b) > 0}

    def permutate_candidate_dict_all_lengths_prefix_cnt(cand_dict):
        # input format: {'useracct': {('u_id',): 61}, 'item': {('i_id',): 62}, 'review': {('i_id',): 99, ('i_id', 'u_id'): 58, ('u_id',): 30}, 'trust': {('source_u_id', 'target_u_id'): 88, ('source_u_id',): 37}}
        # output will enumerate all subsets, since if the subset appear many times it will be generated. The count to be consistent.
        # {'useracct': {('u_id',): 61}, 'item': {('i_id',): 62}, 'review': {('i_id',): 157, ('u_id',): 88, ('i_id', 'u_id'): 58, ('u_id', 'i_id'): 58}, 'trust': {('source_u_id',): 125, ('target_u_id',): 88, ('source_u_id', 'target_u_id'): 88, ('target_u_id', 'source_u_id'): 88}}
        ret = cand_dict.copy()
        for tab, cands in cand_dict.items():
            permute_dict = {}
            for cand_w_underscore, cnt in cands.items():
                for num_cols in range(1, min(cand_max_len + 1, len(cand_w_underscore) + 1)):
                    for permute_tuple in itertools.permutations(cand_w_underscore, num_cols):
                        # print(tab, permute_tuple)
                        permute_dict[permute_tuple] = permute_dict[
                                                          permute_tuple] + cnt if permute_tuple in permute_dict.keys() else cnt
            ret[tab] = permute_dict
        return ret

    def merge_permutate_candidate_dict_all_lengths_prefix_cnt(sum_dict, cand_dict):
        ret = deepcopy(sum_dict)
        for tab, cands in cand_dict.items():
            if tab not in ret.keys():
                ret[tab] = {}
            for cand_w_underscore, cnt in cands.items():
                for num_cols in range(1, min(cand_max_len + 1, len(cand_w_underscore) + 1)):
                    for permute_tuple in itertools.permutations(cand_w_underscore, num_cols):
                        # print(tab, permute_tuple)
                        ret[tab][permute_tuple] = ret[tab][permute_tuple] + cnt if permute_tuple in ret[
                            tab].keys() else cnt
        return ret

    def merge_no_permutate_candidate_dict_prefix_cnt(sum_dict, cand_dict):
        # 注意：orderby prefix where里的col不能要
        ret = deepcopy(sum_dict)
        for tab, cands in cand_dict.items():
            if tab not in ret.keys():
                ret[tab] = {}
            for cand_w_underscore, cnt in cands.items():
                # orderby 修改 start_num = 带perfix个数+1
                start_num = 1
                for num_cols in range(start_num, min(cand_max_len + 1, len(cand_w_underscore) + 1)):
                    prefix_tuple = cand_w_underscore[:num_cols]
                    ret[tab][prefix_tuple] = ret[tab][prefix_tuple] + cnt if prefix_tuple in ret[tab].keys() else cnt
        return ret

    banned_set = preprocessor.get_banned_set()
    where_candidate_no_permutation = get_appeared_candidate_with_cnt_dict(table_names_dict, dfw, banned_set)
    # print("where_candidate_no_permutation: ", where_candidate_no_permutation) #{'useracct': {('u_id',): 61}, 'item': {('i_id',): 62}, 'review': {('i_id',): 99, ('i_id', 'u_id'): 58, ('u_id',): 30}, 'trust': {('source_u_id', 'target_u_id'): 88, ('source_u_id',): 37}}
    where_candidate_permutation = permutate_candidate_dict_all_lengths_prefix_cnt(where_candidate_no_permutation)
    # print("where_candidate_permutation: ", where_candidate_permutation)

    join_candidate_no_permutation = get_appeared_candidate_with_cnt_dict(table_names_dict, dfj, banned_set)
    # print("join_candidate_no_permutation: ", join_candidate_no_permutation)
    join_candidate_permutation = permutate_candidate_dict_all_lengths_prefix_cnt(join_candidate_no_permutation)
    # print("join_candidate_permutation: ", join_candidate_permutation)

    order_candidate_no_permutation = get_appeared_candidate_with_cnt_dict(table_names_dict, dfo, banned_set)
    print("order_candidate_no_permutation: ", order_candidate_no_permutation)

    merged_candidate = merge_permutate_candidate_dict_all_lengths_prefix_cnt(where_candidate_permutation,
                                                                             join_candidate_no_permutation)
    # print("merged_candidate: ", merged_candidate)

    merged_candidate = merge_no_permutate_candidate_dict_prefix_cnt(merged_candidate, order_candidate_no_permutation)
    print("merged_candidate_with_o: ", merged_candidate)

    # 处理merged_candidate，按cnt高低确定搜索顺序. 元素格式：(cnt, cols, table), 第一个相等的按第二个排序
    possible_permute_cand_sort_list = []
    for tab, cands in merged_candidate.items():
        for cand, cnt in cands.items():
            possible_permute_cand_sort_list.append((cnt, cand, tab))

    possible_permute_cand_sort_list.sort(key=lambda x: (-x[0], len(x[1])))
    # print("possible_permute_cand_sort_list: ",possible_permute_cand_sort_list)

    # 带每个template count的
    # df1 = preprocessor.get_grouped_dataframe_interval() # sorted by cnts,
    # print(df1.to_string())

    # 行数
    ratio = min(1, 1000 / len(preprocessor.get_dataframe().index))
    sampled_workload = preprocessor.get_sampled_rows(ratio)  # sample by this number!!!可以计算累加百分位数做，带上where claus
    wlkd_size = len(sampled_workload.index)
    print("sampled workload size: ", wlkd_size)
    # print(sampled_workload.to_string())
    # 爆搜策略开始
    # 每次开始，要更新cost eval那里记录的当前所有index为db上有的那些index。需要解决：上一轮建立好了，咋drop的问题---db会重置，需要记录drop操作
    # 以下集合包括table_index_dict，因为可能在延长前缀
    searched_candidate = set()
    related_curr_table_cols = set()
    related_curr_table_cols2index_name = {}
    for tab in merged_candidate.keys():
        related_curr_table_cols.update([(cols, tab) for cols in table_index_dict[tab].keys()])
        related_curr_table_cols2index_name.update([((cols, tab), v) for cols, v in table_index_dict[tab].items()])

    print("related curr real indexes:", related_curr_table_cols)
    print("related curr real indexes2index_name:", related_curr_table_cols2index_name)
    current_best_cols = deepcopy(related_curr_table_cols)  # 不用dump
    searched_candidate.update(current_best_cols)

    # TODO: more db connectors parallelism
    cost_eval = CostEvaluation(db_connector, related_curr_table_cols, related_curr_table_cols2index_name,
                               sampled_workload)

    # tmp = set()#related_curr_table_cols.copy()
    # tmp.add((('int_field5',), 'jungle'))
    # current_best_cost = cost_eval.calculate_cost(tmp)
    # print(current_best_cost)

    possible_permute_cand_sorted_set = set([(cand[1], cand[2]) for cand in possible_permute_cand_sort_list])
    utilized_indexes_benefits, utilized_indexes_old, query_details, current_indexes_cost, potential_better_cost = cost_eval.get_wkld_utilized_indexes_improvement(
        possible_permute_cand_sorted_set)
    print("utilized_indexes_benefits", utilized_indexes_benefits)
    # only keep new indexes
    utilized_new_hypo_indexes_benefits = {k: v for k, v in utilized_indexes_benefits.items() if
                                          k not in related_curr_table_cols}
    print("utilized_new_hypo_indexes_benefits", utilized_new_hypo_indexes_benefits)
    # no longer user indexes
    new_setting_not_utilized_real_indexes = set([k for k in related_curr_table_cols if k not in utilized_indexes_benefits.keys()])
    print("new_setting_not_utilized_real_indexes", new_setting_not_utilized_real_indexes)
    # old setting not utilized indexes
    old_setting_not_utilized_real_indexes = related_curr_table_cols- utilized_indexes_old
    print("old_setting_not_utilized_real_indexes", old_setting_not_utilized_real_indexes)
    
    sorted_benefits = [(k, v) for k, v in utilized_new_hypo_indexes_benefits.items()]
    sorted_benefits.sort(key=lambda x: (-x[1], len(x[0][0])))  # least cols first

    # print(query_details)
    # print(current_best_cost)

    # current_best_cost = cost_eval.calculate_cost(related_curr_table_cols)
    # print(current_best_cost)

    is_firstround = False
    round_number = 1
    try:
        with open('mystate.pkl', 'rb') as f:
            dump_vars = pickle.load(f)
        # use new parsed tables to check if appeared before
        print("load success")
        for cols, tab in possible_permute_cand_sorted_set: # a index tup list
            if tab not in dump_vars["best_single_cost_on_table"].keys(): # new combination of tab and cols
                is_firstround = True
                print("is first round! seeing not seen table: ", tab)
                break
    except IOError:
        is_firstround = True

    if not is_firstround:
        print("is not first round")

    to_drop_list = set()  # [(col, tab)]
    to_build_list = set()
    to_cluster_list = set()

    if is_firstround:
        best_single_cost_on_table = {}  # cost, clustered_index cols
        best_subsumed_cost_on_table = {}
        clustered_on_table = {}

        print("round", round_number)
        # pick all indexes over first_round_benefit_thresh
        print("sorted_benefits", sorted_benefits)
        for col_tup, benefit in sorted_benefits:
            cols, tab = col_tup
            if tab not in best_single_cost_on_table.keys(): # but the largest on table
                to_build_list.add(col_tup)
                print("adding:", col_tup)
                best_single_cost_on_table[tab] = (benefit, cols)

            elif benefit > first_round_benefit_thresh * wlkd_size:
                to_build_list.add(col_tup)
                print("adding:", col_tup)
            else:
                print("not enough:", col_tup)

        # clustering：simple heuristic. All subsuming cost (from db2advisor paper) get merged makes sense here.
        subsumed_benefits = deepcopy(utilized_new_hypo_indexes_benefits)
        index_benefits_to_remove = set()
        for high_ratio_pos, index_benefit_high_ratio in enumerate(sorted_benefits):
            if index_benefit_high_ratio in index_benefits_to_remove:
                continue
            # Test all following elements (with lower ratios) in the list
            iteration_pos = high_ratio_pos + 1
            cols, tab = index_benefit_high_ratio[0]
            for index_benefit_lower_ratio in sorted_benefits[iteration_pos:]:
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
        # at least one index is in both best_single_cost_on_table and to_build, so cluster_list must be an index already
        # Now, extend the clustered index to be the longest possible prefix
        print("best_subsumed_cost_on_table: ", best_subsumed_cost_on_table)
        sorted_possible_clusters = [(k, v) for k, v in subsumed_cols_and_benefits.items()]
        sorted_possible_clusters.sort(key=lambda x: (-x[1], len(x[0][0])))  # least cols first
        for col_tup, benefit in sorted_possible_clusters: # extend all prefixes by the earliest appeared index
            cols, tab = col_tup
            if tab in clustered_on_table.keys():
                prefix = clustered_on_table[tab][1] # previously seen a better one
            else:
                prefix = best_single_cost_on_table[tab][1] # here we use single to avoid random but large costs for multi col indexes
            if tuple(cols[:len(prefix)]) == prefix:
                print("better or curr cluster index for table: ", tab, cols)
                clustered_on_table[tab] = (benefit, cols)
        
        to_cluster_list.update([(benefit_cols[1], tab) for tab, benefit_cols in clustered_on_table.items()])
        # 更新best, 第一轮可以keep real
        # current_best_reals_hypo_cost = cost_eval.calculate_cost(to_build_list, keep_real=True)
        current_best_reals_hypo_cost = -1
        current_best_built_cols = to_build_list
        current_best_real_result = -1
        to_drop_list = old_setting_not_utilized_real_indexes
        dump_vars = {} # new for 1st round
    else:
        searched_candidate.update(dump_vars["searched_candidate"]) # set of (cols, tab)
        round_number = dump_vars["round_number"] + 1
        best_single_cost_on_table = dump_vars["best_single_cost_on_table"]
        best_subsumed_cost_on_table = dump_vars["best_subsumed_cost_on_table"]
        clustered_on_table = dump_vars["clustered_on_table"]
        current_best_reals_hypo_cost = dump_vars["current_best_reals_hypo_cost"]
        current_best_built_cols = dump_vars["current_best_built_cols"]
        current_best_real_result = dump_vars["current_best_real_result"]
        print("round", round_number)
        revert = False

        # might be buggy: how to match bench with current tesing bench??
        if round_number > 2:
            try:
                print("checking results", round_number)
                summary_path_format = './grading/iteration_{}/*summary.json'.format(round_number-1)
                files = glob.glob(summary_path_format)
                max_file = max(files, key=os.path.getctime)
                with open(max_file, 'r') as f:
                    dict_new = json.load(f)
                # 读取dict["Goodput (requests/second)"]

                summary_path_format = './grading/iteration_{}/*summary.json'.format(round_number-2)
                files = glob.glob(summary_path_format)
                max_file_prev = max(files, key=os.path.getctime)
                with open(max_file_prev, 'r') as f:
                    dict_old = json.load(f)
                # same benchmark
                bench1 = max_file.split('_')[1].split('/')[1]
                bench2 = max_file_prev.split('_')[1].split('/')[1]
                print(bench1, bench2)
                if bench1 == bench2:
                    print("new and old throughput: ", dict_new["Goodput (requests/second)"], dict_old["Goodput (requests/second)"])
                    if dict_new["Goodput (requests/second)"] < revert_threshold * dict_old["Goodput (requests/second)"]:
                        print("big degrade! ")
                        to_drop_list.update(dump_vars["to_build_list_" + str(round_number-1)])
                        to_build_list.update(dump_vars["to_drop_list_" + str(round_number-1)])
                        if len(to_drop_list) !=0 or len(to_build_list) !=0:
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
                pass

        if revert == False: # can do some exploring
            # 在真正build后，本来收益并不大的会出现--因为build和假象有差距 比如trust (source_u_id,target_u_id)和反过来，所以再build一次看看吧
            print(sorted_benefits)
            for col_tup, benefit in sorted_benefits:
                if col_tup in searched_candidate:
                    print("searched, ignoring:", col_tup)
                    continue
                cols, tab = col_tup
                if tab not in best_single_cost_on_table.keys() or benefit > best_single_cost_on_table[tab][0]:
                    print("new single best, adding and clustering on:", col_tup)
                    to_build_list.add(col_tup)
                    to_cluster_list = set([k for k in to_cluster_list if k[1] != tab]) # remove old one
                    to_cluster_list.add(col_tup)
                    clustered_on_table[tab] = (benefit, cols) # changing prefix, the following rounds will extend it
                else: # seen and not best
                    prefix = clustered_on_table[tab][1]
                    if cols[:len(prefix)] == prefix:
                        print("clustered is prefix, consider adding:", col_tup)
                        to_build_list.add(col_tup)
                        to_cluster_list = set([k for k in to_cluster_list if k[1] != tab]) # remove old one
                        to_cluster_list.add(col_tup)
                        clustered_on_table[tab] = (benefit + clustered_on_table[tab][0], cols)
                    elif benefit > first_round_benefit_thresh * wlkd_size:
                        to_build_list.add(col_tup)
                        print("over threshold, consider adding:", col_tup)
                    else:
                        print("not enough, not consider:", col_tup)
            if len(to_build_list) == 0:
                # 本轮utilized都搜过了
                # 再看prefix，很可能没啥用。。。
                # 控制变量drop index

                for index_tup in old_setting_not_utilized_real_indexes: # does happen, especially hypopg made a lot of mistake at start
                    cols, tab = index_tup
                    if tab not in clustered_on_table.keys():
                        continue # let's assume this will not happen....
                    if cols == clustered_on_table[tab]:
                        continue # let's assume this will not happen, we should not simply revert to its prefix since it may never appeared....
                    to_drop_list.add(index_tup)
                # 还是看如果drop。且cluster index在他上，退回best single column。有可能prefix不好.此时重新cluster？

                # TODO: Orderby列 CLUSTER review using index_i_id_creation_date_backwards; 爆搜结束的轮做orderby cluster实验

    def dict_to_actions_sql(to_build, to_drop, to_cluster):
        # format of to_build: candidate list (cols, tab)
        # format of to_drop: candidate list (cols, tab)
        actions_sql_list = [
            # "CREATE xxx"
        ]
        # build里出现的，一定没在当前数据库里，每个cand只会被检查一次。如果同时出现在了 build里，一定是本轮先加入的，那么从to build删除
        # drop first, then cluster, then create, to modify less indexes
        for cols, tab in to_drop:
            names = ",".join(cols)
            statement = (
                f"drop index {'index_' + tab + '_' + '_'.join(cols)} ;"
            )
            actions_sql_list.append(statement)

        both = to_build.intersection(to_cluster)
        to_cluster -= both
        to_build -= both

        for cols, tab in to_cluster:
            names = ",".join(cols)
            statement = (
                f"cluster {tab} using {'index_' + tab + '_' + '_'.join(cols)};"
            )
            actions_sql_list.append(statement)

        for ele in both:
            cols, tab = ele
            names = ",".join(cols)
            statement = (
                f"create index if not exists {'index_' + tab + '_' + '_'.join(cols)} "
                f"on {tab} ({names});"
            )
            actions_sql_list.append(statement)

            statement = (
                f"cluster {tab} using {'index_' + tab + '_' + '_'.join(cols)};"
            )
            actions_sql_list.append(statement)

        for cols, tab in to_build:
            names = ",".join(cols)
            statement = (
                f"create index if not exists {'index_' + tab + '_' + '_'.join(cols)} "
                f"on {tab} ({names});"
            )
            actions_sql_list.append(statement)
        # no modify on the original lists
        to_cluster |= both
        to_build |= both
        return actions_sql_list

    searched_candidate.update(to_build_list)
    both = to_build_list.intersection(to_drop_list)
    to_build_list -= both
    to_drop_list -= both
    print("to build list: ", to_build_list)
    print("to drop list: ", to_drop_list)
    print("to cluster list: ", to_cluster_list)

    actions_sql_list = dict_to_actions_sql(to_build_list, to_drop_list, to_cluster_list)
    with open("actions.sql", 'w') as f:
        f.writelines('\n'.join(actions_sql_list))

    # dumping

    dump_vars.update({"round_number": round_number, "searched_candidate": searched_candidate,
                 "best_single_cost_on_table": best_single_cost_on_table,
                 "best_subsumed_cost_on_table": best_subsumed_cost_on_table,
                 "to_build_list_" + str(round_number): to_build_list,
                 "to_drop_list_" + str(round_number): to_drop_list,
                 "to_cluster_list_" + str(round_number): to_cluster_list,
                 "clustered_on_table": clustered_on_table,
                 "current_best_built_cols" : current_best_built_cols,
                 "current_best_reals_hypo_cost":current_best_reals_hypo_cost,
                 "current_best_real_result": current_best_real_result,
    })
    print(dump_vars)

    with open('mystate.pkl', 'wb') as f:
        pickle.dump(dump_vars, f)

