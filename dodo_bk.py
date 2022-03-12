import re
import itertools

from preprocessor import Preprocessor
from postgres_dbms import PostgresDatabaseConnector
from copy import deepcopy
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
            'sudo apt-get install postgresql-14-hypopg',
            'sudo pip3 install psycopg2 sql_metadata',
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
        "message", # cols
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
    drop_prefix_no_test = False
    drop_prefix_test = True
    # TODO: 有没有必要每一轮清理所有index？要换成dump当前index
    db_connector = PostgresDatabaseConnector("project1db")
    # db_connector.drop_indexes() # drop 所有index, 不管是不是hypo "select indexname from pg_indexes where schemaname='public'"
    # select * from hypopg_reset() drop hypo，新链接没必要做

    # Set the random seed to obtain deterministic statistics (and cost estimations)
    # because ANALYZE (and alike) use sampling for large tables
    db_connector.create_statistics()
    db_connector.commit()

    result = db_connector.exec_fetch("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'", one=False)
    # print(result)
    table_names_dict = {k[0] : {} for k in result}
    table_column_dict = table_names_dict.copy()
    table_index_dict = table_names_dict.copy()


    # cols: index_name
    # [('pg_class_relname_nsp_index', 'CREATE UNIQUE INDEX pg_class_relname_nsp_index ON pg_catalog.pg_class USING btree (relname, relnamespace)'), ('pg_class_tblspc_relfilenode_index', 'CREATE INDEX pg_class_tblspc_relfilenode_index ON pg_catalog.pg_class USING btree (reltablespace, relfilenode)'), ('pg_class_oid_index', 'CREATE UNIQUE INDEX pg_class_oid_index ON pg_catalog.pg_class USING btree (oid)')]

    for tab in table_column_dict.keys():
        cols = db_connector.exec_fetch("SELECT column_name FROM information_schema.columns where table_name = '{}'".format(tab), one=False)
        table_column_dict[tab] = {k[0] for k in cols}

        indexes = {}
        curr_index_def = db_connector.exec_fetch("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{}'".format(tab), one=False) # schemaname = 'public'
        p1 = re.compile(r'[(](.*?)[)]', re.S)  #最小匹配括号
        for row in curr_index_def:
            res = re.findall(p1, row[1])
            assert (len(res) == 1)
            cols = tuple(res[0].split(','))
            indexes[cols] = row[0]
        # print(indexes)
        table_index_dict[tab] = indexes

    print("curr table cols:", table_column_dict) # {'useracct': {'name', 'creation_date', 'u_id', 'email'}, 'item': {'i_id', 'creation_date', 'description', 'title'}, 'review': {'rank', 'u_id', 'i_id', 'rating', 'a_id', 'creation_date', 'comment'}, 'review_rating': {'last_mod_date', 'u_id', 'status', 'rating', 'creation_date', 'a_id', 'type', 'vertical_id'}, 'trust': {'creation_date', 'source_u_id', 'trust', 'target_u_id'}}
    print("curr table indexes:", table_index_dict) #{'jungle': {('uuid_field',): 'jungle_pkey', ('int_field9', ' float_field6'): 'index_jungle_intfield9_floatfield6'}, 'sources': {('id',): 'sources_pkey', ('name',): 'sources_name_key'}, 'types': {('id',): 'types_pkey', ('category', ' name'): 'types_category_name_key'}, 'sessions': {('id',): 'sessions_pkey'}, 'observations': {}}

    preprocessor = Preprocessor(csvlogs=[workload_csv], log_columns=_PG_LOG_COLUMNS, table_column_dict = table_column_dict)
    df0 = preprocessor.get_dataframe()
    print(df0.head(n=5).to_string())

    dfw = preprocessor.get_grouped_where_cnt()
    print(dfw.to_string())

    dfo = preprocessor.get_grouped_order_by_cnt()
    print(dfo.to_string())

    dfj = preprocessor.get_grouped_join_cnt()
    print(dfj.to_string())

    def get_appeared_candidate_with_cnt_dict(names_dict, df):
        # df给出的组合可能不来自同一table
        # 提取出现过的所有table，转化为同一table上的所有可能列组合
        # output format: {'useracct': {('u_id',): 61}, 'item': {('i_id',): 62}, 'review': {('i_id',): 99, ('i_id', 'u_id'): 58, ('u_id',): 30}, 'trust': {('source_u_id', 'target_u_id'): 88, ('source_u_id',): 37}}
        appeared_table_candidate_combination_dict = deepcopy(names_dict)
        for tup in df.index:
            tmp_tab_col_dict = {}
            cnt = df['count'][tup]
            for tab_col in tup:
                lst = tab_col.split('.')
                tab = lst[0]
                col = lst[1]
                if tab in tmp_tab_col_dict.keys():
                    tmp_tab_col_dict[tab].append(col) # sorted for groupby and join, not sorted for order by
                else:
                    tmp_tab_col_dict[tab] = [col]
            for table, cols in tmp_tab_col_dict.items():
                key = tuple(cols)
                appeared_table_candidate_combination_dict[table][key] = appeared_table_candidate_combination_dict[table][key] + cnt if key in appeared_table_candidate_combination_dict[table].keys() else cnt
        # 删除appeared_table_candidate_combination_dict 的dict大小为0的
        return {a: b for a, b in appeared_table_candidate_combination_dict.items() if len(b) >0}

    def permutate_candidate_dict_all_lengths_prefix_cnt(cand_dict):
        # input format: {'useracct': {('u_id',): 61}, 'item': {('i_id',): 62}, 'review': {('i_id',): 99, ('i_id', 'u_id'): 58, ('u_id',): 30}, 'trust': {('source_u_id', 'target_u_id'): 88, ('source_u_id',): 37}}
        # output will enumerate all subsets, since if the subset appear many times it will be generated. The count to be consistent.
        # {'useracct': {('u_id',): 61}, 'item': {('i_id',): 62}, 'review': {('i_id',): 157, ('u_id',): 88, ('i_id', 'u_id'): 58, ('u_id', 'i_id'): 58}, 'trust': {('source_u_id',): 125, ('target_u_id',): 88, ('source_u_id', 'target_u_id'): 88, ('target_u_id', 'source_u_id'): 88}}
        ret = cand_dict.copy()
        for tab, cands in cand_dict.items():
            permute_dict = {}
            for cand_w_underscore, cnt in cands.items():
                for num_cols in range(1, min(cand_max_len+1, len(cand_w_underscore) + 1)):
                    for permute_tuple in itertools.permutations(cand_w_underscore, num_cols):
                        # print(tab, permute_tuple)
                        permute_dict[permute_tuple] = permute_dict[permute_tuple] + cnt if permute_tuple in permute_dict.keys() else cnt
            ret[tab] = permute_dict
        return ret

    def merge_permutate_candidate_dict_all_lengths_prefix_cnt(sum_dict, cand_dict):
        ret = deepcopy(sum_dict)
        for tab, cands in cand_dict.items():
            if tab not in ret.keys():
                ret[tab] = {}
            for cand_w_underscore, cnt in cands.items():
                for num_cols in range(1, min(cand_max_len+1, len(cand_w_underscore) + 1)):
                    for permute_tuple in itertools.permutations(cand_w_underscore, num_cols):
                        # print(tab, permute_tuple)
                        ret[tab][permute_tuple] = ret[tab][permute_tuple] + cnt if permute_tuple in ret[tab].keys() else cnt
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
                for num_cols in range(start_num, min(cand_max_len+1, len(cand_w_underscore) + 1)):
                    prefix_tuple = cand_w_underscore[:num_cols]
                    ret[tab][prefix_tuple] = ret[tab][prefix_tuple] + cnt if prefix_tuple in ret[tab].keys() else cnt
        return ret

    where_candidate_no_permutation= get_appeared_candidate_with_cnt_dict(table_names_dict, dfw)
    # print("where_candidate_no_permutation: ", where_candidate_no_permutation) #{'useracct': {('u_id',): 61}, 'item': {('i_id',): 62}, 'review': {('i_id',): 99, ('i_id', 'u_id'): 58, ('u_id',): 30}, 'trust': {('source_u_id', 'target_u_id'): 88, ('source_u_id',): 37}}
    where_candidate_permutation = permutate_candidate_dict_all_lengths_prefix_cnt(where_candidate_no_permutation)
    print("where_candidate_permutation: ", where_candidate_permutation)

    join_candidate_no_permutation= get_appeared_candidate_with_cnt_dict(table_names_dict, dfj)
    # print("join_candidate_no_permutation: ", join_candidate_no_permutation)
    join_candidate_permutation = permutate_candidate_dict_all_lengths_prefix_cnt(join_candidate_no_permutation)
    # print("join_candidate_permutation: ", join_candidate_permutation)

    order_candidate_no_permutation= get_appeared_candidate_with_cnt_dict(table_names_dict, dfo)
    # print("order_candidate_no_permutation: ", order_candidate_no_permutation)

    merged_candidate = merge_permutate_candidate_dict_all_lengths_prefix_cnt(where_candidate_permutation, join_candidate_no_permutation)
    # print("merged_candidate: ", merged_candidate)

    # not ok for epinions
    # merged_candidate = merge_no_permutate_candidate_dict_prefix_cnt(merged_candidate, order_candidate_no_permutation)
    # print("merged_candidate_with_o: ", merged_candidate)

    # 处理merged_candidate，按cnt高低确定搜索顺序. 元素格式：(cnt, cols, table), 第一个相等的按第二个排序
    cand_sorted_list = []
    for tab, cands in merged_candidate.items():
        for cand, cnt in cands.items():
            cand_sorted_list.append((cnt, cand, tab))

    cand_sorted_list.sort(key=lambda x:(-x[0], len(x[1])))
    print("cand_sorted_list: ",cand_sorted_list)

    # 带每个template count的
    df1 = preprocessor.get_grouped_dataframe_interval() # sorted by cnts,
    # print(df1.to_string())

    sampled_workload = preprocessor.get_sampled_rows(0.05) # sample by this number!!!可以计算累加百分位数做，带上where claus
    # print(sampled_workload.to_string())

    # 爆搜策略开始
    # 每次开始，要更新cost eval那里记录的当前所有index为db上有的那些index。需要解决：上一轮建立好了，咋drop的问题---db会重置，需要记录drop操作
    # 以下集合包括table_index_dict，因为可能在延长前缀
    searched_candidate = set() # TODO: 每次dump记录搜索位置, 格式：(cols, tab), 没有cnt因为可能变化
    # for tab in table_index_dict.keys():
    #     for cols in table_index_dict[tab].keys():
    #         searched_candidate.add((cols, tab))
    related_curr_table_cols = set((cols, tab) for cols in table_index_dict[tab].keys() for tab in table_index_dict.keys() if tab in merged_candidate.keys())
    current_best_cols = deepcopy(related_curr_table_cols) # 不用dump
    searched_candidate.update(current_best_cols)
    print("searched_candidate: ", searched_candidate)

    # TODO: 把table_index_dict 同步给cost_eval对象，别对他们simulate了，虽然只会做一次
    # TODO: 有没有必要改多个db connector，看时间够不够吧
    # current_best_cost = self.cost_evaluation.calculate_cost(
    #     self.workload, index_combination, store_size=True
    # )
    current_best_cost = random.uniform(200, 202) # 这个不load
    current_best_improvement = { k : (0, ()) for k in table_names_dict.keys()} # cost, clustered_index cols，TODO：这个需要load dump
    to_drop_list = set() # [(col, tab)]
    to_build_list = set()
    to_cluster_list = set() # TODO: 这部分时间很可能算进grading总时长，注意

    # TODO: cluster 每个table排第一的那个单列/多列（cnt相同时），这件事没法hypo
    # TODO: 这里加入计时环节
    # TODO：利用上一轮results
    # TODO: Orderby列暂时彻底不管了。CLUSTER review using index_i_id_creation_date_backwards; 爆搜结束的轮做orderby cluster实验
    for _, cols, tab in cand_sorted_list:
        print("looking at cols ", cols, " table ", tab)
        if (cols, tab) in searched_candidate:
            print("already searched cols ", cols, " table ", tab)
            continue
        # 当且仅当前缀被选了，再搜我，否则不搜
        prefix = ()
        if len(cols) > 1:
            prefix = tuple(cols[:-1])
            if (prefix, tab) not in current_best_cols:
                print("prefix", prefix, "not picked. skip. (table: ", tab)
                continue


        # 前缀一定已经搜索过，但是不一定在best. 暂时不管自己的permutation在不在里面了？drop数据库真实index也非常费劲。因为不在乎存储
        # 需要drop尝试 统一先create新的后drop旧的，这一点costeval写的没问题
        # 最后当且仅当真的要应用更长的prefix，才加入所有新的create index后，dropindex。drop的名字可以查table_index_dict
        # 所以需要drop返回True表示drop了一个真index，需要加入dropped index list。
        current_cols = current_best_cols.copy()
        current_cols.add((cols, tab))
        # 暂时忽略orderby rating，那么不用删除这个index。 todo: 加drop的实验
        if drop_prefix_no_test and len(cols) > 1:
            current_cols.remove((prefix, tab))
            print("removed prefix:", prefix)
        # TODO: evaluation
        # cost = self.cost_evaluation.calculate_cost(
        #     self.workload, current_cols, store_size=True
        # )
        print("added cols, evaluating current:", current_cols)
        cost = random.uniform(50, 100) if len(cols) <= 1 else random.uniform(0, 50)
        # 最后搜完，把自己加入searched candidate，更新最优
        thresh = cost * min_cost_improvement # 1.003
        if thresh < current_best_cost:
            print("better!, current best:", current_cols)
            # 对于没有pkey的table（只有jungle有，好像用处不大，直接cluster倒是没问题），
            # 这里能否枚举cluster/ cluster多列·？尤其是epinions，observations table
            benefit = (current_best_cost - cost)/cost
            if benefit > current_best_improvement[tab][0]:
                print("has benefit, choosing cluster index: ", current_cols)
                current_best_improvement[tab] = (benefit, cols)
                for n_cols in range(1, len(cols)): # 可能prefix优化不大，没被cluster但是更小prefix被cluster了
                    pre = (tuple(cols[:n_cols]), tab)
                    if pre in to_cluster_list: # 节省一个操作
                        to_cluster_list.remove(pre)
                to_cluster_list.add((cols, tab))
            # 更新best
            current_best_cost = thresh
            current_best_cols = current_cols
            to_build_list.add((cols, tab))
            if len(cols) > 1:
                if drop_prefix_no_test:
                    print("removing prefix ", prefix)
                    to_drop_list.add((prefix, tab))
                elif drop_prefix_test: # 这里实验drp掉prefix带来的后果，小于1.003就drop
                    current_cols.remove((prefix, tab))
                    cost = random.uniform(current_best_cost-5, current_best_cost+5)
                    if cost < current_best_cost * max_dropping_cost_degrade:
                        print("test drop better! removing prefix ", prefix, ", current best:", current_cols)
                        # 更新best
                        current_best_cost = cost
                        current_best_cols = current_cols
                        to_drop_list.add((prefix, tab))
                    else:
                        print("test drop not better! prefix ", prefix, "kept")

        else:
            print("not better:", current_cols)
        searched_candidate.add((cols, tab))


    # 每个hypo要存自己的oid，虽然也可以select拿到？注意人家index是带下划线的.不用每次dump序列化，反正断链就没了




    def dict_to_actions_sql(to_build, to_drop, to_cluster):
        # format of to_build: candidate list (cols, tab)
        # format of to_drop: candidate list (cols, tab)
        actions_sql_list = [
            # "CREATE xxx"
        ]
        # build里出现的，一定没在当前数据库里，每个cand只会被检查一次。如果同时出现在了 build里，一定是本轮先加入的，那么从to build删除
        both = to_build.intersection(to_drop)
        to_build -= both
        to_drop -= both
        for cols, tab in to_build:
            names = ",".join(cols)
            statement = (
                f"create index if not exists {'index_' + tab +'_'+ '_'.join(cols)} "
                f"on {tab} ({names});"
            )
            actions_sql_list.append(statement)
        for cols, tab in to_drop:
            names = ",".join(cols)
            statement = (
                f"drop index {'index_' + tab +'_'+ '_'.join(cols)} ;"
            )
            actions_sql_list.append(statement)
        for cols, tab in to_cluster:
            names = ",".join(cols)
            statement = (
                f"cluster {tab} using {'index_' + tab +'_'+ '_'.join(cols)};"
            )
            actions_sql_list.append(statement)
        return actions_sql_list
    print("to build list: ", to_build_list)
    print("to drop list: ", to_drop_list)
    actions_sql_list = dict_to_actions_sql(to_build_list, to_drop_list, to_cluster_list)
    with open("actions.sql", 'w') as f:
        f.writelines('\n'.join(actions_sql_list))



    # print(df1.head(n=10).index[0]) # [0]就是第一个索引，就拿出一行了
    #
    # # TODO: build workload对象！， 用table参数创建column对象
    # # return：column候选集合+ 为了代码改动极小化，试着返回wkld对象



    # 下面是如何代入template
    # one_template = df1.head(n=1).index[0]
    # params_indexed_count = preprocessor.get_params(one_template) # [0]就是第一个索引
    # one_params = params_indexed_count.index[0] 要几个 这里可以切多少片
    # print(one_template)
    # print(one_params) 拿到一组参数。
    #
    # sql_raw = preprocessor.substitute_params(one_template, one_params)
    # print(sql_raw)
    # pser = Parser(sql_raw)
    # dc = pser.columns_dict
    #
    # wheres, orderbys, joins = [], [], []
    # if 'where' in dc.keys():
    #     wheres = dc['where']
    # if 'order_by' in dc.keys():
    #     orderbys= dc['order_by']
    # if 'joins' in dc.keys():
    #     joins= dc['join']
    # print (wheres, orderbys, joins)


    # df2 = preprocessor.get_grouped_dataframe_params()
    # print(df2.head(n=1).to_string()) #index=False
    # 丢弃所有pg_开头的列和table

    # print(df2["query_template"].head(n=2).to_string(index=False)) 不可
    # print(df2["query_params"].head(n=2).to_string(index=False)) 不可
    # print(df2.index.head(n=2).to_string(index=False)) 不可
    # print(df2["count"].head(n=2).to_string(index=False)) 可以但是没意义