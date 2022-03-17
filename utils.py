import itertools
from copy import deepcopy

cand_max_len = 3


class Index:
    def __init__(self, cols, table_name, type, index_name, is_real=False):
        self.cols = cols  # tuple of col names
        self.table_name = table_name
        self.type = type  # only b-tree and hash are supported
        self.is_real = is_real
        self.index_name = index_name  # the name for this index (real or to be built)
        self.key = (cols, table_name, type)  # used for addressing

    def __str__(self):
        return '+'.join([str(self.cols), self.table_name, self.type, self.index_name, str(self.is_real)])

    def __repr__(self):
        return self.__str__()

    def __eq__(self, i):
        return self.table_name == i.table_name and self.cols == i.cols and self.type == i.type


# candidate processing helpers
def get_appeared_candidate_with_cnt_dict(names_dict, df, banned_set):
    # output format: {'useracct': {('u_id',): 61}, 'item': {('i_id',): 62}, 'review': {('i_id',): 99, ('i_id', 'u_id'): 58, ('u_id',): 30}, 'trust': {('source_u_id', 'target_u_id'): 88, ('source_u_id',): 37}}
    appeared_table_candidate_combination_dict = deepcopy(names_dict)
    for tup in df.index:  # (col col col) appears together in a query
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
                tmp_tab_col_dict[tab].append(
                    col)  # this is sorted for group_by and join, not sorted for order by/group by
            else:
                tmp_tab_col_dict[tab] = [col]

        for table, cols in tmp_tab_col_dict.items():
            key = tuple(cols)
            appeared_table_candidate_combination_dict[table][key] = \
                appeared_table_candidate_combination_dict[table][key] + cnt if key in \
                                                                               appeared_table_candidate_combination_dict[
                                                                                   table].keys() else cnt
    # remove empty dicts
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
                    ret[tab][permute_tuple] = ret[tab][permute_tuple] + cnt if permute_tuple in ret[
                        tab].keys() else cnt
    return ret


def merge_no_permutate_candidate_dict_prefix_cnt(sum_dict, cand_dict):
    ret = deepcopy(sum_dict)
    for tab, cands in cand_dict.items():
        if tab not in ret.keys():
            ret[tab] = {}
        for cand_w_underscore, cnt in cands.items():
            # orderby's start_num: 1 + len of perfix of "where" cols
            start_num = 2
            for num_cols in range(start_num, min(cand_max_len + 1, len(cand_w_underscore) + 1)):
                prefix_tuple = cand_w_underscore[:num_cols]
                ret[tab][prefix_tuple] = ret[tab][prefix_tuple] + cnt if prefix_tuple in ret[tab].keys() else cnt
    return ret


def dict_to_actions_sql(to_build, to_drop, to_cluster, related_curr_table_triples2index_obj, db_connector):
    # format of to_build / to_drop: candidate sets (cols, tab)
    actions_sql_list = [
        # "CREATE xxx"
    ]
    restricted_indexes_names = db_connector.get_restricted_indexnames()
    # drop first, then cluster, then create, to modify less indexes
    for triple in to_drop:
        if triple in related_curr_table_triples2index_obj.keys() and related_curr_table_triples2index_obj[
            triple].index_name not in restricted_indexes_names:
            statement = (
                f"drop index {related_curr_table_triples2index_obj[triple].index_name};"
            )
            # avoid dropping errors on primary keys, which makes all actions fail. try dropping is fast
            # for building, we don't worry since we never use duplicate names
            # if db_connector.try_exec(statement):
            actions_sql_list.append(statement)

        else:
            print("We cannot drop a index not exists / is restricted: ", triple)

    both = to_build.intersection(to_cluster)
    to_cluster -= both
    to_build -= both

    for triple in to_cluster:
        cols, tab, type = triple
        if triple in related_curr_table_triples2index_obj.keys() and type == 'btree':
            statement = (
                f"cluster {tab} using {related_curr_table_triples2index_obj[triple].index_name};"
            )
            actions_sql_list.append(statement)
        else:
            print("error! cannot cluster on a index: ", triple)

    for ele in both:
        cols, tab, type = ele
        names = ",".join(cols)
        statement = (
            f"create index if not exists {type + '_' + tab + '_' + '_'.join(cols)} "
            f"on {tab} using {type} ({names});"
        )
        actions_sql_list.append(statement)

        statement = (
            f"cluster {tab} using {type + '_' + tab + '_' + '_'.join(cols)};"
        )
        actions_sql_list.append(statement)

    for cols, tab, type in to_build:
        names = ",".join(cols)
        statement = (
            f"create index if not exists {type + '_' + tab + '_' + '_'.join(cols)} "
            f"on {tab} using {type} ({names});"
        )
        actions_sql_list.append(statement)
    # no modify on the original lists
    to_cluster |= both
    to_build |= both

    return actions_sql_list
