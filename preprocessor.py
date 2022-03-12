# adapted from noisepage: https://github.com/cmu-db/noisepage-pilot/
# Used for extracting where / orderby columns and sampling workload grouped by templates

import glob
import re
import time

import pandas as pd
import pglast
from pandarallel import pandarallel
from sql_metadata import Parser
from tqdm.contrib.concurrent import process_map

# Enable parallel pandas operations.
# pandarallel is a little buggy. For example, progress_bar=True does not work,
# and if you are using PyCharm you will want to enable "Emulate terminal in
# output console" instead of using the PyCharm Python Console.
# The reason we're using this library anyway is that:
# - The parallelization is dead simple: change .blah() to .parallel_blah().
# - swifter has poor string perf; we're mainly performing string ops.
# - That said, Wan welcomes any switch that works.
pandarallel.initialize(verbose=1)


class Preprocessor:
    """
    Convert PostgreSQL query logs into pandas DataFrame objects.
    """

    def get_dataframe(self):
        """
        Get a raw dataframe of query log data.

        Returns
        -------
        df : pd.DataFrame
            Dataframe containing the query log data.
            Note that irrelevant query log entries are still included.
        """
        return self._df

    def get_grouped_dataframe_interval(self, interval=None):
        """
        Get the pre-grouped version of query log data.

        Parameters
        ----------
        interval : pd.TimeDelta or None
            time interval to group and count the query templates
            if None, pd is only aggregated by template

        Returns
        -------
        grouped_df : pd.DataFrame
            Dataframe containing the pre-grouped query log data.
            Grouped on query template and optionally log time.
        """
        gb = None
        if interval is None:
            gb = self._df.groupby(["query_template", "where_cols"]).size()  # must appear, or cannot have size
            # gb.drop("", axis=0, inplace=True)
            # gb.drop(gb.loc[gb.index==("", ())].index)
        # else:
        #     gb = self._df.groupby("query_template").resample(interval).size()
        #     # gb.drop("", axis=0, level=0, inplace=True)
        #     gb.drop(gb.loc[gb.index==""].index)
        grouped_df = pd.DataFrame(gb, columns=["count"])
        grouped_df.reset_index(inplace=True, level=['where_cols'])
        gb.drop(gb.loc[gb.index == ""].index)
        return grouped_df.sort_values(by=['count'], ascending=False)

    def get_sampled_rows(self, fraction=1.0):
        if fraction == 1.0:
            self._raw_wkld_df[["query_subst", "where_cols"]]
        gb_sample = self._raw_wkld_df.groupby("query_template").sample(frac=fraction)
        # print(gb_sample)
        return gb_sample[["query_subst", "where_cols"]]

    def get_grouped_dataframe_params(self):
        """
        Get the pre-grouped version of query log data.

        Returns
        -------
        grouped_df : pd.DataFrame
            Dataframe containing the pre-grouped query log data.
            Grouped on query template and query parameters.
        """
        return self._grouped_df_params

    def get_grouped_where_cnt(self):
        return self._grouped_where_cnt

    def get_grouped_order_by_cnt(self):
        return self._grouped_order_by_cnt

    def get_grouped_join_cnt(self):
        return self._grouped_join_cnt

    def get_grouped_group_by_cnt(self):
        return self._grouped_group_by_cnt

    def get_banned_set(self):
        return self._banned_cols

    def get_params(self, query):
        """
        Find the parameters associated with a particular query.

        Parameters
        ----------
        query : str
            The query template to look up parameters for.

        Returns
        -------
        params : pd.Series
            The counts of parameters associated with a particular query.
            Unfortunately, due to quirks of the PostgreSQL CSVLOG format,
            the types of parameters are unreliable and may be stringly typed.
        """
        params = self._grouped_df_params.query("query_template == @query")
        return params.droplevel(0).squeeze(axis=1)

    def sample_params(self, query, n, replace=True, weights=True):
        """
        Find a sampling of parameters associated with a particular query.

        Parameters
        ----------
        query : str
            The query template to look up parameters for.
        n : int
            The number of parameter vectors to sample.
        replace : bool
            True if the sampling should be done with replacement.
        weights : bool
            True if the sampling should use the counts as weights.
            False if the sampling should be equal probability weighting.

        Returns
        -------
        params : np.ndarray
            Sample of the parameters associated with a particular query.
        """
        params = self.get_params(query)
        weight_vec = params if weights else None
        sample = params.sample(n, replace=replace, weights=weight_vec)
        return sample.index.to_numpy()

    @staticmethod
    def substitute_params(query_template, params):
        assert type(query_template) == str
        query = query_template
        keys = [f"${i}" for i in range(len(params), 0, -1)]

        for k, v in zip(keys, reversed(params)):
            query = query.replace(k, v)
        return query

    @staticmethod
    def _read_csv(csvlog, log_columns):  # 主要工作：留下对应的列
        """
        Read a PostgreSQL CSVLOG file into a pandas DataFrame.

        Parameters
        ----------
        csvlog : str
            Path to a CSVLOG file generated by PostgreSQL.
        log_columns : List[str]
            List of columns in the csv log.

        Returns
        -------
        df : pd.DataFrame
            DataFrame containing the relevant columns for query forecasting.
        """
        # This function must have a separate non-local binding from _read_df
        # so that it can be pickled for multiprocessing purposes.
        return pd.read_csv(
            csvlog,
            names=log_columns,
            parse_dates=["log_time", "session_start_time"],
            usecols=[
                "log_time",
                "session_start_time",
                "command_tag",
                "message",
                "detail",
            ],
            header=None,
            index_col=False,
        )

    @staticmethod
    def _read_df(csvlogs, log_columns):
        """
        Read the provided PostgreSQL CSVLOG files into a single DataFrame.

        Parameters
        ----------
        csvlogs : List[str]
            List of paths to CSVLOG files generated by PostgreSQL.
        log_columns : List[str]
            List of columns in the csv log.

        Returns
        -------
        df : pd.DataFrame
            DataFrame containing the relevant columns for query forecasting.
        """
        return pd.concat(process_map(Preprocessor._read_csv, csvlogs, [log_columns for _ in csvlogs]))

    @staticmethod
    def _extract_query(message_series):  # 拿到裸sql string，比如select
        """
        Extract SQL queries from the CSVLOG's message column.

        Parameters
        ----------
        message_series : pd.Series
            A series corresponding to the message column of a CSVLOG file.

        Returns
        -------
        query : pd.Series
            A str-typed series containing the queries from the log.
        """
        simple = r"statement: ((?:DELETE|INSERT|SELECT|UPDATE).*)"
        extended = r"execute .+: ((?:DELETE|INSERT|SELECT|UPDATE).*)"
        regex = f"(?:{simple})|(?:{extended})"
        query = message_series.str.extract(regex, flags=re.IGNORECASE)
        # Combine the capture groups for simple and extended query protocol.
        query = query[0].fillna(query[1])
        # Prettify each SQL query for standardized formatting.
        # query = query.parallel_map(pglast.prettify, na_action='ignore')
        # Replace NA values (irrelevant log messages) with empty strings.
        query.fillna("", inplace=True)
        return query.astype(str)

    @staticmethod
    def _extract_params(detail_series):
        """
        Extract SQL parameters from the CSVLOG's detail column.
        If there are no such parameters, an empty {} is returned.

        Parameters
        ----------
        detail_series : pd.Series
            A series corresponding to the detail column of a CSVLOG file.

        Returns
        -------
        params : pd.Series
            A dict-typed series containing the parameters from the log.
        """

        def extract(detail):
            detail = str(detail)
            prefix = "parameters: "
            idx = detail.find(prefix)
            if idx == -1:
                return {}
            parameter_list = detail[idx + len(prefix):]
            params = {}
            for pstr in parameter_list.split(", "):
                pnum, pval = pstr.split(" = ")
                assert pnum.startswith("$")
                assert pnum[1:].isdigit()
                params[pnum] = pval
            return params

        return detail_series.parallel_apply(extract)

    @staticmethod
    def _substitute_params(df, query_col, params_col):
        """
        Substitute parameters into the query, wherever possible.

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe of query log data.
        query_col : str
            Name of the query column produced by _extract_query.
        params_col : str
            Name of the parameter column produced by _extract_params.
        Returns
        -------
        query_subst : pd.Series
            A str-typed series containing the query with parameters inlined.
        """

        def substitute(query, params):
            # Consider '$2' -> "abc'def'ghi".
            # This necessitates the use of a SQL-aware substitution,
            # even if this is much slower than naive string substitution.
            new_sql, last_end = [], 0
            for token in pglast.parser.scan(query):
                token_str = str(query[token.start: token.end + 1])
                if token.start > last_end:
                    new_sql.append(" ")
                if token.name == "PARAM":
                    assert token_str.startswith("$")
                    assert token_str[1:].isdigit()
                    new_sql.append(params[token_str])
                else:
                    new_sql.append(token_str)
                last_end = token.end + 1
            new_sql = "".join(new_sql)
            return new_sql

        def subst(row):
            return substitute(row[query_col], row[params_col])

        return df.parallel_apply(subst, axis=1)

    ## 去掉了static
    def _parse(self, query_series):
        """
        Parse the SQL query to extract (prepared queries, parameters).

        Parameters
        ----------
        query_series : pd.Series
            SQL queries with the parameters inlined.

        Returns
        -------
        queries_and_params : pd.Series
            A series containing tuples of (prepared SQL query, parameters).
        """

        def table_parse(col_lst, table_lst, mappings):  # parse which table it's from
            ret = []
            for col in col_lst:
                if '.' not in col:
                    for tab in table_lst:
                        if tab in mappings.keys() and col in mappings[tab]:
                            col = tab + '.' + col
                            break
                    ret.append(col)  # no match, also append
                else:
                    ret.append(col)
            return ret

        def table_parse_order_by_group_by(col_lst, table_lst, mappings, where_cols):  # parse which table it's from
            ret = []
            for col in col_lst:
                if '.' not in col:
                    for tab in table_lst:
                        if tab in mappings.keys() and col in mappings[tab]:
                            col = tab + '.' + col
                            break
                # 拿到确定了col的orderby 最高列
                for where_col in where_cols:
                    if col.split('.')[0] == where_col.split('.')[0]: # same table
                        ret.append(where_col)
                if len(ret) > 0:
                    ret.append(col)
                break
            return ret # not appeared in where

        def parse(sql, table_column_dict):
            sql = sql.replace("\r", "").replace("\n", "")
            new_sql, params, last_end = [], [], 0
            wheres, orderbys, joins, groupbys, banned_cols = [], [], [], [], []
            try:
                pser = Parser(sql)
                dc = pser.columns_dict
                if dc is not None:  # empty queries
                    tables = pser.tables
                    if 'where' in dc.keys():
                        wheres = table_parse(dc['where'], tables, table_column_dict)
                        if 'order_by' in dc.keys():
                            orderbys = table_parse_order_by_group_by(dc['order_by'], tables, table_column_dict, wheres)
                            # no sort
                        if 'group_by' in dc.keys():
                            groupbys = table_parse_order_by_group_by(dc['group_by'], tables, table_column_dict, wheres)
                        wheres.sort()
                    else:  # order/group by with no where clause
                        if 'order_by' in dc.keys():
                            # # 裸的order by
                            # # no sort
                            orderbys = table_parse(dc['order_by'], tables, table_column_dict)
                        if 'group_by' in dc.keys():
                            groupbys = table_parse(dc['group_by'], tables, table_column_dict)
                    if 'joins' in dc.keys():
                        joins = table_parse(dc['join'], tables, table_column_dict)
                        joins.sort()

                    pos = sql.find('SET ')
                    if pos >= 0:
                        banned = sql[pos+4: sql.find('=')].strip()
                        banned_cols = table_parse([banned], tables, table_column_dict)
                        banned_cols.sort()
            except Exception as e:  # parser error, must be the strange title in epinions, hack
                print(e, "try hacking:", sql) # "UPDATE item SET title = 'A2S;:J)X p/TL''@!Et1q=ey:)U}xi?77Ig%''Kt9l@~dYVHw .NLSF&8ryWe-m/v*&)X p;HDX#4)Qx=Yvr)1d''Ox=#1v8J*e.x''$_6\\pV_/ &a_[&dwFXbpvABD*Bs.' WHERE i_id=208;"
                st = sql.find('SET')
                end =sql.find('WHERE')
                sql2 = sql[:st] + sql[end:] # 'UPDATE item WHERE i_id=208;'
                try:
                    pser = Parser(sql2)
                    dc = pser.columns_dict
                    if dc is not None:  # empty queries
                        tables = pser.tables
                        if 'where' in dc.keys():
                            wheres = table_parse(dc['where'], tables, table_column_dict)
                            wheres.sort()
                except Exception as e2:
                    print("still cannot parse columns:", sql2)


            for token in pglast.parser.scan(sql):  # 用pglast
                token_str = str(sql[token.start: token.end + 1])
                if token.start > last_end:
                    new_sql.append(" ")  # 插入一个空格在list，最后join
                if token.name in ["ICONST", "FCONST", "SCONST"]:  # 所有字符串等常量
                    # Integer, float, or string constant.
                    new_sql.append("$" + str(len(params) + 1))
                    params.append(token_str)
                else:
                    new_sql.append(token_str)
                last_end = token.end + 1

            new_sql = "".join(new_sql)  # ？？还是转化成参数形式，只是带了数字
            return new_sql, tuple(params), tuple(wheres), tuple(orderbys), tuple(joins), tuple(
                groupbys), tuple(banned_cols)  # keep old

        return query_series.parallel_apply(parse, args=(self.table_column_dict,))  # 想办法打出来看下/看别人怎么用

    def _from_csvlogs(self, csvlogs, log_columns):  # 主函数在这里！！
        """
        Glue code for initializing the Preprocessor from CSVLOGs.

        Parameters
        ----------
        csvlogs : List[str]
            List of PostgreSQL CSVLOG files.
        log_columns : List[str]
            List of columns in the csv log.

        Returns
        -------
        df : pd.DataFrame
            A dataframe representing the query log.
        """
        time_end, time_start = None, time.perf_counter()

        def clock(label):
            nonlocal time_end, time_start
            time_end = time.perf_counter()
            print("\r{}: {:.2f} s".format(label, time_end - time_start))
            time_start = time_end

        df = self._read_df(csvlogs, log_columns)
        clock("Read dataframe")

        print("Extract queries: ", end="", flush=True)
        df["query_raw"] = self._extract_query(df["message"])
        df.drop(columns=["message"], inplace=True)
        # filter pg_catalog / pg_
        df = df[~(df["query_raw"] == "")]
        df = df[~df.query_raw.str.contains("pg_")]

        clock("Extract queries")

        print("Extract parameters: ", end="", flush=True)
        df["params"] = self._extract_params(df["detail"])  # simple为空操作
        df.drop(columns=["detail"], inplace=True)
        clock("Extract parameters")

        print("Substitute parameters into query: ", end="", flush=True)
        df["query_subst"] = self._substitute_params(df, "query_raw",
                                                    "params")  # simple为空操作，返回"query_subst"就是原来的string没变化
        df.drop(columns=["query_raw", "params"], inplace=True)
        clock("Substitute parameters into query")

        print("Parse query: ", end="", flush=True)
        parsed = self._parse(df["query_subst"])  # 调用pglast
        df[["query_template", "query_params", "where_cols", "order_by_cols", "join_cols",
            "group_by_cols", "banned_cols"]] = pd.DataFrame(parsed.tolist(), index=df.index)  # template是拿来找一摸一样形式的
        clock("Parse query")
        self._raw_wkld_df = df[
            ["query_subst", "query_template", "query_params", "where_cols", "order_by_cols", "join_cols",
             "group_by_cols", "banned_cols"]]
        return df[
            ["log_time", "query_template", "query_params", "where_cols", "order_by_cols", "join_cols", "group_by_cols", "banned_cols"]]

    def __init__(self, csvlogs=None, log_columns=None, parquet_path=None, table_column_dict=None):
        """
        Initialize the preprocessor with either CSVLOGs or a HDF dataframe.

        Parameters
        ----------
        csvlogs : List[str] | None
            List of PostgreSQL CSVLOG files.

        hdf_path : str | None
            Path to a .h5 file containing a Preprocessor's get_dataframe().
        """
        assert table_column_dict is not None
        self.table_column_dict = table_column_dict
        if csvlogs is not None:
            df = self._from_csvlogs(csvlogs, log_columns)
            df.set_index("log_time", inplace=True)
        else:
            assert parquet_path is not None
            df = pd.read_parquet(parquet_path)
            # convert params from array back to tuple so it is hashable
            df["query_params"] = df["query_params"].map(lambda x: tuple(x))

        # grouping queries by template-parameters count.
        gbp = df.groupby(["query_template", "query_params"]).size()  # 这应该就是一摸一样的q个数。直接拿template where后面对应的数字东西即可
        grouped_by_params = pd.DataFrame(gbp, columns=["count"])
        # grouped_by_params.drop('', axis=0, level=0, inplace=True)
        # TODO(WAN): I am not sure if I'm wrong or pandas is wrong.
        #  Above raises ValueError: Must pass non-zero number of levels/codes.
        #  So we'll do this instead...
        grouped_by_params = grouped_by_params[~grouped_by_params.index.isin([("", ())])]
        self._df = df  # 原始结果存在get dataframe离了
        self._grouped_df_params = grouped_by_params

        gbp_w = df.groupby(["where_cols"]).size()  # 这应该就是一摸一样的q个数。直接拿template where后面对应的数字东西即可
        gbp_w.drop((), axis=0, inplace=True)
        # gbp_w.drop(, axis=0, inplace=True)
        w_grouped_by_params = pd.DataFrame(gbp_w, columns=["count"])
        self._grouped_where_cnt = w_grouped_by_params.sort_values(by=['count'],
                                                                  ascending=False)  # [~w_grouped_by_params.index.isin([("", ())])]

        gbp_o = df.groupby(["order_by_cols"]).size()  # 这应该就是一摸一样的q个数。直接拿template where后面对应的数字东西即可
        gbp_o.drop((), axis=0, inplace=True)
        o_grouped_by_params = pd.DataFrame(gbp_o, columns=["count"])
        self._grouped_order_by_cnt = o_grouped_by_params.sort_values(by=['count'], ascending=False)

        gbp_j = df.groupby(["join_cols"]).size()  # 这应该就是一摸一样的q个数。直接拿template where后面对应的数字东西即可
        gbp_j.drop((), axis=0, inplace=True)
        j_grouped_by_params = pd.DataFrame(gbp_j, columns=["count"])
        self._grouped_join_cnt = j_grouped_by_params.sort_values(by=['count'], ascending=False)

        gbp_g = df.groupby(["group_by_cols"]).size()
        gbp_g.drop((), axis=0, inplace=True)
        g_grouped_by_params = pd.DataFrame(gbp_g, columns=["count"])
        self._grouped_group_by_cnt = g_grouped_by_params.sort_values(by=['count'], ascending=False)

        gbp_b = df.groupby(["banned_cols"]).size()
        gbp_b.drop((), axis=0, inplace=True)
        self._banned_cols = set(a[0] for a in gbp_b.index)
        print("self._banned_cols", self._banned_cols)

        # for cols in
