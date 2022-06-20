import yaml
from tunetools import db_utils
import numpy as np
import pandas as pd
from scipy import stats
import json



def _singleton_dict_to_tuple(singleton_dict):
    return list(singleton_dict.items())[0]


def _check_param(param, total_param, mark_param: set):
    if param is None:
        return []
    new_param = []
    for x in param:
        if x.startswith(":"):
            x = x[1:]
            mark_param.add(x)
        if x not in total_param:
            raise ValueError("Unknown param: " + x + str(total_param))
        new_param.append(x)
    return new_param


def _parse(conn, yaml_path, formatter, csv: bool):
    yml_dict = yaml.load(open(yaml_path), Loader=yaml.FullLoader)

    total_params = [x[6:] for x in db_utils.get_columns(conn, "RESULT") if x.startswith("param_")]
    target_result = {}
    has_direction = False
    for x in yml_dict.get("target", []):
        if type(x) == dict:
            cur_tuple = _singleton_dict_to_tuple(x)
            target_result[cur_tuple[1]] = cur_tuple[0]
            has_direction = True
        else:
            target_result[x] = ""

    mark_params = set()
    group_by_params = _check_param(yml_dict.get("group_by", []), total_params, mark_params)
    find_best_params = _check_param(yml_dict.get("find_best", []), total_params, mark_params)
    ignore_params = _check_param(yml_dict.get("ignore", []), total_params, set())
    if not has_direction and len(find_best_params) != 0:
        raise ValueError("Unknown direction for find best params: " + str(find_best_params))
    if len(find_best_params) == 0:
        find_best_params = [group_by_params[0]]
    current_params = group_by_params + find_best_params
    left_params = [x for x in total_params if x not in current_params]

    where_list = yml_dict.get("where", [])
    where_clauses = ["STATUS = 'TERMINATED'"]
    where_clause_params = []
    for where_condition in where_list:
        if type(where_condition) == dict:
            item = _singleton_dict_to_tuple(where_condition)
            where_clauses.append(str(item[0]) + "=?")
            where_clause_params.append(item[1])
        elif type(where_condition) == str:
            where_clauses.append(where_condition)
    where_clauses_statement = " AND ".join(list(map(lambda x: "(%s)" % x, where_clauses)))

    statement = "SELECT * FROM RESULT"
    if len(where_clauses) != 0:
        statement += " WHERE " + where_clauses_statement
    cursor = db_utils.execute_sql(conn, statement, where_clause_params)
    columns = [description[0] for description in cursor.description]
    result = list(cursor)

    data = pd.DataFrame(result, columns=columns)  # (group_by, find_best, num_sample) -> result

    def apply_find_best(df: pd.DataFrame):
        # input: (**group_by**, find_best, num_sample) -> result
        # output: (**group_by**) -> best ArrayWrapper
        agg = df.groupby(by=["param_" + x for x in find_best_params], as_index=False).apply(
            apply_aggregate_sample)
        # agg: (**group_by**, **find_best**) -> ArrayWrapper
        best_row = None
        for _, row in agg.iterrows():
            if best_row is None:
                best_row = row
            else:
                for ret_name, direction in target_result.items():
                    if direction != 'max' and direction != 'min':
                        continue
                    larger = row['ret_' + ret_name].mean() > best_row['ret_' + ret_name].mean()
                    lower = row['ret_' + ret_name].mean() < best_row['ret_' + ret_name].mean()
                    is_better = (direction == 'max' and larger) or (direction == 'min' and lower)
                    is_worse = (direction == 'max' and lower) or (direction == 'min' and larger)
                    if is_better:
                        best_row = row
                        break
                    if is_worse:
                        break
        return best_row

    def apply_aggregate_sample(df: pd.DataFrame):
        # input: (**group_by**, **find_best**, num_sample) -> result
        # output: (**group_by**, **find_best**) -> ArrayWrapper
        current_group = dict(
            ('param_' + g, df['param_' + g].iloc[0]) for g in (group_by_params + find_best_params))

        for p in left_params:
            if p in ignore_params:
                continue
            flatten_set = set(df['param_' + p])
            if len(flatten_set) != 1:
                raise ValueError("Identifiability check failed: there exist distinct values " + str(
                    flatten_set) +
                                 " on parameter '" + p + "' in group: " + str(current_group) +
                                 ", which may make the aggregated target inaccurate. " +
                                 "Please check it. You can add '" + p +
                                 "' into 'find_best', 'group_by' or 'ignore' configurations, or filter "
                                 "this case in 'where' configurations.")

        current_group.update(
            dict(('ret_' + g, ArrayWrapper(list(df['ret_' + g]), formatter)) for g in target_result))
        x = pd.Series(current_group)
        return x

    data = data.groupby(by=["param_" + x for x in group_by_params], as_index=False).apply(
        apply_find_best)
    data.index = range(len(data))

    # t-test
    t_test_param = yml_dict.get("t_test", None)
    if t_test_param is not None:
        t_test(data, dict([_singleton_dict_to_tuple(x) for x in t_test_param]), target_result)

    print_group(data, mark_params, csv)

    draw_params = yml_dict.get("draw", None)
    if draw_params is not None:
        draw_params = dict(_singleton_dict_to_tuple(x) for x in draw_params)
        draw(data, draw_params)


def print_group(data: pd.DataFrame, mark_params, csv: bool):
    group = data.copy()
    mark_params = ["param_" + x for x in mark_params]
    group.drop(labels=mark_params, axis=1, inplace=True)
    param_columns = list(map(lambda x: x.replace("param_", ""),
                             filter(lambda x: x.startswith("param_"), group.columns)))
    group.columns = map(lambda x: x.replace("param_", "").replace("ret_", ""), group.columns)
    group = group.groupby(by=param_columns).agg('first')
    if csv:
        print(group.to_csv())
    else:
        print(group.to_string())


def t_test(data: pd.DataFrame, t_test_param, target_result):
    baseline_cond = [_singleton_dict_to_tuple(x) for x in t_test_param['baseline']]
    baseline = []
    for _, row in data.iterrows():
        hit = True
        for k, v in baseline_cond:
            if row['param_' + k] != v:
                hit = False
                break
        if hit:
            baseline.append(row.copy())
    if len(baseline) != 1:
        raise ValueError(str(len(baseline)) + " baseline(s) found!")
    baseline = baseline[0]
    for _, row in data.iterrows():
        for target in target_result.keys():
            name = 'ret_' + target
            if row[name].is_numeric():
                row[name].t_test(baseline[name], t_test_param['equal_var'])


def draw(data: pd.DataFrame, draw_params):
    def get_axis_index(name):
        if name not in draw_params:
            raise ValueError("No " + name + " found in draw params!")
        return draw_params[name]

    x_name = "param_" + get_axis_index("x")
    y_name = "ret_" + get_axis_index("y")
    legend_template = draw_params.get("legend", "")
    legend_to_xy = {}  # legend -> [x], [y]
    for _, record in data.iterrows():
        # legend = str(record[legend_names])
        legend = legend_template
        for name in record.index:
            legend = legend.replace("${%s}" % name, str(record[name]))
        if legend not in legend_to_xy:
            legend_to_xy[legend] = {'x': [], 'y': [], 'y_sup': [], 'y_inf': [], 'marker': '', 'line_style': '', 'color': ''}
        legend_to_xy[legend]['x'].append(record[x_name])
        legend_to_xy[legend]['y'].append(record[y_name].mean())
        legend_to_xy[legend]['y_sup'].append(record[y_name].mean() + record[y_name].std())
        legend_to_xy[legend]['y_inf'].append(record[y_name].mean() - record[y_name].std())

    content_json = {
        'pre_command': [
            'plt.xlabel("%s")' % draw_params['x'],
            'plt.ylabel("%s")' % draw_params['y']
        ],
        'post_command': [
            'plt.legend().get_frame().set_facecolor("none")'
        ],
        'plot': legend_to_xy,
    }
    draw_with_json(content_json, True)


def draw_with_json(content, from_statistics=False):
    from matplotlib import pyplot as plt

    if 'pre_command' in content:
        for command in content['pre_command']:
            eval(command)

    legend_to_xy = content['plot']
    patterns = ["\\", ".", "o", "/", "+", "-", "*", "x", "O", "|"]
    marker = ['.', '+', '*', 'v', 'D', 'o', 'v', '1', '2', '3', '4']
    line_style = ['-', ':', '-.', '--', '-', ':', '-.', '--', '-', ':', '-.', '--']
    has_legend = False
    for i, label in enumerate(legend_to_xy):
        if label == "":
            label_text = None
        else:
            label_text = label
            has_legend = True
        cur_marker = legend_to_xy[label].get('marker', '')
        cur_line_style = legend_to_xy[label].get('line_style', '')
        cur_color = legend_to_xy[label].get('color', '')
        plt.plot(legend_to_xy[label]['x'], legend_to_xy[label]['y'], label=label_text,
                 marker=cur_marker if cur_marker != '' else marker[i],
                 linestyle=cur_line_style if cur_line_style != '' else line_style[i],
                 color=cur_color if cur_color != '' else None)
        if 'y_sup' in legend_to_xy[label] and 'y_inf' in legend_to_xy[label]:
            plt.fill_between(legend_to_xy[label]['x'],
                             legend_to_xy[label]['y_sup'],
                             legend_to_xy[label]['y_inf'],
                             alpha=0.2)

    if 'post_command' in content:
        for command in content['post_command']:
            eval(command)

    if from_statistics:
        print("draw params: ")
        viewLim = plt.gca().viewLim
        content['post_command'].append("plt.axis([%lf, %lf, %lf, %lf])" % (viewLim.xmin, viewLim.xmax, viewLim.ymin, viewLim.ymax))
        print(json.dumps(content))
        print("You can save the above draw params into a json file, then use the command 'tunetools draw <path>' to reproduce the picture.")


    plt.show()


class ArrayWrapper:
    def __init__(self, content, formatter: str):
        try:
            self._array = list(map(float, content))
            self._formatter = formatter
            self._is_numeric = True
        except Exception:
            self._array = content
            self._is_numeric = False
        self._mean = None
        self._std = None
        self._p_value = None

    def __repr__(self):
        if self._is_numeric:
            content = self._formatter.format(count=self.count(), mean=self.mean(), std=self.std())
            if self._p_value is not None:
                content += " (%.4lf)" % (self._p_value)
            return content
        if len(set(self._array)) == 1:
            return str(self._array[0])
        return str(self._array)

    def mean(self):
        assert self._is_numeric
        if self._mean is None:
            self._mean = sum(self._array) / len(self._array)
        return self._mean

    def is_numeric(self):
        return self._is_numeric

    def std(self):
        assert self._is_numeric
        if self._std is None:
            self._std = np.std(self._array)
        return self._std

    def t_test(self, another, equal_var):
        assert self._is_numeric
        if self._p_value is None:
            self._p_value = stats.ttest_ind(self._array, another._array, equal_var=equal_var)[1]
        return self._p_value

    def count(self):
        return len(self._array)

    def __str__(self):
        return self.__repr__()


