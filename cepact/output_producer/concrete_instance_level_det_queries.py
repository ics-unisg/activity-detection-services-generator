""" Concrete instance level detection query classes (strategy pattern). """
from math import ceil

from .abstract_instance_level_det_query import InstanceLevelDetQuery

DATETIME_FORMAT = 'yyyy-MM-dd HH:mm:ss.SS'


class AllHighLevelPatternQuery(InstanceLevelDetQuery):
    """ Concrete instance level detection query for all high level patterns. """

    def generate(self, num_changes: int, act_name: str) -> str:
        """ Generate the query. """
        ahli_query = (
            f'@info(name="Detect-AllHighLevelPattern-InstanceLevelActivity")\n'
            f'from every e1 = DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-1"] '
            f'-> not HelperStream[event == "HighToLow"] and '
            f'e2 = DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-{num_changes}"]\n'
            f'select "{act_name}" as activity, '
            f'"AllHighLevelPattern" as detection_type, '
            f"e1.ts_second as ts_start, "
            f"time:timestampInMilliseconds(e1.ts_second, '{DATETIME_FORMAT}') as ts_start_unix, "
            f'e2.ts_second as ts_end, '
            f"time:timestampInMilliseconds(e2.ts_second, '{DATETIME_FORMAT}') as ts_end_unix"
            f'\n'
            f'insert into DetectedInstanceLevelActivities;'
        )
        return ahli_query


class First50HighLevelPatternQuery(InstanceLevelDetQuery):
    """ Concrete instance level detection query for first 50% high level patterns. """

    def generate(self, num_changes: int, act_name: str) -> str:
        """ Generate the query. """
        if num_changes > 2:
            ffpi_query = (
                f'@info(name="Detect-First50HighLevelPattern-InstanceLevelActivity")\n'
                f'from every e1 = DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-1"] '
                f'-> not HelperStream[event == "HighToLow"] '
                f'and e2 = DetectedHighLevelActivityEvents'
                f'[event == "HighLevel-Pattern-{ceil(num_changes / 2)}"]\n'
                f'select "{act_name}" as activity, '
                f'"First50HighLevelPattern" as detection_type, '
                f'e1.ts_second as ts_start, '
                f"time:timestampInMilliseconds"
                f"(e1.ts_second, '{DATETIME_FORMAT}') as ts_start_unix, "
                f'e2.ts_second as ts_end, '
                f"time:timestampInMilliseconds(e2.ts_second, '{DATETIME_FORMAT}') as ts_end_unix"
                f'\n'
                f'insert into DetectedInstanceLevelActivities;'
            )
        elif num_changes in [1, 2]:
            ffpi_query = (
                f'@info(name="Detect-First50HighLevelPattern-InstanceLevelActivity")\n'
                f'from every e1 = DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-1"]\n'
                f'select "{act_name}" as activity, '
                f'"First50HighLevelPattern" as detection_type, '
                f'e1.ts_second as ts_start, '
                f"time:timestampInMilliseconds"
                f"(e1.ts_second, '{DATETIME_FORMAT}') as ts_start_unix, "
                f'e1.ts_second as ts_end, '
                f"time:timestampInMilliseconds(e1.ts_second, '{DATETIME_FORMAT}') as ts_end_unix"
                f'\n'
                f'insert into DetectedInstanceLevelActivities;'
            )
        else:
            raise ValueError(f"Number of distinct change timestamps must "
                             f"be greater than 0; act_name: {act_name}")
        return ffpi_query


class FirstLastLowLevelPatternQuery(InstanceLevelDetQuery):
    """ Concrete instance level detection query for first last low level patterns. """

    def generate(self, num_changes: int, act_name: str) -> str:
        """ Generate the query. """
        fllli_query = (
            f'@info(name="Detect-FirstLastLowLevelPattern-InstanceLevelActivity")\n'
            f'from every e1 = DetectedLowLevelActivityEvents[event == "LowLevel-Pattern-1"] '
            f'-> not HelperStream[event == "HighToLow"] '
            f'and e2 = DetectedLowLevelActivityEvents[event == "LowLevel-Pattern-{num_changes}"]\n'
            f'select "{act_name}" as activity, '
            f'"FirstLastLowLevelPattern" as detection_type, e1.ts_second as ts_start, '
            f"time:timestampInMilliseconds(e1.ts_second, '{DATETIME_FORMAT}') as ts_start_unix, "
            f'e2.ts_second as ts_end, '
            f"time:timestampInMilliseconds(e2.ts_second, '{DATETIME_FORMAT}') as ts_end_unix"
            f'\n'
            f'insert into DetectedInstanceLevelActivities;'
        )
        return fllli_query


def create_any_quartile_low_level_instance_query(num_changes: int,
                                                 activity_name: str,
                                                 percentage_req: int) -> str:
    """ Create the query for any quartile low level instance detection. """
    if percentage_req not in [25, 50, 75]:
        raise ValueError("Error during app creation. Percentage must be 25, 50 or 75.")
    upper_bound = ceil(num_changes * (percentage_req / 100))
    afpi_query = (
        f'@info(name="Detect-Any{percentage_req}LowLevelPattern'
        f'-InstanceLevelActivity-Startup")\n'
        f'from e1 = DetectedLowLevelActivityEvents,\n') if (upper_bound > 1) else (
        f'@info(name="Detect-Any{percentage_req}LowLevelPattern'
        f'-InstanceLevelActivity-Startup")\n'
        f'from e1 = DetectedLowLevelActivityEvents\n')
    for i in range(2, (upper_bound + 1)):
        if i < upper_bound:
            afpi_query += (f'\te{i} = DetectedLowLevelActivityEvents'
                           f'[e{i - 1}.ll_pattern_num < ll_pattern_num and '
                           f'time:timestampInMilliseconds(e{i - 1}.ts_second, \'yyyy-MM-dd '
                           f'HH:mm:ss.SS\') <= time:timestampInMilliseconds(ts_second, '
                           f'\'yyyy-MM-dd HH:mm:ss.SS\')],\n')
        else:
            afpi_query += (f'\te{i} = DetectedLowLevelActivityEvents'
                           f'[e{i - 1}.ll_pattern_num < ll_pattern_num and '
                           f'time:timestampInMilliseconds(e{i - 1}.ts_second, \'yyyy-MM-dd '
                           f'HH:mm:ss.SS\') <= time:timestampInMilliseconds(ts_second, '
                           f'\'yyyy-MM-dd HH:mm:ss.SS\')]\n')
    afpi_query += (
        f'select "{activity_name}" as activity, '
        f'"Any{percentage_req}LowLevelPattern" as detection_type, '
        f'e1.ts_second as ts_start, '
        f"time:timestampInMilliseconds(e1.ts_second, '{DATETIME_FORMAT}') as ts_start_unix, "
        f'e{upper_bound}.ts_second as ts_end, '
        f"time:timestampInMilliseconds(e{upper_bound}.ts_second, "
        f"'{DATETIME_FORMAT}') as ts_end_unix"
        f'\n')
    afpi_query += 'insert into DetectedInstanceLevelActivities;\n\n'

    afpi_query += (
        f'@info(name="Detect-Any{percentage_req}LowLevelPattern-InstanceLevelActivity")\n'
        f'from every e1 = DetectedLowLevelActivityEvents,\n')
    for i in range(2, (upper_bound + 2)):
        if i == 2:
            afpi_query += ('\te2 = DetectedLowLevelActivityEvents'
                           '[e1.ll_pattern_num >= ll_pattern_num and '
                           'time:timestampInMilliseconds(e1.ts_second, \'yyyy-MM-dd '
                           'HH:mm:ss.SS\') <= time:timestampInMilliseconds(ts_second, \'yyyy-MM-dd '
                           'HH:mm:ss.SS\')],\n') if (
                    upper_bound > 1) else ('\te2 = DetectedLowLevelActivityEvents'
                                           '[e1.ll_pattern_num >= ll_pattern_num and '
                                           'time:timestampInMilliseconds(e1.ts_second, '
                                           '\'yyyy-MM-dd HH:mm:ss.SS\') <= '
                                           'time:timestampInMilliseconds(ts_second, \'yyyy-MM-dd '
                                           'HH:mm:ss.SS\')]\n')
        elif i < upper_bound + 1:
            afpi_query += (f'\te{i} = DetectedLowLevelActivityEvents'
                           f'[e{i - 1}.ll_pattern_num < ll_pattern_num and '
                           f'time:timestampInMilliseconds(e{i - 1}.ts_second, \'yyyy-MM-dd '
                           f'HH:mm:ss.SS\') <= time:timestampInMilliseconds(ts_second, '
                           f'\'yyyy-MM-dd HH:mm:ss.SS\')],\n')
        else:
            afpi_query += (f'\te{i} = DetectedLowLevelActivityEvents'
                           f'[e{i - 1}.ll_pattern_num < ll_pattern_num and '
                           f'time:timestampInMilliseconds(e{i - 1}.ts_second, \'yyyy-MM-dd '
                           f'HH:mm:ss.SS\') <= time:timestampInMilliseconds(ts_second, '
                           f'\'yyyy-MM-dd HH:mm:ss.SS\')]\n')
    afpi_query += (
        f'select "{activity_name}" as activity, '
        f'"Any{percentage_req}LowLevelPattern" as detection_type, '
        f'e2.ts_second as ts_start, '
        f"time:timestampInMilliseconds(e2.ts_second, '{DATETIME_FORMAT}') as ts_start_unix, "
        f'e{upper_bound + 1}.ts_second as ts_end, '
        f"time:timestampInMilliseconds(e{upper_bound + 1}.ts_second, "
        f"'{DATETIME_FORMAT}') as ts_end_unix"
        f'\n')
    afpi_query += 'insert into DetectedInstanceLevelActivities;'
    return afpi_query


class Any25LowLevelPatternQuery(InstanceLevelDetQuery):
    """ Concrete instance level detection query for any 25% low level patterns. """

    def generate(self, num_changes: int, act_name: str) -> str:
        """ Generate the query. """
        return create_any_quartile_low_level_instance_query(num_changes, act_name, 25)


class Any50LowLevelPatternQuery(InstanceLevelDetQuery):
    """ Concrete instance level detection query for any 50% low level patterns. """

    def generate(self, num_changes: int, act_name: str) -> str:
        """ Generate the query. """
        return create_any_quartile_low_level_instance_query(num_changes, act_name, 50)


class Any75LowLevelPatternQuery(InstanceLevelDetQuery):
    """ Concrete instance level detection query for any 75% low level patterns. """

    def generate(self, num_changes: int, act_name: str) -> str:
        """ Generate the query. """
        return create_any_quartile_low_level_instance_query(num_changes, act_name, 75)
