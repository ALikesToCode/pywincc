from collections import namedtuple
from helper import local_time_to_utc, datetime_to_str_without_ms,\
    str_to_datetime

Alarm = namedtuple('Alarm',
                   'id state datetime classname priority location text')


class AlarmRecord():
    """Class to hold alarm records returned by a WinCC mssql query"""

    state_dict = {1: 'COME', 2: 'GO  ', 3: 'ACK ', 16: 'GACK'}

    def __init__(self, alarms=None):
        if alarms:
            self.alarms = alarms
        else:
            self.alarms = []

    def push(self, alarm):
        """Push a new Alarm to alarms list.
        Alarm must be of type alarm.Alarm.
        """
        if isinstance(alarm, Alarm):
            self.alarms.append(alarm)
        else:
            raise TypeError("AlarmRecord: Expected type 'Alarm'. Got type "
                            "{type}.".format(type=type(alarm)))

    def __unicode__(self):
        output = ""
        for alarm in self.alarms:
            state = self.alarm_state_as_text(alarm)
            output += u"{a.id} {state:4} ".format(a=alarm, state=state)
            output += u"{a.datetime} {a.priority:9} ".format(a=alarm)
            output += u"{a.location:14} {a.text}\n".format(a=alarm)
        return output

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __iter__(self):
        return iter(self.alarms)

    def alarm_state_as_text(self, alarm):
        """Return the alarm state as text e.g. "COME" instead of 1."""
        if alarm.state in self.state_dict:
            state = self.state_dict[alarm.state]
        else:
            state = alarm.state
        return state

    def count_all(self):
        """Return number of alarms in record."""
        return len(self.alarms)

    def count_come(self):
        """Return number of alarms with state 'COME' in record"""
        return len([1 for a in self.alarms if a.state == 1])

    def count_by_state_and_priority(self, state, priority):
        """Counts all alarms in record that fit given state and priority
        state = [1, 2, 3]
        priority = [u'WARNING', u'ERROR_DAY', u'ERROR_NOW', u'STOP_ALL']
        """
        return len([1 for a in self.alarms
                    if (a.priority == priority and a.state == state)])

    def count_come_warning(self):
        """Return number of alarms of state 'COME' and priority 'Warning'."""
        return self.count_by_state_and_priority(1, u'Warning')

    def count_come_alarm(self):
        """Return number of alarms of state 'COME' and priority 'Alarm'."""
        return self.count_by_state_and_priority(1, u'Alarm')

    def count_come_failure(self):
        """Return number of alarms of state 'COME' and priority 'Failure'."""
        return self.count_by_state_and_priority(1, u'Failure')

    def to_html(self):
        """Return a string that holds a HTML representation of alarms in record."""
        html = u"<table>\n"
        html += u"<tr>\n<th>ID</th><th>Datetime</th><th>State</th><th>Priority"
        html += u"</th><th>Location</th><th>Text</th>\n</tr>\n"
        for alarm in self.alarms:
            if alarm.state in self.state_dict:
                state = self.state_dict[alarm.state]
            else:
                state = alarm.state
            html += u"<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td><td>{5}</td></tr>\n".format(alarm.id, alarm.datetime, state, alarm.priority, alarm.location, alarm.text)
        html += u"</table>\n"
        return html

    def count_grouped_to_html(self):
        """Return a string that holds a HTML representation of grouped alarm priorities in record."""
        html = u"<table>\n"
        html += u"<tr><th>Priority</th><th>Count</th></tr>"
        html += u"<tr><td>Warning</td><td>{count}</td></tr>".format(count=self.count_come_warning())
        html += u"<tr><td>Alarm</td><td>{count}</td></tr>".format(count=self.count_come_alarm())
        html += u"<tr><td>Failure</td><td>{count}</td></tr>".format(count=self.count_come_failure())
        html += u"<tr><td>SUM</td><td>{count}</td></tr>".format(count=self.count_come())
        html += u"</table>\n"
        return html

    def get_count_grouped(self):
        """Return a dict of alarm priorities and counts."""
        return {'warning': self.count_come_warning(),
                'alarm': self.count_come_alarm(),
                'failure': self.count_come_failure(),
                'sum': self.count_come()}

    def filter_by_priority(self, priority):
        """Return a filtered list of alarms."""
        return [alarm for alarm in self.alarms if alarm.priority == priority]

    def filter_by_priorities(self, priorities):
        """Return a filters list of alarms. Expects a list of priorities."""
        return [alarm for prio in priorities for alarm in self.alarms
                if alarm.priority == prio]

    def filter_by_state(self, state):
        """Return a filtered list of alarms."""
        return [alarm for alarm in self.alarms
                if self.alarm_state_as_text(alarm) == state]

    def filter_by_states(self, states):
        """Return a filtered list of alarms. Expects a list of states"""
        return [alarm for state in states for alarm in self.alarms
                if self.alarm_state_as_text(alarm) == state]


def alarm_state_as_text(alarm_state):
    """Translate Integer alarm state to text.
    Return integer cast to string of state unknown.
    COME, GO, ACK, GACK.
    """
    state_dict = {1: 'COME', 2: 'GO  ', 3: 'ACK ', 16: 'GACK'}
    if alarm_state in state_dict:
        return state_dict[alarm_state]
    else:
        return str(alarm_state)


def alarm_query_builder(begin_time, end_time, msg_text='', utc=False, state='', priority='', priority2=''):
    """Build wincc alarm query string

    >>> alarm_query_builder("2015-08-24 10:07:48", "2015-08-24 10:08:12", '', False, '')
    u"ALARMVIEW:SELECT * FROM ALGVIEWDEU WHERE MsgNr < 12508141 AND DateTime > '2015-08-24 08:07:48' AND DateTime < '2015-08-24 08:08:12'"

    >>> alarm_query_builder("2015-08-24 10:07:48", "2015-08-24 10:08:12", '', True, '')
    u"ALARMVIEW:SELECT * FROM ALGVIEWDEU WHERE MsgNr < 12508141 AND DateTime > '2015-08-24 10:07:48' AND DateTime < '2015-08-24 10:08:12'"

    >>> alarm_query_builder("2015-08-24 10:07:48", '', '', False, '')
    u"ALARMVIEW:SELECT * FROM ALGVIEWDEU WHERE MsgNr < 12508141 AND DateTime > '2015-08-24 08:07:48'"

    >>> alarm_query_builder("2015-08-24 10:07:48", '', 'Trogkettenf', False, '')
    u"ALARMVIEW:SELECT * FROM ALGVIEWDEU WHERE MsgNr < 12508141 AND DateTime > '2015-08-24 08:07:48' AND Text1 LIKE '%Trogkettenf%'"

    >>> alarm_query_builder("2015-08-24 10:07:48", '', '', False, '>2')
    u"ALARMVIEW:SELECT * FROM ALGVIEWDEU WHERE MsgNr < 12508141 AND DateTime > '2015-08-24 08:07:48' AND State >2"
    """

    dt_begin_time = str_to_datetime(begin_time)
    if not utc:
        dt_begin_time = local_time_to_utc(dt_begin_time)
    # MsgNr < 12508141 to filter out operator messages
    datetime_str = datetime_to_str_without_ms(dt_begin_time)
    query = u"ALARMVIEW:SELECT * FROM ALGVIEWDEU WHERE MsgNr < 12508141 AND "
    query += u"DateTime > '{0}'".format(datetime_str)

    if end_time != '':
        dt_end_time = str_to_datetime(end_time)
        if not utc:
            dt_end_time = local_time_to_utc(dt_end_time)
        dt_end_time_str = datetime_to_str_without_ms(dt_end_time)
        query += u" AND DateTime < '{0}'".format(dt_end_time_str)

    # if msg_id != '':
    #    query += " AND MsgNr = {id}".format(id=msg_id)

    if msg_text != '':
        query += u" AND Text1 LIKE '%{text}%'".format(text=msg_text)

    if state != '':
        query += u" AND State {state_condition}".format(state_condition=state)

    if priority != '' and priority2 != '':
        query += u" AND (Typename LIKE '%{prio}%'".format(prio=priority)
        query += u" OR Typename LIKE '%{prio}%')".format(prio=priority2)
    elif priority != '':
        query += u" AND Typename LIKE '%{priority}%'".format(priority=priority)

    return query

if __name__ == "__main__":
    import doctest
    doctest.testmod()
