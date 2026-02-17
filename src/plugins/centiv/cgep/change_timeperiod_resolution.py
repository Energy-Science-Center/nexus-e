import numpy as np

def extend_hourly_timeseries(timeseries, timeperiods, hydrodrought=False):
    if timeperiods > len(timeseries):
        if hydrodrought:
            temp = (timeseries * (timeperiods // len(timeseries)))[:timeperiods-len(timeseries)]
            drought_timeseries = [x * 0.8 for x in temp] # MP: this is a simple way to create a drought timeseries, by reducing the values of the original timeseries by 20%
            extended_timeseries = np.concatenate((timeseries, drought_timeseries))
        else:
            extended_timeseries = (timeseries * (timeperiods // len(timeseries) + 1))[:timeperiods]
        return extended_timeseries
    return timeseries

def extend_daily_timeseries(timeseries, timeperiods):
    timeperiods_days = timeperiods // 24
    if (timeperiods_days) > len(timeseries):
        extended_timeseries = (timeseries * (timeperiods_days // len(timeseries) + 1))[:timeperiods_days]
        return extended_timeseries
    return timeseries

class ChangeResolution:
    def __init__(self, numperiods, tpResolution):
        self.tpResolution = tpResolution
        self.numperiods = numperiods
        self.hours = range(0, self.numperiods)
        self.alldays = [self.hours[i:i + 24] for i in range(0, len(self.hours), 24)] #split the list of hours in lists with the days
        self.alldays_day = range(0, int(len(self.alldays))) # MP
        self.usedays = self.alldays[::tpResolution]
        self.usedays_day = self.alldays_day[::tpResolution] # MP
        self.usehours = [item for sublist in self.usedays for item in sublist] #list of the actual hours we will simulate
        self.usehoursconsec = range(0,len(self.usehours)) #list of consecutive hours
        self.usedaysconsec = range(0,len(self.usedays_day)) #MP
        self.mappingdict = dict(zip(self.usehours, self.usehoursconsec)) #contains the mapping of the actual hours (keys in the dict) to the new consecutive hours (values in dict)
        self.mappingdict_days = dict(zip(self.usedays_day, self.usedaysconsec))
        self.revmapping = dict(zip(self.usehoursconsec, self.usehours))

    def remap_hours_dict(self, d):
        for k1, _ in list(d.items()):
            if k1 not in self.mappingdict:
                del d[k1]
            else:
                d[self.mappingdict[k1]] = d.pop(k1)
    def remap_days_dict(self, d):
        for k1, _ in list(d.items()):
            if k1 not in self.mappingdict_days:
                del d[k1]
            else:
                d[self.mappingdict_days[k1]] = d.pop(k1)

    def remap_hours_dict_in_dict(self, d):
        for _, v1 in list(d.items()):
            self.remap_hours_dict(v1)
    def remap_days_dict_in_dict(self, d):
        for _, v1 in list(d.items()):
            self.remap_days_dict(v1)

    def new_timeperiods(self):
        return len(self.usehours)

    def new_days(self):
        return len(self.usedays)

    '''
    Expands power generated array. By duplicating dates.
    '''
    def expand_array(self, arr):
        result = np.zeros(self.numperiods)
        for hour in self.usehoursconsec:
            outhour = self.revmapping[hour]
            for i in range(self.tpResolution):
                outindex = outhour + i*24
                if outindex < self.numperiods:
                    result[outindex] = arr[hour]
        return result

    '''
    Expands state of charge array by interpolating.
    '''
    def expand_soc_array(self, arr):
        deltas = np.zeros(self.new_timeperiods())
        for i in range(self.new_timeperiods()):
            if i > 0:
                deltas[i] = arr[i] - arr[i-1]
        expdeltas = self.expand_array(deltas) / self.tpResolution

        result = np.zeros(self.numperiods)
        result[0] = arr[0]
        for i in range(self.numperiods):
            if i > 0:
                result[i] = result[i - 1] + expdeltas[i]
        return result