"""
This class contains the working hours for each day for every store
"""


class StoreWorkingHours:
    def __init__(self, store_id: int):
        self.store_id = store_id
        self.working_hours = {
            "Monday": [],
            "Tuesday": [],
            "Wednesday": [],
            "Thursday": [],
            "Friday": [],
            "Saturday": [],
            "Sunday": [],
        }

    def get_store_id(self):
        return self.store_id

    def set_working_hours(self, day: str, *args):
        if len(args) == 1:
            self.working_hours[day].append(args[0])
        else:
            self.working_hours[day].append(args)

    def get_working_hours_all(self):
        return self.working_hours

    def get_working_hours(self, day: str):
        return self.working_hours[day]

    def get_monday_working_hours(self):
        return self.working_hours["Monday"]

    def get_tuesday_working_hours(self):
        return self.working_hours["Tuesday"]

    def get_wednesday_working_hours(self):
        return self.working_hours["Wednesday"]

    def get_thursday_working_hours(self):
        return self.working_hours["Thursday"]

    def get_friday_working_hours(self):
        return self.working_hours["Friday"]

    def get_saturday_working_hours(self):
        return self.working_hours["Saturday"]

    def get_sunday_working_hours(self):
        return self.working_hours["Sunday"]

    def __str__(self):
        return f"StoreDetails(store_id={self.store_id}, working_hours={self.working_hours})"

    def __repr__(self):
        return str(self)
