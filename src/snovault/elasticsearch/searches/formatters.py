from .interfaces import APPENDED
from .interfaces import FIELD_KEY
from .interfaces import TERMS
from .interfaces import TITLE
from .interfaces import TOTAL
from .interfaces import TYPE_KEY



class AggregationFormatter():

    def __init__(self, aggregations, query_builder):
        self.aggregations = aggregations
        self.query_builder = query_builder

    def _get_aggregation_field(self):
         pass

    def _get_aggregation_title(self):
        pass

    def _get_aggregation_type(self):
        pass

    def _get_aggregation_results(self):
        pass

    def _get_aggregation_total(self):
        pass

    def _aggregation_is_appeneded(self):
        pass

    def _format_aggregation(self):
        return {
            FIELD_KEY: get_aggregation_field(),
            TITLE: get_aggregation_title(),
            TERMS: get_aggregation_results(),
            TOTAL: get_aggregation_total(),
            TYPE_KEY: get_aggregation_type(),
            APPENDED: aggregation_is_appeneded(),
        }


    def format_aggregations(self):
        pass