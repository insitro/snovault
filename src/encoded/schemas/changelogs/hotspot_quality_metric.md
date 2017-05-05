## Changelog for hotspot_quality_metric.json

### Schema version 6

* *generated_by* field has been added as a required field, to distinguish between Hotspot1 and Hotspot2 scores.
* *total tags* and *hotspot tags* fields have been added to characterize Hotspot1 methods"

### Schema version 5

* *assay_term_id* is no longer allowed to be submitted, it will be automatically calculated based on the term_name
* *notes* field is no longer allowed to have leading or trailing whitespace or contain just an empty string.