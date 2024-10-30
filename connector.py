# region imports
from dc_sdk import errors
# end region imports

# region class
class Connector:
    # region init
    def __init__(self, credentials):
        """
        store the credentials
        :param credentials: the credentials needed to authenticate with the connector, formatted as a dictionary
        """
        self.credentials = credentials
        self.batch_size = None
    # end region init
    # region authenticate
    def authenticate(self):
        """
        authenticate with the connector using self.credentials, update self.credentials if credentials change
        :return: a boolean indicating whether the connector was able to successfully authenticate
        """
        raise errors.NotImplementedError()
    # end region authenticate
    # region get_metadata
    def get_metadata(self):
        """
        get metadata relating to the object. This is just for destination connectors
        :return: a dictionary containing metadata relating to the object, formatted as
                {
                  "column_type_flg": true | false,
                  "size_flg": true | false,
                  "new_object_regex": "string" | null,
                  "size_regex": "string" | null,
                  "data_types": ["string1", "string2"] | null,
                  "object_id_delimiter": "string" | null  # used to separate the group from the id of the object
                }
        """
        raise errors.NotImplementedError()
    # end region get_metadata
    # region get_objects
    def get_objects(self):
        """
        returns a list of all objects connected to the user's account,
            where object refers to the container of data, whether that is a table, spreadsheet, Salesforce object, etc.
        :return: a list of dictionaries, where each dictionary contains information about the object
            [
                {
                    object_id: <object_id>,
                    object_name: <object_name>,
                    object_label: <object_label>,
                    object_group: <object_group>
                },
                {...},
                ...
            ]
            object_id: the unique identifier of the object. It's what the connector uses to access the object.
            object_name: the actual name of the object. if the object_name is None,
                the frontend will only display the object_label
            object_label: what should be displayed by the frontend.
        """
        raise errors.NotImplementedError()
    # end region get_objects
    # region get_fields
    def get_fields(self, object_id, options=dict()):
        """
        returns a list of all fields (columns) connected to the specified object (table)
        :param object_id: one of the object_id's returned by the get_objects() function
        :return: a list of dictionaries, where each dictionary contains information about a field
        [
            {
                field_id: <field_id>,
                field_name: <field_name>,
                field_label: <field_label>,
                data_type: <data_type>,
                size: <size>
            },
            {...},
            ...
        ]
        """
        raise errors.NotImplementedError()
    # end region get_fields
    # region determine_batch_size
    def determine_batch_size(self, object_id, field_ids, filters=None):
        """
        determines how many rows can be pulled at once without going over the 5mB limit. This is done automatically by
        some APIs, so it might not need to be implemented
        :param object_id:
        :param field_ids:
        :param filters:
        :return:
        """
        self.batch_size = 1000
        raise errors.NotImplementedError()
    # end region determine_batch_size
    # region get_data
    def get_data(self, object_id, field_ids, n_rows=None, filters=None, next_page=None, options=dict()):
        """
        :param object_id: one of the object_id's returned by the get_objects() function
        :param field_ids: a list of strings containing the field_ids returned by the get_fields() function
        :param n_rows: the number of rows for which to return the data
        :param filters:
        {
            filtered_column_nm: <column name>,
            start_selection_nm: <category of start filter ("Today", "Yesterday", "Today Subtract", "Custom Date")>,
            end_selection_nm: <category of end filter ("Today", "Yesterday", "Today Subtract", "Custom Date")>,
            start_value_txt: <the date of the beginning of the range by which to filter>,
            end_value_txt: <the date of the end of the range by which to filter>,
            timezone_offset_nbr: <integer representing offset from UTC time>
        }
                if start_selection_nm is None, pull all data from the beginning of time to the end date
                if end_selection_nm is None, pull all data from the start date to the most recent stuff
        :param next_page: This gives the identifier for the next batch to be pulled if the data is too large to pull all
        together. For some connectors, this may be an identifier returned by the API, and for others, this may need to
        be an identifier for the next rows to pull.
        :return: the data pulled from the source, formatted as an list of dicts where each dict is a row
            {next_page: <the identifier for the next page to pull or None>,
            data:
            [{field_ids[1]: <value string in 1st row, 1st column>,
              field_ids[2]: <value string in 1st row, 2nd column>, ...},
            {field_ids[1]: <value string in 2nd row, 1st column>,
             field_ids[2]: <value string in 2nd row, 2nd column>, ...},
            ...
            {field_ids[N]: <value string in Nth row, 1st column>, ...}]
            }
        """
        raise errors.NotImplementedError()
    # end region get_data
    # region load_data
    def load_data(self, data, object_id, m, update_method, batch_number: int, total_batches: int):
        """
        DESTINATION ONLY FUNCTION
        :param update_method: int flag to show whether the data should be appended or added as a new table,
            0: append -> add the new data to the end of the table without changing the existing data
            1: replace -> remove the old data, insert the new data
            2: upsert -> the data overwrite existing data, currently not supported
        :param data: the data pulled from the source's get_data() function,
            formatted as an list of dicts where each dict is a row
                [{field_ids[1]: <value string in 1st row, 1st column>,
                field_ids[2]: <value string in 1st row, 2nd column>, ...},
                {field_ids[1]: <value string in 2nd row, 1st column>,
                field_ids[2]: <value string in 2nd row, 2nd column>, ...},
                ...
                {field_ids[N]: <value string in Nth row, first column>, ...}]
        :param object_id: the unique id string of the object returned from get_objects()
        :param m: a list of dicts, used to map all the data in the source column to the destination column name.
            basically, the source column's name will change to that of the destination column,
            but the data in that column will not change
            formatted as a list of dicts, where each dict represents a column
                [{'source_field_id': <value of source column's id>,
                'destination_field_id': <value of the destination column>,
                'datatype': '<datatype of the source column>',
                'size': '<size>' }, ... ]
        :param batch_number: an int indicating what batch the connector is on.
        :param total_batches: an int indicating how many batches the connector will have to load in total.
        :return: a boolean to indicate the success of the process
        """

        # This is the default if the connector is not a destination
        raise errors.NotADestinationError()
    # end region load_data
    # region other_functions
    # end region other_functions
#end region class
