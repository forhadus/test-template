import os, requests, json, hubspot
from dc_sdk import errors
from requests import request
from requests.structures import CaseInsensitiveDict
from datetime import datetime
from datetime import timezone
from hubspot.utils.oauth import get_auth_url
from hubspot.auth.oauth import ApiException
from hubspot import HubSpot
from dotenv import load_dotenv
load_dotenv()


class Connector:
    def __init__(self, credentials):
        """
        store the credentials
        :param credentials: the credentials needed to authenticate with the connector, formatted as a dictionary
        """
        self.credentials = credentials
        self.batch_size = None
        self.redirect_uri = "http://localhost:3000/oauth"
        self.scope = ('crm.schemas.quotes.read', 'crm.objects.line_items.read', 'content', 'crm.schemas.deals.read',
                      'media_bridge.read', 'crm.schemas.line_items.read', 'social', 'timeline',
                      'crm.objects.owners.read', 'tickets', 'e-commerce', 'crm.schemas.custom.read',
                      'crm.objects.companies.read', 'crm.lists.read', 'crm.objects.deals.read',
                      'crm.schemas.contacts.read', 'crm.objects.contacts.read', 'crm.schemas.companies.read',
                      'crm.objects.quotes.read',)
        self.access_token = None
        self.token = None
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.client_id = os.getenv("CLIENT_ID")
        self.custom_reports_objects = ["Contacts", "Companies", "Deals","Tickets", "Products", "Quotes", "Line Items", "Feedback Submissions", "Custom Objects"]

    def authenticate(self):
        """
        authenticate with the connector using self.credentials, update self.credentials if credentials change
        :return: a boolean indicating whether the connector was able to successfully authenticate
        """
        try:
            api_client = HubSpot()
            if "code" in self.credentials and self.credentials.get("code") is not None:
                code_input = self.credentials.get("code")
                resp = api_client.auth.oauth.tokens_api.create_token(
                    grant_type="authorization_code",
                    redirect_uri=self.credentials["redirectURL"],
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                    code=code_input,
                )
                if resp:
                    self.credentials.pop("code", None)
                    self.credentials["token"] = resp.refresh_token or None
                    self.credentials["access_token"] = resp.access_token or None
                    return True
                else:
                    raise errors.AuthenticationError("Problem with credentials. Please re-authenticate account.")
            elif 'token' in self.credentials and self.credentials.get("token"):
                token = self.credentials.get("token")
                print("______red", self.credentials["redirectURL"])
                resp = api_client.auth.oauth.tokens_api.create_token(
                    grant_type="refresh_token",
                    redirect_uri=self.credentials["redirectURL"],
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                    refresh_token=token
                )
                if resp:
                    self.credentials["token"] = resp.refresh_token or None
                    self.credentials["access_token"] = resp.access_token or None
                    return True
                else:
                    raise errors.AuthenticationError("Problem with credentials. Please re-authenticate account.")


        except Exception as ace:
            message = f"Unable to authenticate to Hubspot: {str(ace)}"
            raise errors.AuthenticationError(message)

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
    results = []
    for business_object in self.custom_reports_objects:
        results.append({"object_id": business_object.replace(" ", "_").lower(),
                        "object_name": business_object,
                        "object_label": business_object,
                        "object_group": "Custom Report Objects"
                        })
    return results

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
    try:
        access_token = self.credentials.get("access_token")
        client = hubspot.Client.create(access_token=access_token)
        api_response = client.crm.properties.core_api.get_all(object_type=object_id, archived=False)
        list_field = []
        for fld in api_response.results:
            field_dict = {
                "field_id": fld.name,
                "field_name": fld.name,
                "field_label": fld.label,
                "data_type": fld.type,
                "size": None,
            }
            list_field.append(field_dict)
        return list_field
    except ApiException as e:
        return f"Exception when calling the API: %s\n" % e

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

    def datetime_to_epoch(self, year_month_day_with_dash=None):
        date_time = year_month_day_with_dash
        date = date_time.split(':')[0]
        datetime_obj = datetime.strptime(date, '%Y-%m-%d')
        timestamp = datetime_obj.replace(tzinfo=timezone.utc).timestamp()
        con_epoch_date = int(str(timestamp).replace('.', ''))
        return con_epoch_date

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
    access_token = self.credentials.get("access_token")
    n_rows = n_rows or 1000
    next_page = next_page or 0

    if filters and filters.get("filter_column_nm"):
        raise errors.DataError("Filter applied is not supported yet.")

    if object_id is not None or '':
        if object_id in [ids.get('object_id') for ids in self.get_objects()]:
            if field_ids:
                json_str = json.dumps(field_ids)
                url = f"https://api.hubapi.com/crm/v3/objects/{object_id}/search"
                headers = CaseInsensitiveDict()
                headers["authorization"] = f"Bearer {access_token}"
                headers["content-type"] = "application/json"
                data = f"""
                {{
                    "properties": {json_str},
                    "limit": {n_rows},
                    "after": {next_page}
                    }}
                """
                resp = requests.post(url, headers=headers, data=data)
                if resp.ok:
                    resp_text = resp.text
                    resp_json = json.loads(resp_text)
                    output = {}
                    rows = []
                    for rslt in resp_json['results']:
                        rows.append(rslt['properties'])
                    output['next_page'] = None
                    output['data'] = rows
                    return output
                else:
                    message = "Unable to Get Data."
                    raise errors.DataError(message)
            else:
                message = "Put the Field IDs"
                raise errors.BadFieldIDError(message)
        else:
            message = "Invalid Object Id to Get Data."
            raise errors.BadObjectIDError(message)
    else:
        message = "Need Object Id to Get Data."
        raise errors.BadObjectIDError(message)

        # except ApiException as e:
        #     return f"Exception when calling the API: %s\n" % e

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
