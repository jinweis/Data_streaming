#########################################################
#                                                       #
#       YOUR HOMEWORK BEGINS HERE IN THIS SCRIPT        #
#                                                       #
#########################################################

trip_stream_schema = """
{
  "type": "record",
  "name": "Trip",
  "fields": [
    {
      "name": "trip_id",
      "type": "string"
    },
    {
      "name": "driver_id",
      "type": "string"
    },
    {
      "name": "duration",
      "type": "string"
    },
    {
      "name": "mileage",
      "type": "string"
    },
    {
      "name": "pickup_location",
      "type": "string"
    },
    {
      "name": "destination_location",
      "type": "string"
    },
    {
      "name": "start_time",
      "type": ["null", "string"],
      "default": null
    },
    {
      "name": "completion_time",
      "type": ["null", "string"],
      "default": null
    }
  ]
}"""


# Define the Avro schema for RiderStreamModel
rider_stream_schema = """
{
  "type": "record",
  "name": "Rider",
  "fields": [
    {
      "name": "rider_id",
      "type": "string"
    },
    {
      "name": "trip_id",
      "type": "string"
    },
    {
      "name": "duration_estimate",
      "type": "string"
    },
    {
      "name": "initial_fare_estimate",
      "type": "float"
    },
    {
      "name": "final_adjusted_fare",
      "type": "float"
    },
    {
      "name": "payment_status",
      "type": "string"
    },
    {
      "name": "rating_to_driver",
      "type": "int"
    }
  ]
}
"""

# Define the Avro schema for DriverEarningStreamModel
driver_earning_stream_schema = """
{
  "type": "record",
  "name": "DriverEarning",
  "fields": [
    # Add fields for driver_id, trip_id, and earnings_from_trip
    {
      "name": "driver_id",
      "type": "string"
    },
    {
      "name": "trip_id",
      "type": "string"
    },
    {
      "name": "earnings_from_trip",
      "type": "float"
      }
  ]
}
"""
