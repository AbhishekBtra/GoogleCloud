DECLARE JSON_ STRING DEFAULT """
  {
    "race": "800M",
    "participants": [
      {
        "name": "Rudisha",
        "splits": [
          "23.4",
          "26.3",
          "26.4",
          "26.1"
        ]
      },
      {
        "name": "Makhloufi",
        "splits": [
          "24.5",
          "25.4",
          "26.6",
          "26.1"
        ]
      },
      {
        "name": "Murphy",
        "splits": [
          "23.9",
          "26.0",
          "27.0",
          "26.0"
        ]
      },
      {
        "name": "Bosse",
        "splits": [
          "23.6",
          "26.2",
          "26.5",
          "27.1"
        ]
      },
      {
        "name": "Rotich",
        "splits": [
          "24.7",
          "25.6",
          "26.9",
          "26.4"
        ]
      },
      {
        "name": "Lewandowski",
        "splits": [
          "25.0",
          "25.7",
          "26.3",
          "27.2"
        ]
      },
      {
        "name": "Kipketer",
        "splits": [
          "23.2",
          "26.1",
          "27.3",
          "29.4"
        ]
      },
      {
        "name": "Berian",
        "splits": [
          "23.7",
          "26.1",
          "27.0",
          "29.3"
        ]
      }
    ]
  }
""";

SELECT 
        array_agg(participants) as all_particpants_json_string_array,
        array_agg(json_value(participants,"$.name")) all_names_string_array,
        array_agg(json_extract_scalar(participants,"$.name")) all_names_string_array_using_json_extract_scalar,
        array_agg(json_extract(participants,"$.splits")) all_splits_array_by_player,
        array_agg(json_query(participants,"$.splits")) all_splits_array_by_player_same_as_json_extract,
        array_concat_agg((JSON_EXTRACT_ARRAY(participants,"$.splits"))) all_splits,
        array_concat_agg((json_query_array(participants,"$.splits"))) all_splits_same_as_json_extract_array
from 
unnest(JSON_EXTRACT_ARRAY(JSON_,"$.participants")) as participants;

/*conert sql to json string*/
select 
        to_json_string(
          struct(
              [1,2,3] as numbers,
              "1" as id
          ),true
        ) as json_
/*
{
  "numbers": [
    1,
    2,
    3
  ],
  "id": "1"
}
*/