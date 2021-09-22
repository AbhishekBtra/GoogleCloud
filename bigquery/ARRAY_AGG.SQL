SELECT
  CONCAT(
    'Less Than Equal To ',txt,' => ',FORMAT('%T',ARRAY_AGG(txt) OVER (ORDER BY txt ))
  ) AS less_than_eqaul_to
FROM
  UNNEST(SPLIT('45 98 12 23 50',' ')) AS txt

  /*
  INPUT
  -----------------------------------------------------------------------------------------------------------
  [45,98,12,23,50]


  OUTPUT json
  -----------------------------------------------------------------------------------------------------------
  [
  {
    "less_than_eqaul_to": "Less Than Equal To 12 => [\"12\"]"
  },
  {
    "less_than_eqaul_to": "Less Than Equal To 23 => [\"12\", \"23\"]"
  },
  {
    "less_than_eqaul_to": "Less Than Equal To 45 => [\"12\", \"23\", \"45\"]"
  },
  {
    "less_than_eqaul_to": "Less Than Equal To 50 => [\"12\", \"23\", \"45\", \"50\"]"
  },
  {
    "less_than_eqaul_to": "Less Than Equal To 98 => [\"12\", \"23\", \"45\", \"50\", \"98\"]"
  }
]
  */