CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`machine_learning_curated` (
  `timeStamp` timestamp,
  `user` string,
  `x` float,
  `y` float,
  `z` float,
  `serialNumber` string,
  `distanceFromObject` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://em-stedi-lkhouse/machine_learning/curated/'
TBLPROPERTIES ('classification' = 'json');